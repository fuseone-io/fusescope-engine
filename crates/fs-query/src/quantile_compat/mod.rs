//! `quantile_compat(value, q)`: a quantile compatible with ClickHouse's
//! `quantile` under the parity's rules (ADR 0011), not its algorithm.
//!
//! Up to [`EXACT_LIMIT`] values a group is in the parity's exact regime: it
//! keeps them all and answers the linear interpolation at rank `q·(n − 1)`.
//! ClickHouse already samples past 8,192 values. Past the limit the values
//! fold into a t-digest of [`DIGEST_CENTROIDS`] centroids, whose estimate
//! stays within the parity's rank tolerance. NULL and NaN values are skipped;
//! a group without values is NULL.
//!
//! Partial states carry either the values or the digest: `values` and
//! `digest` are `List<Float64>`, one of them NULL. A digest is
//! `[sum, count, max, min, mean₀, weight₀, mean₁, weight₁, …]`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, ListArray};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion_functions_aggregate_common::tdigest::{Centroid, TDigest};

/// The largest group kept exact: the parity judges a quantile `q` over `n`
/// values by its order statistics while `0.1·q·(1 − q) ≤ 1/n`, which for the
/// backend's highest quantile, p999, holds up to `n = 10 010`.
pub const EXACT_LIMIT: usize = 10_010;

/// The t-digest size past [`EXACT_LIMIT`].
pub const DIGEST_CENTROIDS: usize = 1_000;

const NAME: &str = "quantile_compat";

/// Builds the aggregate function registered in every tenant session.
pub fn quantile_compat() -> AggregateUDF {
    AggregateUDF::new_from_impl(QuantileCompat::new())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct QuantileCompat {
    signature: Signature,
}

impl QuantileCompat {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for QuantileCompat {
    fn name(&self) -> &'static str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let list = DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true)));
        Ok(vec![
            Arc::new(Field::new(
                format!("{}[values]", args.name),
                list.clone(),
                true,
            )),
            Arc::new(Field::new(format!("{}[digest]", args.name), list, true)),
            Arc::new(Field::new(
                format!("{}[quantile]", args.name),
                DataType::Float64,
                true,
            )),
        ])
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CompatAccumulator::default()))
    }
}

#[derive(Debug)]
enum Values {
    Exact(Vec<f64>),
    Digest(TDigest),
}

/// One group's state, on one partition. It holds at most [`EXACT_LIMIT`]
/// values (about 80 KB) or a digest of [`DIGEST_CENTROIDS`] centroids (about
/// 16 KB); while a batch crosses the limit, and while a state is serialized,
/// both the values and their copy are held.
#[derive(Debug)]
struct CompatAccumulator {
    values: Values,
    quantile: Option<f64>,
}

impl Default for CompatAccumulator {
    fn default() -> Self {
        Self {
            values: Values::Exact(Vec::new()),
            quantile: None,
        }
    }
}

impl CompatAccumulator {
    fn set_quantile(&mut self, quantile: f64) -> Result<()> {
        if !quantile.is_finite() || !(0.0..=1.0).contains(&quantile) {
            return Err(DataFusionError::Execution(format!(
                "{NAME} quantile must be between zero and one"
            )));
        }
        if self
            .quantile
            .is_some_and(|known| known.to_bits() != quantile.to_bits())
        {
            return Err(DataFusionError::Execution(format!(
                "{NAME} quantile must be constant within a group"
            )));
        }
        self.quantile = Some(quantile);
        Ok(())
    }

    fn add_values(&mut self, batch: Vec<f64>) {
        if batch.is_empty() {
            return;
        }
        match &mut self.values {
            Values::Exact(values) if values.len() + batch.len() <= EXACT_LIMIT => {
                let wanted = values.len() + batch.len();
                if wanted > values.capacity() {
                    let grown = (values.capacity() * 2).clamp(wanted, EXACT_LIMIT);
                    values.reserve_exact(grown - values.len());
                }
                values.extend(batch);
            }
            Values::Exact(values) => {
                let mut all = std::mem::take(values);
                all.extend(batch);
                self.values =
                    Values::Digest(TDigest::new(DIGEST_CENTROIDS).merge_unsorted_f64(all));
            }
            Values::Digest(digest) => *digest = digest.merge_unsorted_f64(batch),
        }
    }

    fn add_digest(&mut self, other: TDigest) {
        self.values = Values::Digest(
            match std::mem::replace(&mut self.values, Values::Exact(Vec::new())) {
                Values::Exact(values) if values.is_empty() => other,
                Values::Exact(values) => other.merge_unsorted_f64(values),
                Values::Digest(digest) => TDigest::merge_digests([&digest, &other]),
            },
        );
    }

    fn values_state(&self) -> ScalarValue {
        match &self.values {
            Values::Exact(values) => list_scalar(values.clone()),
            Values::Digest(_) => null_list(),
        }
    }

    fn digest_state(&self) -> ScalarValue {
        match &self.values {
            Values::Exact(_) => null_list(),
            Values::Digest(digest) => list_scalar(encode_digest(digest)),
        }
    }
}

impl Accumulator for CompatAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            self.values_state(),
            self.digest_state(),
            ScalarValue::Float64(self.quantile),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values, quantiles] = values else {
            return Err(internal("arguments"));
        };
        let values = values
            .as_primitive_opt::<Float64Type>()
            .ok_or_else(|| internal("value array"))?;
        let quantiles = quantiles
            .as_primitive_opt::<Float64Type>()
            .ok_or_else(|| internal("quantile array"))?;
        let mut batch = Vec::with_capacity(values.len() - values.null_count());
        for row in 0..values.len() {
            if values.is_null(row) || quantiles.is_null(row) || values.value(row).is_nan() {
                continue;
            }
            self.set_quantile(quantiles.value(row))?;
            batch.push(values.value(row));
        }
        self.add_values(batch);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [values, digests, quantiles] = states else {
            return Err(internal("state"));
        };
        let values = values
            .as_list_opt::<i32>()
            .ok_or_else(|| internal("values state"))?;
        let digests = digests
            .as_list_opt::<i32>()
            .ok_or_else(|| internal("digest state"))?;
        let quantiles = quantiles
            .as_primitive_opt::<Float64Type>()
            .ok_or_else(|| internal("quantile state"))?;
        for row in 0..quantiles.len() {
            if quantiles.is_null(row) {
                continue;
            }
            self.set_quantile(quantiles.value(row))?;
            if !values.is_null(row) {
                self.add_values(floats(values, row)?.values().to_vec());
            }
            if !digests.is_null(row) {
                self.add_digest(decode_digest(floats(digests, row)?.values())?);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let Some(q) = self.quantile else {
            return Ok(ScalarValue::Float64(None));
        };
        Ok(ScalarValue::Float64(match &mut self.values {
            Values::Exact(values) => exact_quantile(values, q),
            Values::Digest(digest) => Some(digest.estimate_quantile(q)),
        }))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + match &self.values {
                Values::Exact(values) => values.capacity() * std::mem::size_of::<f64>(),
                Values::Digest(digest) => digest.size() - std::mem::size_of_val(digest),
            }
    }
}

/// The linear interpolation between the order statistics around rank
/// `q·(n − 1)`, never outside them.
fn exact_quantile(values: &mut [f64], q: f64) -> Option<f64> {
    let last = values.len().checked_sub(1)?;
    let (rank, fraction) = rank_of(q, last);
    let (_, low, above) = values.select_nth_unstable_by(rank, f64::total_cmp);
    let low = *low;
    let high = if fraction > 0.0 {
        above.iter().copied().min_by(f64::total_cmp).unwrap_or(low)
    } else {
        low
    };
    Some((low + (high - low) * fraction).clamp(low, high))
}

/// The integer and fractional parts of `q·last`, `last` at most
/// [`EXACT_LIMIT`] − 1 so the conversions are exact.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn rank_of(q: f64, last: usize) -> (usize, f64) {
    let h = q * last as f64;
    let rank = (h.floor() as usize).min(last);
    (rank, (h - rank as f64).clamp(0.0, 1.0))
}

fn encode_digest(digest: &TDigest) -> Vec<f64> {
    let mut out = Vec::with_capacity(4 + 2 * digest.centroids().len());
    out.extend([digest.sum(), digest.count(), digest.max(), digest.min()]);
    for centroid in digest.centroids() {
        out.extend([centroid.mean(), centroid.weight()]);
    }
    out
}

fn decode_digest(parts: &[f64]) -> Result<TDigest> {
    let (&[sum, count, max, min], centroids) = parts
        .split_first_chunk::<4>()
        .ok_or_else(|| DataFusionError::Execution(format!("{NAME} digest state is truncated")))?;
    if centroids.len() % 2 != 0 {
        return Err(DataFusionError::Execution(format!(
            "{NAME} digest state is truncated"
        )));
    }
    let mut parsed = Vec::with_capacity(centroids.len() / 2);
    for pair in centroids.chunks_exact(2) {
        if let &[mean, weight] = pair {
            parsed.push(Centroid::new(mean, weight));
        }
    }
    TDigest::try_from_parts(DIGEST_CENTROIDS, sum, count, max, min, parsed)
}

fn floats(list: &ListArray, row: usize) -> Result<Float64Array> {
    list.value(row)
        .as_primitive_opt::<Float64Type>()
        .cloned()
        .ok_or_else(|| internal("list items"))
}

fn list_scalar(values: Vec<f64>) -> ScalarValue {
    ScalarValue::List(Arc::new(
        SingleRowListArrayBuilder::new(Arc::new(Float64Array::from(values))).build_list_array(),
    ))
}

fn null_list() -> ScalarValue {
    ScalarValue::List(Arc::new(ListArray::new_null(
        Arc::new(Field::new_list_field(DataType::Float64, true)),
        1,
    )))
}

fn internal(what: &str) -> DataFusionError {
    DataFusionError::Internal(format!("{NAME} {what}"))
}

#[cfg(test)]
mod tests;
