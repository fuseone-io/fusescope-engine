//! `quantile_tdigest(digest, q)`: merge service-graph latency digests and
//! estimate one quantile without expanding their observations.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use fs_schema::service_graph::LatencyDigest;

/// Builds the aggregate function registered in every tenant session.
pub fn quantile_tdigest() -> AggregateUDF {
    AggregateUDF::new_from_impl(QuantileTDigest::new())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct QuantileTDigest {
    signature: Signature,
}

impl QuantileTDigest {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![DataType::Binary, DataType::Float64]),
                    Exact(vec![DataType::Binary, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for QuantileTDigest {
    fn name(&self) -> &'static str {
        "quantile_tdigest"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format!("{}[digest]", args.name),
                DataType::Binary,
                true,
            )),
            Arc::new(Field::new(
                format!("{}[quantile]", args.name),
                DataType::Float64,
                true,
            )),
        ])
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(QuantileAccumulator::default()))
    }
}

#[derive(Debug, Default)]
struct QuantileAccumulator {
    digest: Option<LatencyDigest>,
    quantile: Option<f64>,
}

impl QuantileAccumulator {
    fn absorb(&mut self, digest: &[u8], quantile: f64) -> Result<()> {
        if !quantile.is_finite() || !(0.0..=1.0).contains(&quantile) {
            return Err(DataFusionError::Execution(
                "quantile_tdigest quantile must be between zero and one".into(),
            ));
        }
        if self
            .quantile
            .is_some_and(|known| known.to_bits() != quantile.to_bits())
        {
            return Err(DataFusionError::Execution(
                "quantile_tdigest quantile must be constant within a group".into(),
            ));
        }
        let digest = LatencyDigest::decode(digest).map_err(|e| {
            DataFusionError::Execution(format!("quantile_tdigest invalid digest: {e}"))
        })?;
        self.digest = Some(match self.digest.take() {
            Some(known) => LatencyDigest::merge([&known, &digest]),
            None => digest,
        });
        self.quantile = Some(quantile);
        Ok(())
    }

    fn digests(values: &[ArrayRef]) -> Result<&BinaryArray> {
        let digests = values
            .first()
            .and_then(|a| a.as_any().downcast_ref::<BinaryArray>())
            .ok_or_else(|| DataFusionError::Internal("quantile_tdigest digest array".into()))?;
        Ok(digests)
    }
}

impl Accumulator for QuantileAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Binary(self.digest.as_ref().map(LatencyDigest::encode)),
            ScalarValue::Float64(self.quantile),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let digests = Self::digests(values)?;
        let quantiles = values
            .get(1)
            .ok_or_else(|| DataFusionError::Internal("quantile_tdigest quantile array".into()))?;
        if let Some(quantiles) = quantiles.as_any().downcast_ref::<Float64Array>() {
            for row in 0..digests.len() {
                if !digests.is_null(row) && !quantiles.is_null(row) {
                    self.absorb(digests.value(row), quantiles.value(row))?;
                }
            }
        } else if let Some(quantiles) = quantiles.as_any().downcast_ref::<Int64Array>() {
            for row in 0..digests.len() {
                if !digests.is_null(row) && !quantiles.is_null(row) {
                    let quantile = match quantiles.value(row) {
                        0 => 0.0,
                        1 => 1.0,
                        _ => {
                            return Err(DataFusionError::Execution(
                                "integer quantile must be zero or one".into(),
                            ));
                        }
                    };
                    self.absorb(digests.value(row), quantile)?;
                }
            }
        } else {
            return Err(DataFusionError::Internal(
                "quantile_tdigest quantile array".into(),
            ));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(
            self.digest
                .as_ref()
                .zip(self.quantile)
                .map(|(digest, quantile)| digest.quantile(quantile)),
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.digest.as_ref().map_or(0, LatencyDigest::size)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{BinaryArray, Float64Array, Int64Array};
    use datafusion::logical_expr::Accumulator;

    use super::*;

    #[test]
    fn aggregate_merges_digests_and_estimates_the_quantile() {
        let a = LatencyDigest::from_values((1..=100).map(f64::from).collect()).encode();
        let b = LatencyDigest::from_values((101..=200).map(f64::from).collect()).encode();
        let mut accumulator = QuantileAccumulator::default();
        accumulator
            .update_batch(&[
                Arc::new(BinaryArray::from_iter_values([a, b])),
                Arc::new(Float64Array::from(vec![0.99, 0.99])),
            ])
            .unwrap();
        let ScalarValue::Float64(Some(value)) = accumulator.evaluate().unwrap() else {
            panic!("quantile is a float");
        };
        assert!((value - 198.0).abs() <= 3.0);
    }

    #[test]
    fn invalid_digest_quantile_or_changing_quantile_is_refused() {
        let digest = LatencyDigest::from_values(vec![1.0]).encode();
        let mut accumulator = QuantileAccumulator::default();
        assert!(accumulator.absorb(b"bad", 0.5).is_err());
        assert!(accumulator.absorb(&digest, 2.0).is_err());
        accumulator.absorb(&digest, 0.5).unwrap();
        assert!(accumulator.absorb(&digest, 0.9).is_err());
    }

    #[test]
    fn an_integer_one_is_a_valid_quantile() {
        let digest = LatencyDigest::from_values(vec![1.0, 2.0]).encode();
        let mut accumulator = QuantileAccumulator::default();
        accumulator
            .update_batch(&[
                Arc::new(BinaryArray::from_iter_values([digest])),
                Arc::new(Int64Array::from(vec![1])),
            ])
            .unwrap();
        assert_eq!(
            accumulator.evaluate().unwrap(),
            ScalarValue::Float64(Some(2.0))
        );
    }
}
