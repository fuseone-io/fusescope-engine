//! `has_token(text, needle)`: true when `needle` is exactly one token of
//! `text` under the text sidecar's normalization, compared case-sensitively.
//! A `NULL` argument gives `NULL`; an empty needle or one that is not a
//! single token gives false. A case-insensitive search is
//! `has_token(lower(body), lower(needle))`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Name the function is registered under.
pub const NAME: &str = "has_token";

/// The `has_token` UDF.
pub fn udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(HasToken {
        signature: Signature::string(2, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct HasToken {
    signature: Signature,
}

impl ScalarUDFImpl for HasToken {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [text, needle] = arrays.as_slice() else {
            return datafusion::common::exec_err!("has_token takes two arguments");
        };
        let out = tokens_in(&utf8(text)?, &utf8(needle)?);
        if scalar {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?));
        }
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

fn utf8(array: &ArrayRef) -> Result<ArrayRef> {
    Ok(arrow::compute::cast(array, &DataType::Utf8)?)
}

fn tokens_in(text: &ArrayRef, needle: &ArrayRef) -> BooleanArray {
    let (text, needle) = (text.as_string::<i32>(), needle.as_string::<i32>());
    (0..text.len())
        .map(|i| {
            (text.is_valid(i) && needle.is_valid(i)).then(|| {
                fs_index::text_index::has_token(
                    text.value(i).as_bytes(),
                    needle.value(i).as_bytes(),
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use super::*;

    async fn answer(sql: &str) -> Vec<Option<bool>> {
        let ctx = SessionContext::new();
        ctx.register_udf(udf());
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches
            .iter()
            .flat_map(|b| b.column(0).as_boolean().iter().collect::<Vec<_>>())
            .collect()
    }

    #[tokio::test]
    async fn a_token_is_a_whole_alphanumeric_run_compared_case_sensitively() {
        let got = answer(
            "select has_token(t, n) from (values
                ('connection timeout: retry', 'timeout'),
                ('connection timeout: retry', 'Timeout'),
                ('connection timeouts', 'timeout'),
                ('a-b', 'a-b'),
                ('a b', ''),
                ('ça va', 'a'),
                (NULL, 'x'),
                ('x', NULL)
            ) as v(t, n)",
        )
        .await;
        assert_eq!(
            got,
            [
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                None,
                None
            ]
        );
        assert_eq!(
            answer("select has_token(lower('Connection TIMEOUT'), lower('Timeout'))").await,
            [Some(true)]
        );
    }
}
