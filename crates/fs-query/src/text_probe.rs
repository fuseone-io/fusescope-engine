//! What the body predicates of a log query ask of the text sidecars.
//!
//! Recognised, on `body` or `lower(body)`: `LIKE` and `ILIKE` with the
//! default `\` escape, `=`, `contains`, `starts_with`, `ends_with`,
//! `strpos(…) > 0` and `has_token`. A case-insensitive form (`ILIKE`, or
//! any form on `lower(body)`) yields a folded probe. Anything else, `NOT`
//! included, is unknown and keeps every row group.

use arrow::datatypes::DataType;
use datafusion::logical_expr::{BinaryExpr, Expr, Like, Operator};
use datafusion::scalar::ScalarValue;
use fs_index::text_index::Probe;
use fs_schema::logs::col;

/// The probe of `filters`, implicitly ANDed.
pub fn probe(filters: &[Expr]) -> Probe {
    Probe::all(filters.iter().map(of))
}

fn of(expr: &Expr) -> Probe {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => Probe::all([of(left), of(right)]),
            Operator::Or => Probe::any([of(left), of(right)]),
            Operator::Eq => equality(left, right).unwrap_or(Probe::Unknown),
            _ => position(left, *op, right).unwrap_or(Probe::Unknown),
        },
        Expr::Like(like) => like_probe(like).unwrap_or(Probe::Unknown),
        Expr::ScalarFunction(call) => {
            let [text, arg] = call.args.as_slice() else {
                return Probe::Unknown;
            };
            let (Some(folded), Some(needle)) = (body(text), string(arg)) else {
                return Probe::Unknown;
            };
            match call.func.name() {
                "contains" | "starts_with" | "ends_with" => {
                    Probe::fragment(needle.as_bytes(), folded)
                }
                crate::has_token::NAME => Probe::token(needle.as_bytes(), folded),
                _ => Probe::Unknown,
            }
        }
        _ => Probe::Unknown,
    }
}

/// `Some(folded)` when `expr` is the body, `Some(true)` under `lower`.
fn body(expr: &Expr) -> Option<bool> {
    match expr {
        Expr::Column(c) if c.name == col::BODY => Some(false),
        Expr::Cast(cast)
            if matches!(
                cast.field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            body(&cast.expr)
        }
        Expr::ScalarFunction(call) if call.func.name() == "lower" => match call.args.as_slice() {
            [arg] => body(arg).map(|_| true),
            _ => None,
        },
        _ => None,
    }
}

fn string(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)),
            _,
        ) => Some(s.as_str()),
        Expr::Cast(cast) => string(&cast.expr),
        _ => None,
    }
}

/// `body = 'x'` holds `x` whole.
fn equality(left: &Expr, right: &Expr) -> Option<Probe> {
    let (folded, value) = match (body(left), body(right)) {
        (Some(folded), None) => (folded, string(right)?),
        (None, Some(folded)) => (folded, string(left)?),
        _ => return None,
    };
    Some(Probe::fragment(value.as_bytes(), folded))
}

/// `strpos(body, 'x') > 0`, or `>= 1`, `<> 0`, either way round.
fn position(left: &Expr, op: Operator, right: &Expr) -> Option<Probe> {
    let (call, op, bound) = match (left, right) {
        (Expr::ScalarFunction(call), Expr::Literal(v, _)) => (call, op, v),
        (Expr::Literal(v, _), Expr::ScalarFunction(call)) => (call, op.swap()?, v),
        _ => return None,
    };
    if call.func.name() != "strpos" {
        return None;
    }
    let bound = match bound {
        ScalarValue::Int64(Some(n)) => *n,
        ScalarValue::Int32(Some(n)) => i64::from(*n),
        _ => return None,
    };
    let found = matches!(
        (op, bound),
        (Operator::Gt | Operator::NotEq, 0) | (Operator::GtEq, 1)
    );
    let [text, needle] = call.args.as_slice() else {
        return None;
    };
    if !found {
        return None;
    }
    Some(Probe::fragment(string(needle)?.as_bytes(), body(text)?))
}

fn like_probe(like: &Like) -> Option<Probe> {
    if like.negated || !matches!(like.escape_char, None | Some('\\')) {
        return None;
    }
    let folded = body(&like.expr)? || like.case_insensitive;
    let pattern = string(&like.pattern)?;
    Some(Probe::all(
        fragments(pattern)
            .iter()
            .map(|f| Probe::fragment(f.as_bytes(), folded)),
    ))
}

/// The literal runs of a `LIKE` pattern, as Arrow reads it: `%` and `_`
/// end a run, `\` takes the next character literally, and a trailing `\`
/// is itself literal.
fn fragments(pattern: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => current.push(chars.next().unwrap_or('\\')),
            '%' | '_' => out.push(std::mem::take(&mut current)),
            c => current.push(c),
        }
    }
    out.push(current);
    out.retain(|f| !f.is_empty());
    out
}

#[cfg(test)]
mod tests {
    use datafusion::functions::expr_fn::{contains, lower, starts_with, strpos};
    use datafusion::logical_expr::{col as c, lit};

    use super::*;

    fn frag(s: &str, folded: bool) -> Probe {
        Probe::fragment(s.as_bytes(), folded)
    }

    fn has_token(text: Expr, needle: &str) -> Expr {
        crate::has_token::udf().call(vec![text, lit(needle)])
    }

    #[test]
    fn like_patterns_give_their_literal_runs() {
        assert_eq!(fragments("%timeout%"), ["timeout"]);
        assert_eq!(fragments("abc%def_ghi"), ["abc", "def", "ghi"]);
        assert_eq!(fragments(r"50\%%off"), ["50%", "off"]);
        assert_eq!(fragments(r"a\_b"), ["a_b"]);
        assert_eq!(fragments(r"a\\b"), [r"a\b"]);
        assert_eq!(fragments(r"end\"), [r"end\"]);
        assert_eq!(fragments("%%"), Vec::<String>::new());
    }

    #[test]
    fn body_predicates_become_probes() {
        let body = || c("body");
        assert_eq!(
            probe(&[body().like(lit("%timeout%"))]),
            frag("timeout", false)
        );
        assert_eq!(
            probe(&[body().ilike(lit("%Timeout%"))]),
            frag("Timeout", true)
        );
        assert_eq!(
            probe(&[lower(body()).like(lit("%timeout%"))]),
            frag("timeout", true)
        );
        assert_eq!(
            probe(&[body().eq(lit("connection reset"))]),
            frag("connection reset", false)
        );
        assert_eq!(
            probe(&[contains(body(), lit("reset"))]),
            frag("reset", false)
        );
        assert_eq!(
            probe(&[starts_with(body(), lit("GET /"))]),
            frag("GET /", false)
        );
        assert_eq!(
            probe(&[strpos(body(), lit("reset")).gt(lit(0))]),
            frag("reset", false)
        );
        assert_eq!(
            probe(&[lit(0).lt(strpos(body(), lit("reset")))]),
            frag("reset", false)
        );
        assert_eq!(
            probe(&[strpos(body(), lit("reset")).gt(lit(1))]),
            Probe::Unknown
        );
        assert_eq!(
            probe(&[has_token(body(), "Timeout")]),
            Probe::token(b"Timeout", false)
        );
        assert_eq!(
            probe(&[has_token(lower(body()), "timeout")]),
            Probe::token(b"timeout", true)
        );
    }

    #[test]
    fn negations_other_columns_and_custom_escapes_prove_nothing() {
        let body = || c("body");
        assert_eq!(probe(&[body().not_like(lit("%timeout%"))]), Probe::Unknown);
        assert_eq!(
            probe(&[Expr::Not(Box::new(body().like(lit("%timeout%"))))]),
            Probe::Unknown
        );
        assert_eq!(
            probe(&[c("severity_text").like(lit("%timeout%"))]),
            Probe::Unknown
        );
        let custom = Expr::Like(Like::new(
            false,
            Box::new(body()),
            Box::new(lit("%a#%b%")),
            Some('#'),
            false,
        ));
        assert_eq!(probe(&[custom]), Probe::Unknown);
        assert_eq!(
            probe(&[body().like(lit("%ab%"))]),
            Probe::Unknown,
            "too short"
        );
        assert_eq!(
            probe(&[body().ilike(lit("%não%"))]),
            Probe::Unknown,
            "not ASCII"
        );
        assert_eq!(probe(&[body().like(lit("%não%"))]), frag("não", false));
    }

    #[test]
    fn and_keeps_known_branches_and_or_needs_every_branch_known() {
        let body = || c("body");
        let timeout = body().like(lit("%timeout%"));
        let reset = body().like(lit("%reset%"));
        let other = c("severity_number").gt(lit(9));
        assert_eq!(
            probe(&[timeout.clone(), other.clone()]),
            frag("timeout", false),
            "a filter on another column is neutral under AND"
        );
        assert_eq!(
            probe(&[timeout.clone().and(reset.clone())]),
            Probe::And(vec![frag("timeout", false), frag("reset", false)])
        );
        assert_eq!(
            probe(&[timeout.clone().or(reset.clone())]),
            Probe::Or(vec![frag("timeout", false), frag("reset", false)])
        );
        assert_eq!(probe(&[timeout.or(other)]), Probe::Unknown);
    }
}
