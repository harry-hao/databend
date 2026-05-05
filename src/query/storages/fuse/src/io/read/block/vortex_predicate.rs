// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use vortex::dtype::DType;
use vortex::dtype::ExtDType;
use vortex::dtype::Nullability;
use vortex::dtype::PType;
use vortex::dtype::datetime::DATE_ID;
use vortex::dtype::datetime::TemporalMetadata;
use vortex::dtype::datetime::TimeUnit;
use vortex::expr::Expression;
use vortex::expr::and;
use vortex::expr::col;
use vortex::expr::eq;
use vortex::expr::gt;
use vortex::expr::gt_eq;
use vortex::expr::is_null;
use vortex::expr::lit;
use vortex::expr::lt;
use vortex::expr::lt_eq;
use vortex::expr::not;
use vortex::expr::not_eq;
use vortex::scalar::Scalar as VortexScalar;

/// Translate a Databend scalar `Expr` into a Vortex [`Expression`] for filter pushdown.
///
/// This is intentionally conservative:
/// - Returns `Ok(None)` if any part of the expression is unsupported.
/// - Supported subset: AND, comparisons (=, !=, <, <=, >, >=), `is_null`, `is_not_null`.
/// - Explicitly not supported: OR, IN, LIKE, casts, and all other functions/operators.
#[allow(dead_code)]
pub fn translate_expr_to_vortex(expr: &Expr, schema: &DataSchema) -> Result<Option<Expression>> {
    Ok(translate_expr_to_vortex_inner(expr, schema))
}

pub fn referenced_field_names(
    expr: &Expr,
    schema: &DataSchema,
) -> Option<vortex::dtype::FieldNames> {
    use std::collections::BTreeSet;
    let mut set = BTreeSet::<String>::new();
    collect_referenced_field_names(expr, schema, &mut set)?;
    let strs = set.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    Some(vortex::dtype::FieldNames::from_iter(strs))
}

fn translate_expr_to_vortex_inner(expr: &Expr, schema: &DataSchema) -> Option<Expression> {
    match expr {
        Expr::Constant(c) => {
            let scalar = vortex_scalar_from_databend(&c.scalar, &c.data_type)?;
            Some(lit(scalar))
        }
        Expr::ColumnRef(c) => {
            let name = schema.fields().get(c.id)?.name();
            Some(col(name.as_str()))
        }
        Expr::FunctionCall(call) => {
            let name = call.function.signature.name.as_str();
            match (name, call.args.as_slice()) {
                ("and" | "and_filters", [lhs, rhs]) => {
                    let lhs = translate_expr_to_vortex_inner(lhs, schema)?;
                    let rhs = translate_expr_to_vortex_inner(rhs, schema)?;
                    Some(and(lhs, rhs))
                }

                // Explicitly reject OR (even though Vortex supports it) for a conservative phase.
                ("or" | "or_filters", _) => None,

                // Unary null checks.
                ("is_null", [child]) => {
                    let child = translate_expr_to_vortex_inner(child, schema)?;
                    Some(is_null(child))
                }
                ("is_not_null", [child]) => {
                    let child = translate_expr_to_vortex_inner(child, schema)?;
                    Some(not(is_null(child)))
                }

                // Binary comparisons.
                ("eq" | "=", [lhs, rhs]) => {
                    translate_binary_compare(lhs, rhs, schema, eq)
                }
                ("ne" | "noteq" | "!=", [lhs, rhs]) => {
                    translate_binary_compare(lhs, rhs, schema, not_eq)
                }
                ("lt" | "<", [lhs, rhs]) => {
                    translate_binary_compare(lhs, rhs, schema, lt)
                }
                ("le" | "lte" | "<=", [lhs, rhs]) => {
                    translate_binary_compare(lhs, rhs, schema, lt_eq)
                }
                ("gt" | ">", [lhs, rhs]) => {
                    translate_binary_compare(lhs, rhs, schema, gt)
                }
                ("ge" | "gte" | ">=", [lhs, rhs]) => {
                    translate_binary_compare(lhs, rhs, schema, gt_eq)
                }

                // Explicitly reject LIKE / IN and everything else.
                ("like" | "in", _) => None,
                _ => None,
            }
        }
        // Planner/type-check may introduce casts. For Phase2 pushdown we conservatively *ignore*
        // the cast wrapper and translate the inner expression.
        Expr::Cast(c) => translate_expr_to_vortex_inner(&c.expr, schema),
        Expr::LambdaFunctionCall(_) => None,
    }
}

fn translate_binary_compare<F>(
    lhs: &Expr,
    rhs: &Expr,
    schema: &DataSchema,
    build: F,
) -> Option<Expression>
where
    F: Fn(Expression, Expression) -> Expression,
{
    let lhs = translate_expr_to_vortex_inner(lhs, schema)?;
    let rhs = translate_expr_to_vortex_inner(rhs, schema)?;
    Some(build(lhs, rhs))
}

fn collect_referenced_field_names(
    expr: &Expr,
    schema: &DataSchema,
    out: &mut std::collections::BTreeSet<String>,
) -> Option<()> {
    match expr {
        Expr::Constant(_) => Some(()),
        Expr::ColumnRef(c) => {
            let name = schema.fields().get(c.id)?.name();
            let display = c.display_name.as_str();
            if is_nested_column_ref(display, name) {
                return None;
            }
            out.insert(name.to_string());
            Some(())
        }
        Expr::FunctionCall(call) => {
            for arg in &call.args {
                collect_referenced_field_names(arg, schema, out)?;
            }
            Some(())
        }
        Expr::Cast(c) => collect_referenced_field_names(&c.expr, schema, out),
        Expr::LambdaFunctionCall(_) => None,
    }
}

fn is_nested_display_name(display_name: &str) -> bool {
    // Conservative: reject common nested syntaxes and deref notations.
    display_name.contains('.')
        || display_name.contains(':')
        || display_name.contains('[')
        || display_name.contains(']')
        || display_name.contains('(')
        || display_name.contains(')')
}

fn is_nested_column_ref(display_name: &str, schema_name: &str) -> bool {
    // Accept common qualified display forms like `table.col` and `table.col (#id)`.
    // Nested references should still be rejected.
    let normalized = display_name
        .split_once(" (#")
        .map(|(prefix, _)| prefix)
        .unwrap_or(display_name)
        .trim();

    if normalized == schema_name {
        return false;
    }

    if normalized.ends_with(schema_name) {
        let prefix_len = normalized.len().saturating_sub(schema_name.len());
        let prefix = &normalized[..prefix_len];
        if prefix.ends_with('.') {
            let qualifier = &prefix[..prefix.len().saturating_sub(1)];
            // Treat one-identifier qualifier (`table.col` or `alias.col`) as non-nested.
            if !qualifier.is_empty() && !qualifier.contains('.') {
                return false;
            }
        }
    }

    is_nested_display_name(normalized)
}

fn vortex_date_ext_dtype(nullability: Nullability) -> Arc<ExtDType> {
    Arc::new(ExtDType::new(
        DATE_ID.clone(),
        Arc::new(DType::Primitive(PType::I32, nullability)),
        Some(TemporalMetadata::Date(TimeUnit::Days).into()),
    ))
}

fn vortex_scalar_from_databend(s: &Scalar, ty: &DataType) -> Option<VortexScalar> {
    let nullability = if matches!(s, Scalar::Null) {
        Nullability::Nullable
    } else {
        Nullability::NonNullable
    };

    match s {
        Scalar::Null => {
            let dtype = vortex_dtype_from_databend(ty, Nullability::Nullable)?;
            Some(VortexScalar::null(dtype))
        }
        Scalar::Boolean(v) => Some(VortexScalar::bool(*v, nullability)),
        Scalar::String(v) => Some(VortexScalar::utf8(v.clone(), nullability)),
        Scalar::Binary(v) => Some(VortexScalar::binary(v.clone(), nullability)),
        Scalar::Date(v) => {
            let ext_dtype = vortex_date_ext_dtype(nullability);
            let storage = VortexScalar::primitive(*v, nullability);
            Some(VortexScalar::extension(ext_dtype, storage))
        }
        Scalar::Number(n) => match n {
            NumberScalar::Int8(v) => Some(VortexScalar::primitive(*v, nullability)),
            NumberScalar::Int16(v) => Some(VortexScalar::primitive(*v, nullability)),
            NumberScalar::Int32(v) => Some(VortexScalar::primitive(*v, nullability)),
            NumberScalar::Int64(v) => Some(VortexScalar::primitive(*v, nullability)),
            NumberScalar::UInt8(v) => Some(VortexScalar::primitive(*v, nullability)),
            NumberScalar::UInt16(v) => Some(VortexScalar::primitive(*v, nullability)),
            NumberScalar::UInt32(v) => Some(VortexScalar::primitive(*v, nullability)),
            NumberScalar::UInt64(v) => Some(VortexScalar::primitive(*v, nullability)),
            NumberScalar::Float32(v) => Some(VortexScalar::primitive(**v, nullability)),
            NumberScalar::Float64(v) => Some(VortexScalar::primitive(**v, nullability)),
        },
        _ => None,
    }
}

fn vortex_dtype_from_databend(ty: &DataType, nullability: Nullability) -> Option<DType> {
    match ty.remove_nullable() {
        DataType::Null => Some(DType::Null),
        DataType::Boolean => Some(DType::Bool(nullability)),
        DataType::Binary => Some(DType::Binary(nullability)),
        DataType::String => Some(DType::Utf8(nullability)),
        DataType::Date => Some(DType::Extension(vortex_date_ext_dtype(nullability))),
        DataType::Number(n) => {
            let p = match n {
                databend_common_expression::types::NumberDataType::Int8 => PType::I8,
                databend_common_expression::types::NumberDataType::Int16 => PType::I16,
                databend_common_expression::types::NumberDataType::Int32 => PType::I32,
                databend_common_expression::types::NumberDataType::Int64 => PType::I64,
                databend_common_expression::types::NumberDataType::UInt8 => PType::U8,
                databend_common_expression::types::NumberDataType::UInt16 => PType::U16,
                databend_common_expression::types::NumberDataType::UInt32 => PType::U32,
                databend_common_expression::types::NumberDataType::UInt64 => PType::U64,
                databend_common_expression::types::NumberDataType::Float32 => PType::F32,
                databend_common_expression::types::NumberDataType::Float64 => PType::F64,
            };
            Some(DType::Primitive(p, nullability))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::Constant;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchema;
    use databend_common_expression::Expr;
    use databend_common_expression::Scalar;
    use databend_common_expression::type_check::check_function;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_functions::BUILTIN_FUNCTIONS;
    use vortex::dtype::DType;

    use super::DATE_ID;
    use super::referenced_field_names;
    use super::translate_expr_to_vortex;
    use super::vortex_scalar_from_databend;

    fn col_ref(id: usize, display_name: &str, data_type: DataType) -> Expr {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id,
            data_type,
            display_name: display_name.to_string(),
        })
    }

    fn lit_i64(v: i64) -> Expr {
        Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Number(NumberScalar::Int64(v)),
            data_type: DataType::Number(NumberDataType::Int64),
        })
    }

    fn lit_str(v: &str) -> Expr {
        Expr::Constant(Constant {
            span: None,
            scalar: Scalar::String(v.to_string()),
            data_type: DataType::String,
        })
    }

    fn lit_date(v: i32) -> Expr {
        Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Date(v),
            data_type: DataType::Date,
        })
    }

    fn schema_ab_i64() -> DataSchema {
        DataSchema::new(vec![
            DataField::new("a", DataType::Number(NumberDataType::Int64)),
            DataField::new("b", DataType::Number(NumberDataType::Int64)),
        ])
    }

    #[test]
    fn supported_and_eq_is_not_null_translate_some() {
        let schema = schema_ab_i64();
        let a = col_ref(0, "a", DataType::Number(NumberDataType::Int64));
        let b = col_ref(1, "b", DataType::Number(NumberDataType::Int64));

        let eq_a_1 = check_function(None, "eq", &[], &[a, lit_i64(1)], &BUILTIN_FUNCTIONS).unwrap();
        let b_not_null =
            check_function(None, "is_not_null", &[], &[b], &BUILTIN_FUNCTIONS).unwrap();
        let and_expr = check_function(
            None,
            "and_filters",
            &[],
            &[eq_a_1, b_not_null],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();

        let translated = translate_expr_to_vortex(&and_expr, &schema).unwrap();
        let translated = translated.expect("expected Some(Expression)");
        let s = translated.to_string();
        assert!(s.contains("$.a"), "expr: {s}");
        assert!(s.contains("$.b"), "expr: {s}");
        assert!(s.contains("and"), "expr: {s}");
    }

    #[test]
    fn unsupported_or_translates_none() {
        let schema = schema_ab_i64();
        let a = col_ref(0, "a", DataType::Number(NumberDataType::Int64));
        let b = col_ref(1, "b", DataType::Number(NumberDataType::Int64));
        let a_eq_1 = check_function(None, "eq", &[], &[a, lit_i64(1)], &BUILTIN_FUNCTIONS).unwrap();
        let b_eq_2 = check_function(None, "eq", &[], &[b, lit_i64(2)], &BUILTIN_FUNCTIONS).unwrap();
        let expr = check_function(
            None,
            "or_filters",
            &[],
            &[a_eq_1, b_eq_2],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();

        let translated = translate_expr_to_vortex(&expr, &schema).unwrap();
        assert!(translated.is_none());
    }

    #[test]
    fn unsupported_like_translates_none() {
        // use a string-typed column
        let schema = DataSchema::new(vec![DataField::new("s", DataType::String)]);
        let s = col_ref(0, "s", DataType::String);
        let expr =
            check_function(None, "like", &[], &[s, lit_str("%x%")], &BUILTIN_FUNCTIONS).unwrap();

        let translated = translate_expr_to_vortex(&expr, &schema).unwrap();
        assert!(translated.is_none());
    }

    #[test]
    fn nested_column_ref_display_name_translates_some() {
        let schema = schema_ab_i64();
        let a_nested = col_ref(0, "a.b", DataType::Number(NumberDataType::Int64));
        let expr =
            check_function(None, "eq", &[], &[a_nested, lit_i64(1)], &BUILTIN_FUNCTIONS).unwrap();

        let translated = translate_expr_to_vortex(&expr, &schema).unwrap();
        let translated = translated.expect("expected Some(Expression)");
        let s = translated.to_string();
        assert!(s.contains("$.a"), "expr: {s}");
    }

    #[test]
    fn qualified_display_name_translates_some() {
        let schema = schema_ab_i64();
        let a = col_ref(0, "t.a (#0)", DataType::Number(NumberDataType::Int64));
        let expr = check_function(None, "eq", &[], &[a, lit_i64(1)], &BUILTIN_FUNCTIONS).unwrap();

        let translated = translate_expr_to_vortex(&expr, &schema).unwrap();
        let translated = translated.expect("expected Some(Expression)");
        let s = translated.to_string();
        assert!(s.contains("$.a"), "expr: {s}");
    }

    #[test]
    fn qualified_display_name_referenced_field_name_ok() {
        let schema = schema_ab_i64();
        let a = col_ref(0, "t.a (#0)", DataType::Number(NumberDataType::Int64));
        let expr = check_function(None, "eq", &[], &[a, lit_i64(1)], &BUILTIN_FUNCTIONS).unwrap();

        let names = referenced_field_names(&expr, &schema).expect("expected referenced fields");
        assert_eq!(names.iter().map(|n| n.as_ref()).collect::<Vec<_>>(), vec!["a"]);
    }

    #[test]
    fn date_literal_ge_translates_some() {
        let schema = DataSchema::new(vec![DataField::new("d", DataType::Date)]);
        let d = col_ref(0, "d", DataType::Date);
        let expr = check_function(None, "ge", &[], &[d, lit_date(15887)], &BUILTIN_FUNCTIONS)
            .unwrap();

        let translated = translate_expr_to_vortex(&expr, &schema).unwrap();
        assert!(translated.is_some());
    }

    #[test]
    fn date_scalar_uses_extension_dtype() {
        let scalar = vortex_scalar_from_databend(&Scalar::Date(15887), &DataType::Date)
            .expect("expected date scalar conversion");
        let dtype = scalar.dtype();
        match dtype {
            DType::Extension(ext) => {
                assert_eq!(ext.id(), &*DATE_ID);
            }
            _ => panic!("expected extension dtype, got {dtype}"),
        }
    }
}
