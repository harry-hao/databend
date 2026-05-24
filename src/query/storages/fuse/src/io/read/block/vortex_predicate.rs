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
use databend_common_expression::FunctionCall;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use vortex::compute::LikeOptions;
use vortex::dtype::DType;
use vortex::dtype::ExtDType;
use vortex::dtype::Nullability;
use vortex::dtype::PType;
use vortex::dtype::datetime::DATE_ID;
use vortex::dtype::datetime::TemporalMetadata;
use vortex::dtype::datetime::TimeUnit;
use vortex::expr::Expression;
use vortex::expr::Like;
use vortex::expr::VTableExt;
use vortex::expr::and;
use vortex::expr::col;
use vortex::expr::eq;
use vortex::expr::gt;
use vortex::expr::gt_eq;
use vortex::expr::is_null;
use vortex::expr::list_contains;
use vortex::expr::lit;
use vortex::expr::lt;
use vortex::expr::lt_eq;
use vortex::expr::not;
use vortex::expr::not_eq;
use vortex::expr::or;
use vortex::scalar::Scalar as VortexScalar;

#[derive(Debug, Clone)]
pub struct VortexPredicateSplit {
    pub scan_filter: Option<Expression>,
    pub residual_filter: Option<Expr>,
}

pub fn split_expr_for_vortex(expr: &Expr, schema: &DataSchema) -> Result<VortexPredicateSplit> {
    Ok(split_expr_for_vortex_inner(expr, schema))
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
                ("or" | "or_filters", [lhs, rhs]) => {
                    let lhs = translate_expr_to_vortex_inner(lhs, schema)?;
                    let rhs = translate_expr_to_vortex_inner(rhs, schema)?;
                    Some(or(lhs, rhs))
                }

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

                ("like", [lhs, rhs]) => {
                    let lhs = translate_expr_to_vortex_inner(lhs, schema)?;
                    let rhs = translate_expr_to_vortex_inner(rhs, schema)?;
                    Some(Like.new_expr(LikeOptions::default(), [lhs, rhs]))
                }
                ("in", [value, rest @ ..]) if !rest.is_empty() => {
                    let value = translate_expr_to_vortex_inner(value, schema)?;
                    let list_elements = rest
                        .iter()
                        .map(vortex_scalar_literal_from_expr)
                        .collect::<Option<Vec<_>>>()?;
                    let element_dtype = list_elements.first()?.dtype().clone();
                    let list = VortexScalar::list(
                        element_dtype,
                        list_elements,
                        Nullability::Nullable,
                    );
                    Some(list_contains(lit(list), value))
                }
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

fn split_expr_for_vortex_inner(expr: &Expr, schema: &DataSchema) -> VortexPredicateSplit {
    if let Expr::FunctionCall(call) = expr {
        let name = call.function.signature.name.as_str();
        if matches!(name, "and" | "and_filters") && call.args.len() == 2 {
            let lhs = split_expr_for_vortex_inner(&call.args[0], schema);
            let rhs = split_expr_for_vortex_inner(&call.args[1], schema);
            return VortexPredicateSplit {
                scan_filter: combine_scan_filters(lhs.scan_filter, rhs.scan_filter),
                residual_filter: combine_residual_filters(call, lhs.residual_filter, rhs.residual_filter),
            };
        }
    }

    let translated = translate_expr_to_vortex_inner(expr, schema);
    let residual_filter = translated.is_none().then(|| expr.clone());
    VortexPredicateSplit {
        scan_filter: translated,
        residual_filter,
    }
}

fn combine_scan_filters(lhs: Option<Expression>, rhs: Option<Expression>) -> Option<Expression> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(and(lhs, rhs)),
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (None, None) => None,
    }
}

fn combine_residual_filters(
    original_call: &FunctionCall,
    lhs: Option<Expr>,
    rhs: Option<Expr>,
) -> Option<Expr> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(Expr::FunctionCall(FunctionCall {
            args: vec![lhs, rhs],
            ..original_call.clone()
        })),
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (None, None) => None,
    }
}

fn vortex_scalar_literal_from_expr(expr: &Expr) -> Option<VortexScalar> {
    match expr {
        Expr::Constant(c) => vortex_scalar_from_databend(&c.scalar, &c.data_type),
        Expr::Cast(c) => vortex_scalar_literal_from_expr(&c.expr),
        _ => None,
    }
}

fn collect_referenced_field_names(
    expr: &Expr,
    schema: &DataSchema,
    out: &mut std::collections::BTreeSet<String>,
) -> Option<()> {
    match expr {
        Expr::Constant(_) => Some(()),
        Expr::ColumnRef(c) => {
            if is_nested_data_type(&c.data_type) {
                return None;
            }
            let name = schema.fields().get(c.id)?.name();
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

fn is_nested_data_type(ty: &DataType) -> bool {
    match ty {
        DataType::Tuple(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Vector(_)
        | DataType::EmptyArray
        | DataType::EmptyMap => true,
        DataType::Nullable(inner) => is_nested_data_type(inner),
        _ => false,
    }
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
    use databend_common_expression::FunctionCall;
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
    use super::split_expr_for_vortex;
    use super::vortex_scalar_from_databend;

    fn function_name(expr: &Expr) -> &str {
        match expr {
            Expr::FunctionCall(FunctionCall { function, .. }) => function.signature.name.as_str(),
            _ => panic!("expected function call, got {expr:?}"),
        }
    }

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
    fn split_and_pushes_supported_subtree_and_keeps_residual() {
        let schema = DataSchema::new(vec![
            DataField::new("a", DataType::Number(NumberDataType::Int64)),
            DataField::new("s", DataType::String),
        ]);
        let a = col_ref(0, "a", DataType::Number(NumberDataType::Int64));
        let a_eq_1 = check_function(None, "eq", &[], &[a, lit_i64(1)], &BUILTIN_FUNCTIONS).unwrap();
        let arithmetic = check_function(
            None,
            "plus",
            &[],
            &[lit_i64(1), lit_i64(2)],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let residual_expr = check_function(
            None,
            "eq",
            &[],
            &[arithmetic, lit_i64(3)],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let expr = check_function(
            None,
            "and_filters",
            &[],
            &[a_eq_1, residual_expr.clone()],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();

        let split = split_expr_for_vortex(&expr, &schema).unwrap();
        assert!(split.scan_filter.is_some());
        let residual = split.residual_filter.expect("expected residual filter");
        assert_eq!(function_name(&residual), "eq");
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
