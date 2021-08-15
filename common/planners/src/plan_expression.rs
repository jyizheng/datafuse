// Copyright 2020 Datafuse Labs.
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

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::aggregates::AggregateFunctionRef;
use common_functions::scalars::FunctionFactory;
use lazy_static::lazy_static;

//use crate::InListExpr;
use crate::PlanNode;

lazy_static! {
    static ref OP_SET: HashSet<&'static str> = ["database", "version",].iter().copied().collect();
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct ExpressionPlan {
    pub exprs: Vec<Expression>,
    pub schema: DataSchemaRef,
    pub input: Arc<PlanNode>,
    pub desc: String,
}

impl ExpressionPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum Expression {
    /// An expression with a alias name.
    Alias(String, Box<Expression>),
    /// Column name.
    Column(String),
    /// select number from t where number in (1, 3, 5)
    //InList(InListExpr),
    /// Note: When literal represents a column, its column_name will not be None
    Literal {
        value: DataValue,
        column_name: Option<String>,
    },
    /// A unary expression such as "NOT foo"
    UnaryExpression { op: String, expr: Box<Expression> },

    /// A binary expression such as "age > 40"
    BinaryExpression {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },

    /// ScalarFunction with a set of arguments.
    /// Note: BinaryFunction is a also kind of functions function
    ScalarFunction { op: String, args: Vec<Expression> },

    /// AggregateFunction with a set of arguments.
    AggregateFunction {
        op: String,
        distinct: bool,
        args: Vec<Expression>,
    },

    /// A sort expression, that can be used to sort values.
    Sort {
        /// The expression to sort on
        expr: Box<Expression>,
        /// The direction of the sort
        asc: bool,
        /// Whether to put Nulls before all other data values
        nulls_first: bool,
    },
    /// All fields(*) in a schema.
    Wildcard,
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast {
        /// The expression being cast
        expr: Box<Expression>,
        /// The `DataType` the expression will yield
        data_type: DataType,
    },
    /// Scalar sub query. such as `SELECT (SELECT 1)`
    ScalarSubquery {
        name: String,
        query_plan: Arc<PlanNode>,
    },
    Subquery {
        name: String,
        query_plan: Arc<PlanNode>,
    },
}

impl Expression {
    pub fn create_literal(value: DataValue) -> Expression {
        Expression::Literal {
            value,
            column_name: None,
        }
    }

    pub fn column_name(&self) -> String {
        match self {
            Expression::Alias(name, _expr) => name.clone(),
            //Expression::InList(_) => {
            //    let mut hasher = DefaultHasher::new();
            //    let name = format!("{:?}", self);
            //    name.hash(&mut hasher);
            //    let hsh = hasher.finish();
            //    format!("InList_{}", hsh)
            //}
            Expression::Column(name) => name.clone(),
            Expression::Literal {
                column_name: Some(name),
                ..
            } => name.clone(),
            Expression::UnaryExpression { op, expr } => {
                format!("({} {})", op, expr.column_name())
            }
            Expression::BinaryExpression { op, left, right } => {
                format!("({} {} {})", left.column_name(), op, right.column_name())
            }
            Expression::ScalarFunction { op, args } => {
                match OP_SET.get(&op.to_lowercase().as_ref()) {
                    Some(_) => format!("{}()", op),
                    None => {
                        let args_column_name =
                            args.iter().map(Expression::column_name).collect::<Vec<_>>();

                        format!("{}({})", op, args_column_name.join(", "))
                    }
                }
            }
            Expression::AggregateFunction { op, distinct, args } => {
                let args_column_name = args.iter().map(Expression::column_name).collect::<Vec<_>>();

                match distinct {
                    true => format!("{}(distinct {})", op, args_column_name.join(", ")),
                    false => format!("{}({})", op, args_column_name.join(", ")),
                }
            }
            Expression::Sort { expr, .. } => expr.column_name(),
            Expression::Cast { expr, data_type } => {
                format!("cast({} as {:?})", expr.column_name(), data_type)
            }
            Expression::Subquery { name, .. } => name.clone(),
            Expression::ScalarSubquery { name, .. } => name.clone(),
            _ => format!("{:?}", self),
        }
    }

    pub fn to_data_field(&self, input_schema: &DataSchemaRef) -> Result<DataField> {
        let name = self.column_name();
        self.to_data_type(input_schema).and_then(|return_type| {
            self.nullable(input_schema)
                .map(|nullable| DataField::new(&name, return_type, nullable))
        })
    }

    // TODO
    pub fn nullable(&self, _input_schema: &DataSchemaRef) -> Result<bool> {
        Ok(false)
    }

    pub fn to_subquery_type(subquery_plan: &PlanNode) -> DataType {
        let subquery_schema = subquery_plan.schema();
        let mut columns_field = Vec::with_capacity(subquery_schema.fields().len());

        for column_field in subquery_schema.fields() {
            columns_field.push(DataField::new(
                column_field.name(),
                DataType::List(Box::new(DataField::new(
                    "item",
                    column_field.data_type().clone(),
                    true,
                ))),
                false,
            ));
        }

        match columns_field.len() {
            1 => columns_field[0].data_type().clone(),
            _ => DataType::Struct(columns_field),
        }
    }

    pub fn to_scalar_subquery_type(subquery_plan: &PlanNode) -> DataType {
        let subquery_schema = subquery_plan.schema();

        match subquery_schema.fields().len() {
            1 => subquery_schema.field(0).data_type().clone(),
            _ => DataType::Struct(subquery_schema.fields().clone()),
        }
    }

    pub fn to_data_type(&self, input_schema: &DataSchemaRef) -> Result<DataType> {
        match self {
            Expression::Alias(_, expr) => expr.to_data_type(input_schema),
            Expression::Column(s) => Ok(input_schema.field_with_name(s)?.data_type().clone()),
            //Expression::InList(inlist_expr) => inlist_expr.expr().to_data_type(input_schema),
            Expression::Literal { value, .. } => Ok(value.data_type()),
            Expression::Subquery { query_plan, .. } => Ok(Self::to_subquery_type(query_plan)),
            Expression::ScalarSubquery { query_plan, .. } => {
                Ok(Self::to_scalar_subquery_type(query_plan))
            }
            Expression::BinaryExpression { op, left, right } => {
                let arg_types = vec![
                    left.to_data_type(input_schema)?,
                    right.to_data_type(input_schema)?,
                ];
                let func = FunctionFactory::get(op)?;
                func.return_type(&arg_types)
            }

            Expression::UnaryExpression { op, expr } => {
                let arg_types = vec![expr.to_data_type(input_schema)?];
                let func = FunctionFactory::get(op)?;
                func.return_type(&arg_types)
            }

            Expression::ScalarFunction { op, args } => {
                let mut arg_types = Vec::with_capacity(args.len());
                for arg in args {
                    arg_types.push(arg.to_data_type(input_schema)?);
                }
                let func = FunctionFactory::get(op)?;
                func.return_type(&arg_types)
            }
            Expression::AggregateFunction { .. } => {
                let func = self.to_aggregate_function(input_schema)?;
                func.return_type()
            }
            Expression::Wildcard => Result::Err(ErrorCode::IllegalDataType(
                "Wildcard expressions are not valid to get return type",
            )),
            Expression::Cast { data_type, .. } => Ok(data_type.clone()),
            Expression::Sort { expr, .. } => expr.to_data_type(input_schema),
        }
    }

    pub fn to_aggregate_function(&self, schema: &DataSchemaRef) -> Result<AggregateFunctionRef> {
        match self {
            Expression::AggregateFunction { op, distinct, args } => {
                let mut func_name = op.clone();
                if *distinct {
                    func_name += "Distinct";
                }

                let mut fields = Vec::with_capacity(args.len());
                for arg in args.iter() {
                    fields.push(arg.to_data_field(schema)?);
                }
                AggregateFunctionFactory::get(&func_name, fields)
            }
            _ => Err(ErrorCode::LogicalError(
                "Expression must be aggregated function",
            )),
        }
    }

    pub fn to_aggregate_function_names(&self) -> Result<Vec<String>> {
        match self {
            Expression::AggregateFunction { args, .. } => {
                let mut names = Vec::with_capacity(args.len());
                for arg in args.iter() {
                    names.push(arg.column_name());
                }
                Ok(names)
            }
            _ => Err(ErrorCode::LogicalError(
                "Expression must be aggregated function",
            )),
        }
    }
}

// Also used as expression column name
impl fmt::Debug for Expression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expression::Alias(alias, v) => write!(f, "{:?} as {:#}", v, alias),
            Expression::Column(ref v) => write!(f, "{:#}", v),
            Expression::Literal { ref value, .. } => write!(f, "{:#}", value),
            Expression::Subquery { name, .. } => write!(f, "subquery({})", name),
            Expression::ScalarSubquery { name, .. } => write!(f, "scalar subquery({})", name),
            Expression::BinaryExpression { op, left, right } => {
                write!(f, "({:?} {} {:?})", left, op, right,)
            }

            Expression::UnaryExpression { op, expr } => {
                write!(f, "({} {:?})", op, expr)
            }
            //Expression::InList(inlist_expr) => {
            //    if inlist_expr.negated() {
            //        write!(f, "Not ")?;
            //    }
            //    write!(
            //        f,
            //        "({:?} In ({:?}))",
            //        inlist_expr.expr(),
            //        inlist_expr.list()
            //    )
            //}
            Expression::ScalarFunction { op, args } => {
                write!(f, "{}(", op)?;

                for (i, _) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i],)?;
                }
                write!(f, ")")
            }

            Expression::AggregateFunction { op, distinct, args } => {
                write!(f, "{}(", op)?;
                if *distinct {
                    write!(f, "distinct ")?;
                }
                for (i, _) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i],)?;
                }
                write!(f, ")")
            }

            Expression::Sort { expr, .. } => write!(f, "{:?}", expr),
            Expression::Wildcard => write!(f, "*"),
            Expression::Cast { expr, data_type } => {
                write!(f, "cast({:?} as {:?})", expr, data_type)
            }
        }
    }
}

pub type Expressions = Vec<Expression>;
