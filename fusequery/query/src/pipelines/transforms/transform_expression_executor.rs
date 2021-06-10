// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataValue;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::Expression;
use common_planners::ExpressionAction;
use common_planners::ExpressionChain;

/// ExpressionExecutor is a helper struct for expressions and projections
/// Aggregate functions is not covered, because all expressions in aggregate functions functions are executed.
#[derive(Debug, Clone)]
pub struct ExpressionExecutor {
    input_schema: DataSchemaRef,
    output_schema: DataSchemaRef,
    chain: Arc<ExpressionChain>,
    // whether to perform alias action in executor
    alias_project: bool,
}

impl ExpressionExecutor {
    pub fn try_create(
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        exprs: Vec<Expression>,
        alias_project: bool,
    ) -> Result<Self> {
        let chain = ExpressionChain::try_create(input_schema.clone(), &exprs)?;

        Ok(Self {
            input_schema,
            output_schema,
            chain: Arc::new(chain),
            alias_project,
        })
    }

    pub fn validate(&self) -> Result<()> {
        Ok(())
    }

    pub fn execute(&self, block: &DataBlock, exists_res: Option<&HashMap<String, bool>>) -> Result<DataBlock> {
        let mut column_map: HashMap<String, DataColumnarValue> = HashMap::new();

        // a + 1 as b, a + 1 as c
        let mut alias_map: HashMap<String, Vec<String>> = HashMap::new();

        for f in block.schema().fields().iter() {
            column_map.insert(
                f.name().clone(),
                block.try_column_by_name(f.name())?.clone(),
            );
        }
        //println!("exists_res:{:?}", exists_res);
        let rows = block.num_rows();
        if let Some(map) = exists_res {
            for (name, b) in map {
                let b = DataColumnarValue::Constant(DataValue::Boolean(Some(*b)), rows).to_array()?;
                column_map.insert(name.to_string(), DataColumnarValue::Array(b));
            }
        }

        for action in self.chain.actions.iter() {
            if let ExpressionAction::Alias(alias) = action {
                if let Some(v) = alias_map.get_mut(&alias.arg_name) {
                    v.push(alias.name.clone());
                } else {
                    alias_map.insert(alias.arg_name.clone(), vec![alias.name.clone()]);
                }
            }

            if column_map.contains_key(action.column_name()) {
                continue;
            }

            match action {
                ExpressionAction::Input(input) => {
                    let column = block.try_column_by_name(&input.name)?.clone();
                    column_map.insert(input.name.clone(), column);
                }
                ExpressionAction::Function(f) => {
                    // check if it's cached
                    let arg_columns = f
                        .arg_names
                        .iter()
                        .map(|arg| {
                            //println!("arg_name={}", arg);
                            //println!("value={:?}", column_map.get(arg));
                            column_map.get(arg).cloned().ok_or_else(|| {
                                ErrorCodes::LogicalError(
                                    "Arguments must be prepared before function transform",
                                )
                            })
                        })
                        .collect::<Result<Vec<DataColumnarValue>>>()?;

                    let func = f.to_function()?;
                    //println!("arg_columns:{:?}", arg_columns);
                    let column = func.eval(&arg_columns, rows)?;
                    column_map.insert(f.name.clone(), column);
                }
                ExpressionAction::Constant(constant) => {
                    let column = DataColumnarValue::Constant(constant.value.clone(), rows);
                    column_map.insert(constant.name.clone(), column);
                }
                ExpressionAction::Exists(exists) => {
                    println!("exists.name={}, val={:?}", exists.name, column_map.get(&exists.name));
                    //let column = DataColumnarValue::Constant(exists.value.clone(), rows);
                    //column_map.insert(exists.name.clone(), column);
                }
                _ => {}
            }
        }

        if self.alias_project {
            for (k, v) in alias_map.iter() {
                let column = column_map.get(k).cloned().ok_or_else(|| {
                    ErrorCodes::LogicalError("Arguments must be prepared before alias transform")
                })?;

                for name in v.iter() {
                    column_map.insert(name.clone(), column.clone());
                }
            }
        }

        let mut project_columns = Vec::with_capacity(self.output_schema.fields().len());
        for f in self.output_schema.fields() {
            let column = column_map.get(f.name()).ok_or_else(|| {
                ErrorCodes::LogicalError(format!(
                    "Projection column: {} not exists in {:?}, there are bugs!",
                    f.name(),
                    column_map.keys()
                ))
            })?;
            project_columns.push(column.clone());
        }
        // projection to remove unused columns
        Ok(DataBlock::create(
            self.output_schema.clone(),
            project_columns,
        ))
    }
}
