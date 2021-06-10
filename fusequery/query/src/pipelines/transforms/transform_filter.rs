// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::convert::TryInto;
use std::sync::Arc;
use std::collections::HashMap;
use futures::TryStreamExt;

use common_arrow::arrow;
use common_datablocks::DataBlock;
use common_datavalues as datavalues;
use common_datavalues::BooleanArray;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::Expression;
use common_planners::find_exists_exprs;
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;

pub struct FilterTransform {
    ctx: FuseQueryContextRef,
    input: Arc<dyn IProcessor>,
    executor: Arc<ExpressionExecutor>,
    predicate: Expression,
    having: bool,
}

impl FilterTransform {
    pub fn try_create(ctx: FuseQueryContextRef, schema: DataSchemaRef, predicate: Expression, having: bool) -> Result<Self> {
        let mut fields = schema.fields().clone();
        fields.push(predicate.to_data_field(&schema)?);

        let executor = ExpressionExecutor::try_create(
            schema,
            DataSchemaRefExt::create(fields),
            vec![predicate.clone()],
            false,
        )?;
        executor.validate()?;

        Ok(FilterTransform {
            ctx: ctx,
            input: Arc::new(EmptyProcessor::create()),
            executor: Arc::new(executor),
            predicate,
            having,
        })
    }
}

#[async_trait::async_trait]
impl IProcessor for FilterTransform {
    fn name(&self) -> &str {
        if self.having {
            return "HavingTransform";
        }
        "FilterTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        println!("filter_predicate: {:?}", self.predicate);

        let exists_vec = find_exists_exprs(&[self.predicate.clone()]);
        let mut exists_res = HashMap::new();

        for exst in exists_vec {
            let name = format!("{:?}", exst);
            if let  Expression::Exists(p) = exst {
                let mut exst_pipeline = PipelineBuilder::create(self.ctx.clone(), (*p).clone()).build();
                match exst_pipeline {
                    Err(e) => panic!("do not expect this to happed"),
                    _ => (),
                }
                let stream = exst_pipeline.unwrap().execute().await?;
                let result = stream.try_collect::<Vec<_>>().await?;
                println!("len: {:?}", result.len());
                let b = if result.len() > 0 {
                    true
                } else {
                    false
                };
                exists_res.insert(name, b);
            }
        }
        println!("after for loop");
        let input_stream = self.input.execute().await?;
        //let result = input_stream.try_collect::<Vec<_>>().await?;
        //println!("len: {:?}", result.len());

        let executor = self.executor.clone();
        let column_name = self.predicate.column_name();
        let exists_map = exists_res.clone();

        let execute_fn = |executor: Arc<ExpressionExecutor>,
                          exists_map: &HashMap::<String, bool>,
                          column_name: &str,
                          block: Result<DataBlock>|
         -> Result<DataBlock> {
            let block = block?;
            let filter_block = executor.execute(&block, Some(exists_map))?;
            let filter_array = filter_block.try_column_by_name(column_name)?.to_array()?;
            // Downcast to boolean array
            let filter_array = datavalues::downcast_array!(filter_array, BooleanArray)?;

            // Convert to arrow record_batch
            let batch = block.try_into()?;
            let batch = arrow::compute::filter_record_batch(&batch, filter_array)?;
            batch.try_into()
        };
        let stream = input_stream.filter_map(move |v| {
            println!("v={:?}", v);
            execute_fn(executor.clone(), &exists_map, &column_name, v)
                .map(Some)
                .transpose()
        });
        println!("after for filter execute");
        Ok(Box::pin(stream))
    }
}
