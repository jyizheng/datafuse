// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::find_exists_exprs;
use common_planners::Expression;
use common_planners::FilterPlan;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_planners::SubqueryResult;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::TryStreamExt;

use crate::interpreters::plan_scheduler::PlanScheduler;
use crate::interpreters::IInterpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizer;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;

pub struct SelectInterpreter {
    ctx: FuseQueryContextRef,
    select: SelectPlan,
}

impl SelectInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, select: SelectPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(SelectInterpreter { ctx, select }))
    }
}

fn get_filter_plan(plan: PlanNode) -> Result<FilterPlan> {
    let mut res = Err(ErrorCode::Ok("Not filter plan found"));
    plan.walk_preorder(|node| -> Result<bool> {
        match node {
            PlanNode::Filter(ref filter_plan) => {
                res = Ok(filter_plan.clone());
                Ok(false)
            }
            _ => Ok(true),
        }
    })?;
    return res;
}

async fn execute_one_select(
    ctx: FuseQueryContextRef,
    plan: PlanNode,
    subquery_res: SubqueryResult,
) -> Result<SendableDataBlockStream> {
    let scheduled_actions = PlanScheduler::reschedule(ctx.clone(), Some(subquery_res), &plan)?;

    let remote_actions_ref = &scheduled_actions.remote_actions;
    let prepare_error_handler = move |error: ErrorCode, end: usize| {
        let mut killed_set = HashSet::new();
        for (node, _) in remote_actions_ref.iter().take(end) {
            if killed_set.get(&node.name).is_none() {
                // TODO: ISSUE-204 kill prepared query stage
                killed_set.insert(node.name.clone());
            }
        }

        Result::Err(error)
    };

    let timeout = ctx.get_flight_client_timeout()?;
    for (index, (node, action)) in scheduled_actions.remote_actions.iter().enumerate() {
        let mut flight_client = node.get_flight_client().await?;
        if let Err(error) = flight_client
            .prepare_query_stage(action.clone(), timeout)
            .await
        {
            return prepare_error_handler(error, index);
        }
    }

    PipelineBuilder::create(
        ctx.clone(),
        Some(subquery_res),
        scheduled_actions.local_plan.clone(),
    )
    .build()?
    .execute()
    .await
}

#[async_trait::async_trait]
impl IInterpreter for SelectInterpreter {
    fn name(&self) -> &str {
        "SelectInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.select.schema()
    }

    #[tracing::instrument(level = "info", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let plan = Optimizer::create(self.ctx.clone()).optimize(&self.select.input)?;
        // Subquery Plan Name : Exists Expression Name
        let mut names = HashMap::<String, String>::new();
        // The execution order is from the bottom to the top
        let mut levels = Vec::<Vec<PlanNode>>::new();
        // The queue for the current level
        let mut queue1 = VecDeque::<PlanNode>::new();
        // The queue for the next level
        let mut queue2 = VecDeque::<PlanNode>::new();

        queue1.push_back(plan.clone());

        while queue1.len() > 0 {
            let mut one_level = Vec::<PlanNode>::new();
            while queue1.len() > 0 {
                if let Some(begin) = queue1.pop_front() {
                    if let Ok(p) = get_filter_plan(begin) {
                        let exists_vec = find_exists_exprs(&[p.predicate.clone()]);
                        for exst in exists_vec {
                            let expr_name = exst.column_name();
                            if let Expression::Exists(p) = exst {
                                queue2.push_back((*p).clone());
                                one_level.push((*p).clone());
                                names.insert(format!("{:?}", p), expr_name);
                            }
                        }
                    }
                }
            }
            if one_level.len() > 0 {
                levels.push(one_level);
            }
            queue1 = VecDeque::from(queue2);
            queue2 = VecDeque::<PlanNode>::new();
        }

        let mut subquery_res = SubqueryResult::new();
        let size = levels.len();
        for i in (0..size).rev() {
            let ex_plans = &levels[i];
            for exp in ex_plans {
                let stream =
                    execute_one_select(self.ctx.clone(), exp.clone(), subquery_res.clone())
                        .await?;
                let result = stream.try_collect::<Vec<_>>().await?;
                //let b = if result.len() > 0 { true } else { false };
                let name = names.get(exp.column_name());
                subquery_res.insert(name.unwrap().to_string(), result[0]);
            }
        }
        execute_one_select(self.ctx.clone(), plan, subquery_res).await
    }
}
