// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;
use std::collections::HashMap;

use common_datavalues::DataSchemaRef;
use common_datablocks::DataBlock;

use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct SelectPlan {
    pub input: Arc<PlanNode>,
}

impl SelectPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.input.schema()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SubqueryResult {
    blocks: HashMap<String, DataBlock>,
}

impl SubqueryResult {
    pub fn new()-> SubqueryResult {
        SubqueryResult {
            blocks: HashMap::<String, DataBlock>::new(),
        }
    }
}
