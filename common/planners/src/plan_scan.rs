// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataSchemaRef;

use crate::ExpressionPlan;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ScanPlan {
    /// The name of the schema
    pub schema_name: String,

    /// The schema of the source data
    pub table_schema: DataSchemaRef,

    pub table_args: Option<ExpressionPlan>,

    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,

    /// The schema description of the output
    pub projected_schema: DataSchemaRef,

    /// Optional limit to skip items
    pub limit: Option<usize>,
}

impl ScanPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.projected_schema.clone()
    }
}
