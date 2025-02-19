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

use databend_common_ast::ast::TableIndexType;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_types::MetaId;
use databend_storages_common_table_meta::meta::Location;

use crate::plans::Plan;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIndexPlan {
    pub create_option: CreateOption,
    pub index_type: TableIndexType,
    pub index_name: String,
    pub original_query: String,
    pub query: String,
    pub table_id: MetaId,
    pub sync_creation: bool,
}

/// Drop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropIndexPlan {
    pub if_exists: bool,
    pub index: String,
}

#[derive(Clone, Debug)]
pub struct RefreshIndexPlan {
    pub index_id: u64,
    pub index_name: String,
    pub index_meta: IndexMeta,
    pub limit: Option<u64>,
    pub table_info: TableInfo,
    pub query_plan: Box<Plan>,
    pub segment_locs: Option<Vec<Location>>,
    pub user_defined_block_name: bool,
}
