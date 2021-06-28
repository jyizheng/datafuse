// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::VecDeque;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::Part;
use common_planners::Partitions;
use common_planners::Statistics;
use common_progress::Progress;
use common_progress::ProgressCallback;
use common_progress::ProgressValues;
use common_runtime::tokio::task::JoinHandle;
use common_runtime::Runtime;
use uuid::Uuid;

use crate::clusters::Cluster;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::datasources::DataSource;
use crate::datasources::Table;
use crate::datasources::TableFunction;
use crate::sessions::Settings;

#[derive(Clone)]
pub struct FuseQueryContext {
    conf: Config,
    uuid: Arc<RwLock<String>>,
    settings: Arc<Settings>,
    cluster: Arc<RwLock<ClusterRef>>,
    datasource: Arc<DataSource>,
    statistics: Arc<RwLock<Statistics>>,
    partition_queue: Arc<RwLock<VecDeque<Part>>>,
    current_database: Arc<RwLock<String>>,
    progress: Arc<Progress>,
    runtime: Arc<RwLock<Runtime>>,
    version: String,
    subquery_res_map: HashMap<String, bool>,
}

pub type FuseQueryContextRef = Arc<FuseQueryContext>;

impl FuseQueryContext {
    pub fn try_create(conf: Config) -> Result<FuseQueryContextRef> {
        let settings = Settings::try_create()?;
        let ctx = FuseQueryContext {
            conf,
            uuid: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            settings: settings.clone(),
            cluster: Arc::new(RwLock::new(Cluster::empty())),
            datasource: Arc::new(DataSource::try_create()?),
            statistics: Arc::new(RwLock::new(Statistics::default())),
            partition_queue: Arc::new(RwLock::new(VecDeque::new())),
            current_database: Arc::new(RwLock::new(String::from("default"))),
            progress: Arc::new(Progress::create()),
            runtime: Arc::new(RwLock::new(Runtime::with_worker_threads(
                settings.get_max_threads()? as usize,
            )?)),
            version: format!(
                "FuseQuery v-{}",
                *crate::configs::config::FUSE_COMMIT_VERSION
            ),
        };

        Ok(Arc::new(ctx))
    }

    pub fn from_settings(
        conf: Config,
        settings: Arc<Settings>,
        default_database: String,
        datasource: Arc<DataSource>,
    ) -> Result<FuseQueryContextRef> {
        Ok(Arc::new(FuseQueryContext {
            conf,
            uuid: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            settings: settings.clone(),
            cluster: Arc::new(RwLock::new(Cluster::empty())),
            datasource,
            statistics: Arc::new(RwLock::new(Statistics::default())),
            partition_queue: Arc::new(RwLock::new(VecDeque::new())),
            current_database: Arc::new(RwLock::new(default_database)),
            progress: Arc::new(Progress::create()),
            runtime: Arc::new(RwLock::new(Runtime::with_worker_threads(
                settings.get_max_threads()? as usize,
            )?)),
            version: format!(
                "FuseQuery v-{}",
                *crate::configs::config::FUSE_COMMIT_VERSION
            ),
        }))
    }

    pub fn with_cluster(&self, cluster: ClusterRef) -> Result<FuseQueryContextRef> {
        *self.cluster.write() = cluster;
        Ok(Arc::new(self.clone()))
    }

    pub fn with_id(&self, uuid: &str) -> Result<FuseQueryContextRef> {
        *self.uuid.write() = uuid.to_string();
        Ok(Arc::new(self.clone()))
    }

    /// ctx.reset will reset the necessary variables in the session
    pub fn reset(&self) -> Result<()> {
        self.progress.reset();
        self.statistics.write().clear();
        self.partition_queue.write().clear();
        Ok(())
    }

    /// Spawns a new asynchronous task, returning a tokio::JoinHandle for it.
    /// The task will run in the current context thread_pool not the global.
    pub fn execute_task<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.runtime.read().spawn(task)
    }

    /// Set progress callback to context.
    /// By default, it is called for leaf sources, after each block
    /// Note that the callback can be called from different threads.
    pub fn progress_callback(&self) -> Result<ProgressCallback> {
        let current_progress = self.progress.clone();
        Ok(Box::new(move |value: &ProgressValues| {
            current_progress.incr(value);
        }))
    }

    pub fn get_progress_value(&self) -> ProgressValues {
        self.progress.as_ref().get_values()
    }

    pub fn get_and_reset_progress_value(&self) -> ProgressValues {
        self.progress.as_ref().get_and_reset()
    }

    // Some table can estimate the approx total rows, such as NumbersTable
    pub fn add_total_rows_approx(&self, total_rows: usize) {
        self.progress.as_ref().add_total_rows_approx(total_rows);
    }

    // Steal n partitions from the partition pool by the pipeline worker.
    // This also can steal the partitions from distributed node.
    pub fn try_get_partitions(&self, num: usize) -> Result<Partitions> {
        let mut partitions = vec![];
        for _ in 0..num {
            match self.partition_queue.write().pop_back() {
                None => break,
                Some(partition) => {
                    partitions.push(partition);
                }
            }
        }
        Ok(partitions)
    }

    // Update the context partition pool from the pipeline builder.
    pub fn try_set_partitions(&self, partitions: Partitions) -> Result<()> {
        for part in partitions {
            self.partition_queue.write().push_back(part);
        }
        Ok(())
    }

    pub fn try_get_statistics(&self) -> Result<Statistics> {
        let statistics = self.statistics.read();
        Ok(Statistics {
            read_rows: statistics.read_rows,
            read_bytes: statistics.read_bytes,
        })
    }

    pub fn try_set_statistics(&self, val: &Statistics) -> Result<()> {
        *self.statistics.write() = val.clone();
        Ok(())
    }

    pub fn try_get_cluster(&self) -> Result<ClusterRef> {
        let cluster = self.cluster.read();
        Ok(cluster.clone())
    }

    pub fn get_datasource(&self) -> Arc<DataSource> {
        self.datasource.clone()
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>> {
        self.datasource.get_table(db_name, table_name)
    }

    // This is an adhoc solution for the metadata syncing problem, far from elegant. let's tweak this later.
    //
    // The reason of not extending IDataSource::get_table (e.g. by adding a remote_hint parameter):
    // Implementation of fetching remote table involves async operations which is not
    // straight forward (but not infeasible) to do in a non-async method.
    pub async fn get_remote_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        self.datasource.get_remote_table(db_name, table_name).await
    }

    pub fn get_table_function(&self, function_name: &str) -> Result<Arc<dyn TableFunction>> {
        self.datasource.get_table_function(function_name)
    }

    pub fn get_id(&self) -> String {
        self.uuid.as_ref().read().clone()
    }

    pub fn get_current_database(&self) -> String {
        self.current_database.as_ref().read().clone()
    }

    pub fn set_current_database(&self, new_database_name: String) -> Result<()> {
        self.datasource
            .get_database(new_database_name.as_str())
            .map(|_| {
                *self.current_database.write() = new_database_name.to_string();
            })
            .map_err(|_| {
                ErrorCode::UnknownDatabase(format!(
                    "Database {}  doesn't exist.",
                    new_database_name
                ))
            })
    }

    pub fn get_max_threads(&self) -> Result<u64> {
        self.settings.get_max_threads()
    }

    pub fn set_max_threads(&self, threads: u64) -> Result<()> {
        *self.runtime.write() = Runtime::with_worker_threads(threads as usize)?;
        self.settings.set_max_threads(threads)
    }

    pub fn get_fuse_version(&self) -> String {
        self.version.clone()
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        self.settings.clone()
    }

    pub fn get_config(&self) -> Config {
        self.conf.clone()
    }
}

impl std::fmt::Debug for FuseQueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.settings)
    }
}
