use crate::error_code::ErrorCode;
use crate::kvs::GenericKvs;
use crate::kvs_api::{Defaults, FlushOnExit, InstanceId, KvsLoad, SnapshotId};
use crate::kvs_backend::{KvsBackend, KvsPathResolver};
use crate::kvs_value::KvsMap;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Key-value-storage parameters builder.
pub struct KvsParameters {
    /// Instance ID.
    instance_id: InstanceId,

    /// Defaults handling mode.
    defaults: Defaults,

    /// KVS load mode.
    kvs_load: KvsLoad,

    /// Flush on exit mode.
    flush_on_exit: FlushOnExit,
}

impl KvsParameters {
    pub fn new(instance_id: InstanceId) -> Self {
        Self {
            instance_id,
            defaults: Defaults::Optional,
            kvs_load: KvsLoad::Optional,
            flush_on_exit: FlushOnExit::Yes,
        }
    }

    pub fn defaults(mut self, flag: Defaults) -> Self {
        self.defaults = flag;
        self
    }

    pub fn kvs_load(mut self, flag: KvsLoad) -> Self {
        self.kvs_load = flag;
        self
    }

    pub fn flush_on_exit(mut self, flag: FlushOnExit) -> Self {
        self.flush_on_exit = flag;
        self
    }
}

pub struct GenericKvsProvider<Backend: KvsBackend, PathResolver: KvsPathResolver = Backend> {
    /// KVS map pool.
    kvs_pool: Mutex<BTreeMap<InstanceId, Arc<Mutex<KvsMap>>>>,

    /// Working directory.
    working_dir: PathBuf,

    /// Marker for `Backend`.
    _backend_marker: PhantomData<Backend>,

    /// Marker for `PathResolver`.
    _path_resolver_marker: PhantomData<PathResolver>,
}

/// KVS provider.
/// Provides KVS objects, which contain data at the moment of `get`.
/// Changes are committed on `flush`.
impl<Backend: KvsBackend, PathResolver: KvsPathResolver> GenericKvsProvider<Backend, PathResolver> {
    pub fn new(working_dir: Option<String>) -> Self {
        let kvs_pool = Mutex::new(BTreeMap::new());

        let working_dir = if let Some(working_dir) = working_dir {
            PathBuf::from(working_dir)
        } else {
            PathBuf::from("")
        };

        Self {
            kvs_pool,
            working_dir,
            _backend_marker: PhantomData,
            _path_resolver_marker: PhantomData,
        }
    }

    pub fn get(
        &self,
        params: KvsParameters,
    ) -> Result<GenericKvs<Backend, PathResolver>, ErrorCode> {
        let instance_id = params.instance_id;

        // Load file containing defaults.
        let defaults_path = PathResolver::defaults_file_path(&self.working_dir, &instance_id);
        let defaults_map = Mutex::new(match params.defaults {
            Defaults::Ignored => KvsMap::new(),
            Defaults::Optional => {
                if defaults_path.exists() {
                    Backend::load_kvs(&defaults_path, None)?
                } else {
                    KvsMap::new()
                }
            }
            Defaults::Required => Backend::load_kvs(&defaults_path, None)?,
        });

        // Initialize entry in pool.
        let mut kvs_pool = self.kvs_pool.lock()?;
        if !kvs_pool.contains_key(&instance_id) {
            let empty_kvs_map = Arc::new(Mutex::new(KvsMap::new()));
            kvs_pool.insert(instance_id.clone(), empty_kvs_map);
        }

        // Load KVS and hash files.
        let snapshot_id = SnapshotId(0);
        let kvs_path = PathResolver::kvs_file_path(&self.working_dir, &instance_id, &snapshot_id);
        let hash_path = PathResolver::hash_file_path(&self.working_dir, &instance_id, &snapshot_id);
        let kvs_map = match params.kvs_load {
            KvsLoad::Ignored => kvs_pool.get(&instance_id).cloned().unwrap(),
            KvsLoad::Optional => {
                let kvs_map = kvs_pool.get(&instance_id).cloned().unwrap();
                if kvs_path.exists() {
                    *kvs_map.lock()? = Backend::load_kvs(&kvs_path, Some(&hash_path))?;
                }
                kvs_map
            }
            KvsLoad::Required => {
                let kvs_map = kvs_pool.get(&instance_id).cloned().unwrap();
                *kvs_map.lock()? = Backend::load_kvs(&kvs_path, Some(&hash_path))?;
                kvs_map
            }
        };

        Ok(GenericKvs::<Backend, PathResolver>::new(
            instance_id,
            self.working_dir.clone(),
            kvs_map,
            defaults_map,
            params.flush_on_exit,
        ))
    }
}

impl From<PoisonError<MutexGuard<'_, BTreeMap<InstanceId, Arc<Mutex<KvsMap>>>>>> for ErrorCode {
    fn from(cause: PoisonError<MutexGuard<'_, BTreeMap<InstanceId, Arc<Mutex<KvsMap>>>>>) -> Self {
        eprintln!("error: Pool mutex locking failed: {cause:#?}");
        ErrorCode::MutexLockFailed
    }
}

#[cfg(test)]
mod kvs_parameters_tests {
    use super::*;

    #[test]
    fn test_parameters_instance_id_default_values() {
        let instance_id = InstanceId(42);
        let params = KvsParameters::new(instance_id);
        assert_eq!(params.instance_id, InstanceId(42));
        assert_eq!(params.defaults, Defaults::Optional);
        assert_eq!(params.kvs_load, KvsLoad::Optional);
        assert_eq!(params.flush_on_exit, FlushOnExit::Yes);
    }

    #[test]
    fn test_parameters_defaults() {
        let instance_id = InstanceId(1);
        let params = KvsParameters::new(instance_id).defaults(Defaults::Required);
        assert_eq!(params.defaults, Defaults::Required);
    }

    #[test]
    fn test_parameters_kvs_load() {
        let instance_id = InstanceId(1);
        let params = KvsParameters::new(instance_id).kvs_load(KvsLoad::Required);
        assert_eq!(params.kvs_load, KvsLoad::Required);
    }

    #[test]
    fn test_parameters_flush_on_exit() {
        let instance_id = InstanceId(1);
        let params = KvsParameters::new(instance_id).flush_on_exit(FlushOnExit::No);
        assert_eq!(params.flush_on_exit, FlushOnExit::No);
    }

    #[test]
    fn test_parameters_chained() {
        let instance_id = InstanceId(1);
        let params = KvsParameters::new(instance_id)
            .defaults(Defaults::Ignored)
            .kvs_load(KvsLoad::Ignored)
            .flush_on_exit(FlushOnExit::No);
        assert_eq!(params.instance_id, InstanceId(1));
        assert_eq!(params.defaults, Defaults::Ignored);
        assert_eq!(params.kvs_load, KvsLoad::Ignored);
        assert_eq!(params.flush_on_exit, FlushOnExit::No);
    }
}
