// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// <https://www.apache.org/licenses/LICENSE-2.0>
//
// SPDX-License-Identifier: Apache-2.0

//std dependencies
use std::fs;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Mutex};

use crate::error_code::ErrorCode;
use crate::kvs_api::{FlushOnExit, InstanceId, KvsApi, SnapshotId};
use crate::kvs_backend::{KvsBackend, KvsPathResolver};
use crate::kvs_value::{KvsMap, KvsValue};

/// Maximum number of snapshots
///
/// Feature: `FEAT_REQ__KVS__snapshots`
const KVS_MAX_SNAPSHOTS: usize = 3;

/// Key-value-storage data
pub struct GenericKvs<Backend: KvsBackend, PathResolver: KvsPathResolver = Backend> {
    /// Instance ID.
    instance_id: InstanceId,

    /// Working directory.
    working_dir: PathBuf,

    /// Storage data
    ///
    /// Feature: `FEAT_REQ__KVS__thread_safety` (Mutex)
    kvs_map: Arc<Mutex<KvsMap>>,

    /// Optional default values
    ///
    /// Feature: `FEAT_REQ__KVS__default_values`
    defaults_map: Mutex<KvsMap>,

    /// Flush on exit flag
    flush_on_exit: AtomicBool,

    /// Marker for `Backend`.
    _backend_marker: PhantomData<Backend>,

    /// Marker for `PathResolver`.
    _path_resolver_marker: PhantomData<PathResolver>,
}

impl From<FlushOnExit> for bool {
    fn from(value: FlushOnExit) -> Self {
        match value {
            FlushOnExit::No => false,
            FlushOnExit::Yes => true,
        }
    }
}

impl<Backend: KvsBackend, PathResolver: KvsPathResolver> GenericKvs<Backend, PathResolver> {
    pub fn new(
        instance_id: InstanceId,
        working_dir: PathBuf,
        kvs_map: Arc<Mutex<KvsMap>>,
        defaults_map: Mutex<KvsMap>,
        flush_on_exit: FlushOnExit,
    ) -> Self {
        Self {
            instance_id,
            working_dir,
            kvs_map,
            defaults_map,
            flush_on_exit: AtomicBool::new(flush_on_exit.into()),
            _backend_marker: PhantomData,
            _path_resolver_marker: PhantomData,
        }
    }

    /// Rotate snapshots
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__snapshots`
    ///
    /// # Return Values
    ///   * Ok: Rotation successful, also if no rotation was needed
    ///   * `ErrorCode::UnmappedError`: Unmapped error
    fn snapshot_rotate(&self) -> Result<(), ErrorCode> {
        for idx in (1..=KVS_MAX_SNAPSHOTS).rev() {
            let old_snapshot_id = SnapshotId(idx - 1);
            let new_snapshot_id = SnapshotId(idx);

            let hash_path_old = PathResolver::hash_file_path(
                &self.working_dir,
                &self.instance_id,
                &old_snapshot_id,
            );
            let hash_path_new = PathResolver::hash_file_path(
                &self.working_dir,
                &self.instance_id,
                &new_snapshot_id,
            );
            let snap_name_old = PathResolver::kvs_file_name(&self.instance_id, &old_snapshot_id);
            let snap_path_old =
                PathResolver::kvs_file_path(&self.working_dir, &self.instance_id, &old_snapshot_id);
            let snap_name_new = PathResolver::kvs_file_name(&self.instance_id, &new_snapshot_id);
            let snap_path_new =
                PathResolver::kvs_file_path(&self.working_dir, &self.instance_id, &new_snapshot_id);

            println!("rotating: {snap_name_old} -> {snap_name_new}");

            let res = fs::rename(hash_path_old, hash_path_new);
            if let Err(err) = res {
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(err.into());
                } else {
                    continue;
                }
            }

            let res = fs::rename(snap_path_old, snap_path_new);
            if let Err(err) = res {
                return Err(err.into());
            }
        }

        Ok(())
    }
}

impl<Backend: KvsBackend, PathResolver: KvsPathResolver> KvsApi
    for GenericKvs<Backend, PathResolver>
{
    /// Control the flush on exit behaviour
    ///
    /// # Parameters
    ///   * `flush_on_exit`: Flag to control flush-on-exit behaviour
    fn flush_on_exit(&self, flush_on_exit: FlushOnExit) {
        self.flush_on_exit
            .store(flush_on_exit.into(), atomic::Ordering::Relaxed);
    }

    /// Resets a key-value-storage to its initial state
    ///
    /// # Return Values
    ///   * Ok: Reset of the KVS was successful
    ///   * `ErrorCode::MutexLockFailed`: Mutex locking failed
    fn reset(&self) -> Result<(), ErrorCode> {
        *self.kvs_map.lock()? = KvsMap::new();
        Ok(())
    }

    /// Reset a key-value pair in the storage to its initial state
    ///
    /// # Parameters
    ///    * 'key': Key being reset to default
    ///
    /// # Return Values
    ///    * Ok: Reset of the key-value pair was successful
    ///    * `ErrorCode::MutexLockFailed`: Mutex locking failed
    ///    * `ErrorCode::KeyDefaultNotFound`: Key has no default value
    fn reset_key(&self, key: &str) -> Result<(), ErrorCode> {
        let mut kvs_map = self.kvs_map.lock()?;
        let defaults_map = self.defaults_map.lock()?;

        if !defaults_map.contains_key(key) {
            eprintln!("error: resetting key without a default value");
            return Err(ErrorCode::KeyDefaultNotFound);
        }

        let _ = kvs_map.remove(key);
        Ok(())
    }

    /// Get list of all keys
    ///
    /// # Return Values
    ///   * Ok: List of all keys
    ///   * `ErrorCode::MutexLockFailed`: Mutex locking failed
    fn get_all_keys(&self) -> Result<Vec<String>, ErrorCode> {
        let kvs_map = self.kvs_map.lock()?;
        Ok(kvs_map.keys().map(|x| x.to_string()).collect())
    }

    /// Check if a key exists
    ///
    /// # Parameters
    ///   * `key`: Key to check for existence
    ///
    /// # Return Values
    ///   * Ok(`true`): Key exists
    ///   * Ok(`false`): Key doesn't exist
    ///   * `ErrorCode::MutexLockFailed`: Mutex locking failed
    fn key_exists(&self, key: &str) -> Result<bool, ErrorCode> {
        let kvs_map = self.kvs_map.lock()?;
        Ok(kvs_map.contains_key(key))
    }

    /// Get the assigned value for a given key
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__default_values`
    ///
    /// # Parameters
    ///   * `key`: Key to retrieve the value from
    ///
    /// # Return Value
    ///   * Ok: Type specific value if key was found
    ///   * `ErrorCode::MutexLockFailed`: Mutex locking failed
    ///   * `ErrorCode::KeyNotFound`: Key wasn't found in KVS nor in defaults
    fn get_value(&self, key: &str) -> Result<KvsValue, ErrorCode> {
        let kvs_map = self.kvs_map.lock()?;
        let defaults_map = self.defaults_map.lock()?;

        if let Some(value) = kvs_map.get(key) {
            Ok(value.clone())
        } else if let Some(value) = defaults_map.get(key) {
            Ok(value.clone())
        } else {
            eprintln!("error: get_value could not find key: {key}");
            Err(ErrorCode::KeyNotFound)
        }
    }

    /// Get the assigned value for a given key
    ///
    /// See [Variants](https://docs.rs/tinyjson/latest/tinyjson/enum.JsonValue.html#variants) for
    /// supported value types.
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__default_values`
    ///
    /// # Parameters
    ///   * `key`: Key to retrieve the value from
    ///
    /// # Return Value
    ///   * Ok: Type specific value if key was found
    ///   * `ErrorCode::MutexLockFailed`: Mutex locking failed
    ///   * `ErrorCode::ConversionFailed`: Type conversion failed
    ///   * `ErrorCode::KeyNotFound`: Key wasn't found in KVS nor in defaults
    fn get_value_as<T>(&self, key: &str) -> Result<T, ErrorCode>
    where
        for<'a> T: TryFrom<&'a KvsValue> + std::clone::Clone,
        for<'a> <T as TryFrom<&'a KvsValue>>::Error: std::fmt::Debug,
    {
        let kvs_map = self.kvs_map.lock()?;
        let defaults_map = self.defaults_map.lock()?;

        if let Some(value) = kvs_map.get(key) {
            match T::try_from(value) {
                Ok(value) => Ok(value),
                Err(err) => {
                    eprintln!(
                        "error: get_value could not convert KvsValue from KVS store: {err:#?}"
                    );
                    Err(ErrorCode::ConversionFailed)
                }
            }
        } else if let Some(value) = defaults_map.get(key) {
            // check if key has a default value
            match T::try_from(value) {
                Ok(value) => Ok(value),
                Err(err) => {
                    eprintln!(
                        "error: get_value could not convert KvsValue from default store: {err:#?}"
                    );
                    Err(ErrorCode::ConversionFailed)
                }
            }
        } else {
            eprintln!("error: get_value could not find key: {key}");

            Err(ErrorCode::KeyNotFound)
        }
    }

    /// Get default value for a given key
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__default_values`
    ///   * `FEAT_REQ__KVS__default_value_retrieval`
    ///
    /// # Parameters
    ///   * `key`: Key to get the default for
    ///
    /// # Return Values
    ///   * Ok: `KvsValue` for the key
    ///   * `ErrorCode::KeyNotFound`: Key not found in defaults
    fn get_default_value(&self, key: &str) -> Result<KvsValue, ErrorCode> {
        let defaults_map = self.defaults_map.lock()?;

        if let Some(value) = defaults_map.get(key) {
            Ok(value.clone())
        } else {
            Err(ErrorCode::KeyNotFound)
        }
    }

    /// Return if the value wasn't set yet and uses its default value
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__default_values`
    ///
    /// # Parameters
    ///   * `key`: Key to check if a default exists
    ///
    /// # Return Values
    ///   * Ok(true): Key currently returns the default value
    ///   * Ok(false): Key returns the set value
    ///   * `ErrorCode::MutexLockFailed`: Mutex locking failed
    ///   * `ErrorCode::KeyNotFound`: Key wasn't found
    fn is_value_default(&self, key: &str) -> Result<bool, ErrorCode> {
        let kvs_map = self.kvs_map.lock()?;
        let defaults_map = self.defaults_map.lock()?;

        if kvs_map.contains_key(key) {
            Ok(false)
        } else if defaults_map.contains_key(key) {
            Ok(true)
        } else {
            Err(ErrorCode::KeyNotFound)
        }
    }

    /// Assign a value to a given key
    ///
    /// # Parameters
    ///   * `key`: Key to set value
    ///   * `value`: Value to be set
    ///
    /// # Return Values
    ///   * Ok: Value was assigned to key
    ///   * `ErrorCode::MutexLockFailed`: Mutex locking failed
    fn set_value<S: Into<String>, V: Into<KvsValue>>(
        &self,
        key: S,
        value: V,
    ) -> Result<(), ErrorCode> {
        let mut kvs_map = self.kvs_map.lock()?;
        kvs_map.insert(key.into(), value.into());
        Ok(())
    }

    /// Remove a key
    ///
    /// # Parameters
    ///   * `key`: Key to remove
    ///
    /// # Return Values
    ///   * Ok: Key removed successfully
    ///   * `ErrorCode::MutexLockFailed`: Mutex locking failed
    ///   * `ErrorCode::KeyNotFound`: Key not found
    fn remove_key(&self, key: &str) -> Result<(), ErrorCode> {
        let mut kvs_map = self.kvs_map.lock()?;
        if kvs_map.remove(key).is_some() {
            Ok(())
        } else {
            Err(ErrorCode::KeyNotFound)
        }
    }

    /// Flush the in-memory key-value-storage to the persistent storage
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__snapshots`
    ///   * `FEAT_REQ__KVS__persistency`
    ///   * `FEAT_REQ__KVS__integrity_check`
    ///
    /// # Return Values
    ///   * Ok: Flush successful
    ///   * `ErrorCode::MutexLockFailed`: Mutex locking failed
    ///   * `ErrorCode::JsonGeneratorError`: Failed to serialize to JSON
    ///   * `ErrorCode::ConversionFailed`: JSON could not serialize into String
    ///   * `ErrorCode::UnmappedError`: Unmapped error
    fn flush(&self) -> Result<(), ErrorCode> {
        self.snapshot_rotate().map_err(|e| {
            eprintln!("error: snapshot_rotate failed: {e:?}");
            e
        })?;
        let kvs_map = self.kvs_map.lock().map_err(|e| {
            eprintln!("error: Mutex lock failed: {e:?}");
            ErrorCode::MutexLockFailed
        })?;

        let snapshot_id = SnapshotId(0);
        let kvs_path =
            PathResolver::kvs_file_path(&self.working_dir, &self.instance_id, &snapshot_id);
        let hash_path =
            PathResolver::hash_file_path(&self.working_dir, &self.instance_id, &snapshot_id);
        Backend::save_kvs(&kvs_map, &kvs_path, Some(&hash_path)).map_err(|e| {
            eprintln!("error: save_kvs failed: {e:?}");
            e
        })?;
        Ok(())
    }

    /// Get the count of snapshots
    ///
    /// # Return Values
    ///   * usize: Count of found snapshots
    fn snapshot_count(&self) -> usize {
        let mut count = 0;

        for idx in 0..KVS_MAX_SNAPSHOTS {
            let snapshot_id = SnapshotId(idx);
            let snapshot_path =
                PathResolver::kvs_file_path(&self.working_dir, &self.instance_id, &snapshot_id);
            if !snapshot_path.exists() {
                break;
            }

            count += 1;
        }

        count
    }

    /// Return maximum snapshot count
    ///
    /// # Return Values
    ///   * usize: Maximum count of snapshots
    fn snapshot_max_count() -> usize {
        KVS_MAX_SNAPSHOTS
    }

    /// Recover key-value-storage from snapshot
    ///
    /// Restore a previously created KVS snapshot.
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__snapshots`
    ///
    /// # Parameters
    ///   * `id`: Snapshot ID
    ///
    /// # Return Values
    ///   * `Ok`: Snapshot restored
    ///   * `ErrorCode::InvalidSnapshotId`: Invalid snapshot ID
    ///   * `ErrorCode::ValidationFailed`: KVS hash validation failed
    ///   * `ErrorCode::JsonParserError`: JSON parser error
    ///   * `ErrorCode::KvsFileReadError`: KVS file not found
    ///   * `ErrorCode::KvsHashFileReadError`: KVS hash file read error
    ///   * `ErrorCode::UnmappedError`: Generic error
    fn snapshot_restore(&self, snapshot_id: &SnapshotId) -> Result<(), ErrorCode> {
        // fail if the snapshot ID is the current KVS
        if *snapshot_id == SnapshotId(0) {
            eprintln!("error: tried to restore current KVS as snapshot");
            return Err(ErrorCode::InvalidSnapshotId);
        }

        if self.snapshot_count() < snapshot_id.0 {
            eprintln!("error: tried to restore a non-existing snapshot");
            return Err(ErrorCode::InvalidSnapshotId);
        }

        let kvs_path =
            PathResolver::kvs_file_path(&self.working_dir, &self.instance_id, snapshot_id);
        let hash_path =
            PathResolver::hash_file_path(&self.working_dir, &self.instance_id, snapshot_id);
        let kvs_map = Backend::load_kvs(&kvs_path, Some(&hash_path))?;
        *self.kvs_map.lock()? = kvs_map;

        Ok(())
    }

    /// Return the KVS-filename for a given snapshot ID
    ///
    /// # Parameters
    ///   * `id`: Snapshot ID to get the filename for
    ///
    /// # Return Values
    ///   * `Ok`: Filename for ID
    ///   * `ErrorCode::FileNotFound`: KVS file for snapshot ID not found
    fn get_kvs_file_path(&self, snapshot_id: &SnapshotId) -> Result<PathBuf, ErrorCode> {
        let path = PathResolver::kvs_file_path(&self.working_dir, &self.instance_id, snapshot_id);
        if !path.exists() {
            Err(ErrorCode::FileNotFound)
        } else {
            Ok(path)
        }
    }

    /// Return the hash-filename for a given snapshot ID
    ///
    /// # Parameters
    ///   * `id`: Snapshot ID to get the hash filename for
    ///
    /// # Return Values
    ///   * `Ok`: Hash filename for ID
    ///   * `ErrorCode::FileNotFound`: Hash file for snapshot ID not found
    fn get_hash_file_path(&self, snapshot_id: &SnapshotId) -> Result<PathBuf, ErrorCode> {
        let path = PathResolver::hash_file_path(&self.working_dir, &self.instance_id, snapshot_id);
        if !path.exists() {
            Err(ErrorCode::FileNotFound)
        } else {
            Ok(path)
        }
    }
}

impl<Backend: KvsBackend, PathResolver: KvsPathResolver> Drop
    for GenericKvs<Backend, PathResolver>
{
    fn drop(&mut self) {
        if self.flush_on_exit.load(atomic::Ordering::Relaxed) {
            if let Err(e) = self.flush() {
                eprintln!("GenericKvs::flush() failed in Drop: {e:?}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::json_backend::JsonBackend;
    use crate::prelude::*;
    use tempfile::tempdir;
    use tinyjson::{JsonGenerator, JsonValue};

    mod mock_backend {
        use crate::kvs_backend::KvsBackend;
        use crate::prelude::*;
        use std::path::{Path, PathBuf};

        #[derive(Default, Clone)]
        pub struct KvsMockBackend {}

        impl KvsBackend for KvsMockBackend {
            fn load_kvs(
                kvs_path: &Path,
                _hash_path: Option<&PathBuf>,
            ) -> Result<KvsMap, ErrorCode> {
                let mut map = KvsMap::new();
                let fname = kvs_path.display().to_string();
                if fname.ends_with("default.json") {
                    map.insert("mock_default_key".to_string(), KvsValue::from(111.0));
                } else {
                    map.insert("mock_key".to_string(), KvsValue::from(123.0));
                }
                Ok(map)
            }

            fn save_kvs(
                _kvs_map: &KvsMap,
                _kvs_path: &Path,
                _hash_path: Option<&PathBuf>,
            ) -> Result<(), ErrorCode> {
                Ok(())
            }
        }

        #[derive(Default, Clone)]
        pub struct KvsMockBackendFail;

        impl KvsBackend for KvsMockBackendFail {
            fn load_kvs(
                _kvs_path: &Path,
                _hash_path: Option<&PathBuf>,
            ) -> Result<KvsMap, ErrorCode> {
                Err(ErrorCode::UnmappedError)
            }

            fn save_kvs(
                _kvs_map: &KvsMap,
                _kvs_path: &Path,
                _hash_path: Option<&PathBuf>,
            ) -> Result<(), ErrorCode> {
                Err(ErrorCode::UnmappedError)
            }
        }
    }

    use mock_backend::KvsMockBackend;
    type MockKvsProvider = GenericKvsProvider<KvsMockBackend, JsonBackend>;
    type MockKvs = GenericKvs<KvsMockBackend, JsonBackend>;

    use mock_backend::KvsMockBackendFail;
    type MockFailKvsProvider = GenericKvsProvider<KvsMockBackendFail, JsonBackend>;
    type MockFailKvs = GenericKvs<KvsMockBackendFail, JsonBackend>;

    fn mock_kvs(kvs_provider: &MockKvsProvider) -> Result<MockKvs, ErrorCode> {
        let instance_id = InstanceId(100);
        let params = KvsParameters::new(instance_id);
        kvs_provider.get(params)
    }

    fn mock_kvs_params(
        kvs_provider: &MockKvsProvider,
        defaults: Defaults,
        kvs_load: KvsLoad,
    ) -> Result<MockKvs, ErrorCode> {
        let instance_id = InstanceId(100);
        let params = KvsParameters::new(instance_id)
            .defaults(defaults)
            .kvs_load(kvs_load);
        kvs_provider.get(params)
    }

    fn mock_kvs_fail(kvs_provider: &MockFailKvsProvider) -> Result<MockFailKvs, ErrorCode> {
        let instance_id = InstanceId(100);
        let params = KvsParameters::new(instance_id)
            .defaults(Defaults::Required)
            .kvs_load(KvsLoad::Required);
        kvs_provider.get(params)
    }

    #[test]
    fn test_open_kvs_with_mock_required_success() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs_params(&kvs_provider, Defaults::Required, KvsLoad::Required).unwrap();
        // Should contain the mock_key from mock backend
        assert!(kvs.key_exists("mock_key").unwrap());
        let value = kvs.get_value("mock_key").unwrap();
        assert_eq!(*value.get::<f64>().unwrap(), 123.0);
    }

    #[test]
    fn test_open_kvs_with_mock_required_fail() {
        let kvs_provider = MockFailKvsProvider::new(Some("".to_string()));
        let res = mock_kvs_fail(&kvs_provider);
        assert!(res.is_err());
        // Should be ErrorCode::UnmappedError
        assert!(matches!(res, Err(ErrorCode::UnmappedError)));
    }

    #[test]
    fn test_open_kvs_with_mock_backend() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs_params(&kvs_provider, Defaults::Required, KvsLoad::Required).unwrap();
        // Should contain the mock_key from mock backend
        assert!(kvs.key_exists("mock_key").unwrap());
        let value = kvs.get_value("mock_key").unwrap();
        assert_eq!(*value.get::<f64>().unwrap(), 123.0);
    }

    #[test]
    fn test_set_and_get_value() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs(&kvs_provider).unwrap();
        kvs.set_value("foo", 42.0).unwrap();
        let value = kvs.get_value("foo").unwrap();
        assert_eq!(*value.get::<f64>().unwrap(), 42.0);
    }

    #[test]
    fn test_get_value_as() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs(&kvs_provider).unwrap();
        kvs.set_value("bar", 99.0).unwrap();
        let v: f64 = kvs.get_value_as("bar").unwrap();
        assert_eq!(v, 99.0);
    }

    #[test]
    fn test_get_default_value_and_is_value_default() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs_params(&kvs_provider, Defaults::Required, KvsLoad::Required).unwrap();
        println!("{:?}", kvs.defaults_map);
        // mock_key is always present as default in mock backend
        assert!(kvs.is_value_default("mock_default_key").unwrap());
        let def = kvs.get_default_value("mock_default_key").unwrap();
        assert_eq!(*def.get::<f64>().unwrap(), 111.0);

        // mock_key is always present as default in mock backend
        assert!(!kvs.is_value_default("mock_key").unwrap());
    }

    #[test]
    fn test_key_exists_and_get_all_keys() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs_params(&kvs_provider, Defaults::Required, KvsLoad::Required).unwrap();
        assert!(kvs.key_exists("mock_key").unwrap());
        kvs.set_value("foo", true).unwrap();
        assert!(kvs.key_exists("foo").unwrap());
        let keys = kvs.get_all_keys().unwrap();
        assert!(keys.contains(&"foo".to_string()));
        assert!(keys.contains(&"mock_key".to_string()));
    }

    #[test]
    fn test_remove_key() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs(&kvs_provider).unwrap();
        kvs.set_value("baz", 1.0).unwrap();
        assert!(kvs.key_exists("baz").unwrap());
        kvs.remove_key("baz").unwrap();
        assert!(!kvs.key_exists("baz").unwrap());
    }

    #[test]
    fn test_reset() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs(&kvs_provider).unwrap();
        kvs.set_value("reset_me", 5.0).unwrap();
        assert!(kvs.key_exists("reset_me").unwrap());
        kvs.reset().unwrap();
        assert!(!kvs.key_exists("reset_me").unwrap());
    }

    #[test]
    fn test_flush_with_mock_backend() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs(&kvs_provider).unwrap();
        // Should not error, even though nothing is written
        kvs.flush().unwrap();
    }

    #[test]
    fn test_snapshot_count_and_max_count() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs(&kvs_provider).unwrap();
        // No real snapshots, so count should be 0
        assert_eq!(kvs.snapshot_count(), 0);
        assert_eq!(
            GenericKvs::<KvsMockBackend, JsonBackend>::snapshot_max_count(),
            3
        );
    }

    #[test]
    fn test_snapshot_restore_invalid_id() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs(&kvs_provider).unwrap();
        // id 1 is always invalid in this mock (no real snapshots)
        let res = kvs.snapshot_restore(&SnapshotId(1));
        assert!(res.is_err());
    }

    #[test]
    fn test_get_kvs_file_path() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_string_lossy().to_string();
        let kvs_provider = KvsProvider::new(Some(dir_path));

        let instance_id = InstanceId(100);
        let kvs = kvs_provider.get(KvsParameters::new(instance_id)).unwrap();

        kvs.flush().unwrap();
        kvs.flush().unwrap();
        let kvs_path = kvs.get_kvs_file_path(&SnapshotId(1)).unwrap();
        let kvs_name = kvs_path.file_name().unwrap().to_str().unwrap();
        assert_eq!(kvs_name, "kvs_100_1.json");
    }

    #[test]
    fn test_get_hash_file_path() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_string_lossy().to_string();
        let kvs_provider = KvsProvider::new(Some(dir_path));

        let instance_id = InstanceId(100);
        let kvs = kvs_provider.get(KvsParameters::new(instance_id)).unwrap();

        kvs.flush().unwrap();
        kvs.flush().unwrap();
        let hash_path = kvs.get_hash_file_path(&SnapshotId(1)).unwrap();
        let hash_name = hash_path.file_name().unwrap().to_str().unwrap();
        assert_eq!(hash_name, "kvs_100_1.hash");
    }

    #[test]
    fn test_get_value_error_cases() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs(&kvs_provider).unwrap();
        // Key does not exist
        let res = kvs.get_value("not_found");
        assert!(matches!(res, Err(ErrorCode::KeyNotFound)));
    }

    #[test]
    fn test_get_value_as_conversion_error() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs(&kvs_provider).unwrap();
        kvs.set_value("str_key", "string".to_string()).unwrap();
        // Try to get as f64, should fail
        let res: Result<f64, _> = kvs.get_value_as("str_key");
        assert!(matches!(res, Err(ErrorCode::ConversionFailed)));
    }

    #[test]
    fn test_is_value_default_error() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs(&kvs_provider).unwrap();
        let res = kvs.is_value_default("not_found");
        assert!(matches!(res, Err(ErrorCode::KeyNotFound)));
    }

    #[test]
    fn test_kvs_open_and_set_get_value() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_string_lossy().to_string();
        let kvs_provider = KvsProvider::new(Some(dir_path));

        let instance_id = InstanceId(42);
        let kvs = kvs_provider.get(KvsParameters::new(instance_id)).unwrap();
        let _ = kvs.set_value("direct", KvsValue::String("abc".to_string()));
        let value = kvs.get_value("direct").unwrap();
        assert_eq!(value.get::<String>().unwrap(), "abc");
    }

    #[test]
    fn test_kvs_reset() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_string_lossy().to_string();
        let kvs_provider = KvsProvider::new(Some(dir_path));

        let instance_id = InstanceId(43);
        let kvs = kvs_provider.get(KvsParameters::new(instance_id)).unwrap();
        let _ = kvs.set_value("reset", 1.0);
        assert!(kvs.get_value("reset").is_ok());
        kvs.reset().unwrap();
        assert!(matches!(
            kvs.get_value("reset"),
            Err(ErrorCode::KeyNotFound)
        ));
    }

    #[test]
    fn test_kvs_key_exists_and_get_all_keys() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_string_lossy().to_string();
        let kvs_provider = KvsProvider::new(Some(dir_path));

        let instance_id = InstanceId(44);
        let kvs = kvs_provider.get(KvsParameters::new(instance_id)).unwrap();
        assert!(!kvs.key_exists("foo").unwrap());
        let _ = kvs.set_value("foo", KvsValue::Boolean(true));
        assert!(kvs.key_exists("foo").unwrap());
        let keys = kvs.get_all_keys().unwrap();
        assert!(keys.contains(&"foo".to_string()));
    }

    #[test]
    fn test_kvs_remove_key() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_string_lossy().to_string();
        let kvs_provider = KvsProvider::new(Some(dir_path));

        let instance_id = InstanceId(45);
        let kvs = kvs_provider.get(KvsParameters::new(instance_id)).unwrap();
        let _ = kvs.set_value("bar", 2.0);
        assert!(kvs.key_exists("bar").unwrap());
        kvs.remove_key("bar").unwrap();
        assert!(!kvs.key_exists("bar").unwrap());
    }

    #[test]
    fn test_kvs_flush_and_snapshot() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_string_lossy().to_string();
        let kvs_provider = KvsProvider::new(Some(dir_path));

        let instance_id = InstanceId(46);
        let kvs = kvs_provider.get(KvsParameters::new(instance_id)).unwrap();
        let _ = kvs.set_value("snap", 3.0);
        // Before flush, snapshot count should be 0 (no snapshots yet)
        assert_eq!(kvs.snapshot_count(), 0);
        kvs.flush().unwrap();
        // After flush, snapshot count should be 1
        assert_eq!(kvs.snapshot_count(), 1);
        // Call flush again to rotate and create a snapshot
        kvs.flush().unwrap();
        assert_eq!(kvs.snapshot_count(), 2);
        // Restore from snapshot if available
        if kvs.snapshot_count() > 0 {
            kvs.snapshot_restore(&SnapshotId(1)).unwrap();
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_kvs_reset_single() {
        let kvs_provider = MockKvsProvider::new(Some("".to_string()));
        let kvs = mock_kvs_params(&kvs_provider, Defaults::Required, KvsLoad::Optional).unwrap();

        kvs.set_value("mock_default_key", 999.0).unwrap();
        assert_eq!(kvs.get_value_as::<f64>("mock_default_key").unwrap(), 999.0);

        kvs.reset_key("mock_default_key").unwrap();
        assert_eq!(kvs.get_value_as::<f64>("mock_default_key").unwrap(), 111.0);

        kvs.set_value("no_default", KvsValue::Boolean(true))
            .unwrap();
        assert!(matches!(
            kvs.reset_key("no_default"),
            Err(ErrorCode::KeyDefaultNotFound)
        ));

        assert!(matches!(
            kvs.reset_key("fail"),
            Err(ErrorCode::KeyDefaultNotFound)
        ));
    }

    #[test]
    fn test_drop() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_string_lossy().to_string();
        let kvs_provider = KvsProvider::new(Some(dir_path));

        let instance_id = InstanceId(0);
        let kvs = kvs_provider
            .get(KvsParameters::new(instance_id).flush_on_exit(FlushOnExit::No))
            .unwrap();

        // Drop is called automatically, but we can check that flush_on_exit is set to false
        assert!(
            !kvs.flush_on_exit.load(std::sync::atomic::Ordering::Relaxed),
            "Expected flush_on_exit to be false"
        );
    }

    /// Create a KVS, close it, modify checksum and try to reopen it.
    #[test]
    fn test_checksum_wrong() {
        let dir = tempdir().unwrap();
        let dir_string = dir.path().to_string_lossy().to_string();

        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_string.clone()),
        )
        .unwrap();

        kvs.set_value("number", 123.0).unwrap();
        kvs.set_value("bool", true).unwrap();
        kvs.set_value("string", "Hello".to_string()).unwrap();
        kvs.set_value("null", ()).unwrap();
        kvs.set_value(
            "array",
            vec![
                KvsValue::from(456.0),
                false.into(),
                "Bye".to_string().into(),
            ],
        )
        .unwrap();

        kvs.flush().unwrap();

        // remember hash filename
        let hash_filename = kvs.get_hash_filename(SnapshotId::new(0)).unwrap();

        // modify the checksum
        std::fs::write(hash_filename, vec![0x12, 0x34, 0x56, 0x78]).unwrap();

        // opening must fail because of the missing checksum file
        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Required,
            Some(dir_string.clone()),
        );

        assert_eq!(kvs.err(), Some(ErrorCode::ValidationFailed));
    }

    /// Create a KVS, close it, delete checksum and try to reopen it.
    #[test]
    fn test_checksum_missing() {
        let dir = tempdir().unwrap();
        let dir_string = dir.path().to_string_lossy().to_string();

        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_string.clone()),
        )
        .unwrap();

        kvs.set_value("number", 123.0).unwrap();
        kvs.set_value("bool", true).unwrap();
        kvs.set_value("string", "Hello".to_string()).unwrap();
        kvs.set_value("null", ()).unwrap();
        kvs.set_value(
            "array",
            vec![
                KvsValue::from(456.0),
                false.into(),
                "Bye".to_string().into(),
            ],
        )
        .unwrap();

        kvs.flush().unwrap();

        // remember hash filename
        let hash_filename = kvs.get_hash_filename(SnapshotId::new(0)).unwrap();

        // delete the checksum
        std::fs::remove_file(hash_filename).unwrap();

        // opening must fail because of the missing checksum file
        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Required,
            Some(dir_string.clone()),
        );

        assert_eq!(kvs.err(), Some(ErrorCode::KvsHashFileReadError));
    }

    /// Test default values
    ///   * Default file must exist
    ///   * Default value must be returned when key isn't set
    ///   * Key must report that default is used
    ///   * Key must be returned when it was written and report it
    ///   * Change in default must be returned when key isn't set
    ///   * Change in default must be ignored when key was once set
    #[test]
    fn kvs_with_defaults() {
        let dir = tempdir().unwrap();
        let dir_string = dir.path().to_string_lossy().to_string();

        // create defaults file
        let defaults: HashMap<String, JsonValue> = HashMap::from([
            (
                "number1".to_string(),
                JsonValue::Object(HashMap::from([
                    ("t".to_string(), JsonValue::String("f64".to_string())),
                    ("v".to_string(), JsonValue::Number(123.0)),
                ])),
            ),
            (
                "bool1".to_string(),
                JsonValue::Object(HashMap::from([
                    ("t".to_string(), JsonValue::String("bool".to_string())),
                    ("v".to_string(), JsonValue::Boolean(true)),
                ])),
            ),
            (
                "string1".to_string(),
                JsonValue::Object(HashMap::from([
                    ("t".to_string(), JsonValue::String("str".to_string())),
                    ("v".to_string(), JsonValue::String("Hello".to_string())),
                ])),
            ),
        ]);

        let json_value = JsonValue::Object(defaults);
        let mut buf = Vec::new();
        let mut gen = JsonGenerator::new(&mut buf).indent("  ");
        gen.generate(&json_value).unwrap();

        let data = String::from_utf8(buf).unwrap();
        let filepath = &dir.path().join(format!("kvs_{}_default.json", 0));
        std::fs::write(filepath, &data).unwrap();

        // create KVS
        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Required,
            OpenNeedKvs::Optional,
            Some(dir_string.clone()),
        )
        .unwrap();

        kvs.set_value("number2", 345.0).unwrap();
        kvs.set_value("bool2", false).unwrap();
        kvs.set_value("string2", "Ola".to_string()).unwrap();

        assert_eq!(kvs.get_value_as::<f64>("number1").unwrap(), 123.0);
        assert_eq!(kvs.get_value_as::<f64>("number2").unwrap(), 345.0);

        assert!(kvs.get_value_as::<bool>("bool1").unwrap());
        assert!(!kvs.get_value_as::<bool>("bool2").unwrap());

        assert_eq!(
            kvs.get_value_as::<String>("string1").unwrap(),
            "Hello".to_string()
        );
        assert_eq!(
            kvs.get_value_as::<String>("string2").unwrap(),
            "Ola".to_string()
        );

        assert!(kvs.is_value_default("number1").unwrap());
        assert!(!kvs.is_value_default("number2").unwrap());

        assert!(kvs.is_value_default("bool1").unwrap());
        assert!(!kvs.is_value_default("bool2").unwrap());

        assert!(kvs.is_value_default("string1").unwrap());
        assert!(!kvs.is_value_default("string2").unwrap());

        // write same-as-default-value into `bool1`
        kvs.set_value("bool1", true).unwrap();

        // write not-same-as-default into `string1`
        kvs.set_value("string1", "Bonjour".to_string()).unwrap();

        // drop the current instance with flush-on-exit enabled and reopen storage
        drop(kvs);

        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Required,
            Some(dir_string.clone()),
        )
        .unwrap();

        assert!(kvs.get_value_as::<bool>("bool1").unwrap());
        assert!(!kvs.is_value_default("bool1").unwrap());

        assert_eq!(
            kvs.get_value_as::<String>("string1").unwrap(),
            "Bonjour".to_string()
        );
        assert!(!kvs.is_value_default("string1").unwrap());

        // drop the current instance with flush-on-exit enabled and reopen storage
        drop(kvs);

        // create defaults file in t-tagged format
        let defaults: HashMap<String, JsonValue> = HashMap::from([
            (
                "number1".to_string(),
                JsonValue::Object(HashMap::from([
                    ("t".to_string(), JsonValue::String("f64".to_string())),
                    ("v".to_string(), JsonValue::Number(987.0)),
                ])),
            ),
            (
                "bool1".to_string(),
                JsonValue::Object(HashMap::from([
                    ("t".to_string(), JsonValue::String("bool".to_string())),
                    ("v".to_string(), JsonValue::Boolean(false)),
                ])),
            ),
            (
                "string1".to_string(),
                JsonValue::Object(HashMap::from([
                    ("t".to_string(), JsonValue::String("str".to_string())),
                    ("v".to_string(), JsonValue::String("Hello".to_string())),
                ])),
            ),
        ]);

        let json_value = JsonValue::from(defaults);
        let mut buf = Vec::new();
        let mut gen = JsonGenerator::new(&mut buf).indent("  ");
        gen.generate(&json_value).unwrap();

        let data = String::from_utf8(buf).unwrap();
        std::fs::write(filepath, &data).unwrap();

        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Required,
            Some(dir_string.clone()),
        )
        .unwrap();

        assert_eq!(kvs.get_value_as::<f64>("number1").unwrap(), 987.0);
        assert!(kvs.is_value_default("number1").unwrap());

        assert!(kvs.get_value_as::<bool>("bool1").unwrap());
        assert!(!kvs.is_value_default("bool1").unwrap());
    }

    /// Create a key-value-storage without defaults
    #[test]
    fn kvs_without_defaults() {
        let dir = tempdir().unwrap();
        let dir_string = dir.path().to_string_lossy().to_string();

        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_string.clone()),
        )
        .unwrap();

        kvs.set_value("number", 123.0).unwrap();
        kvs.set_value("bool", true).unwrap();
        kvs.set_value("string", "Hello".to_string()).unwrap();
        kvs.set_value("null", ()).unwrap();
        kvs.set_value(
            "array",
            vec![
                KvsValue::from(456.0),
                false.into(),
                "Bye".to_string().into(),
            ],
        )
        .unwrap();
        kvs.set_value(
            "object",
            HashMap::from([
                (String::from("sub-number"), KvsValue::from(789.0)),
                ("sub-bool".into(), true.into()),
                ("sub-string".into(), "Hi".to_string().into()),
                ("sub-null".into(), ().into()),
                (
                    "sub-array".into(),
                    KvsValue::from(vec![
                        KvsValue::from(1246.0),
                        false.into(),
                        "Moin".to_string().into(),
                    ]),
                ),
            ]),
        )
        .unwrap();

        // drop the current instance with flush-on-exit enabled and reopen storage
        drop(kvs);

        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Required,
            Some(dir_string.clone()),
        )
        .unwrap();
        assert_eq!(kvs.get_value_as::<f64>("number").unwrap(), 123.0);
        assert!(kvs.get_value_as::<bool>("bool").unwrap());
        assert_eq!(kvs.get_value_as::<String>("string").unwrap(), "Hello");
        assert_eq!(kvs.get_value_as::<()>("null"), Ok(()));

        let json_array = kvs.get_value_as::<Vec<KvsValue>>("array").unwrap();
        assert_eq!(json_array[0].get(), Some(&456.0));
        assert_eq!(json_array[1].get(), Some(&false));
        assert_eq!(json_array[2].get(), Some(&"Bye".to_string()));

        let json_map = kvs
            .get_value_as::<HashMap<String, KvsValue>>("object")
            .unwrap();
        assert_eq!(json_map["sub-number"].get(), Some(&789.0));
        assert_eq!(json_map["sub-bool"].get(), Some(&true));
        assert_eq!(json_map["sub-string"].get(), Some(&"Hi".to_string()));
        assert_eq!(json_map["sub-null"].get(), Some(&()));

        let json_sub_array = &json_map["sub-array"];
        assert!(
            matches!(json_sub_array, KvsValue::Array(_)),
            "Expected sub-array to be an Array"
        );
        if let KvsValue::Array(arr) = json_sub_array {
            assert_eq!(arr[0].get(), Some(&1246.0));
            assert_eq!(arr[1].get(), Some(&false));
            assert_eq!(arr[2].get(), Some(&"Moin".to_string()));
        }

        // test for non-existent values
        assert_eq!(
            kvs.get_value_as::<String>("non-existent").err(),
            Some(ErrorCode::KeyNotFound)
        );
    }

    /// Create an example KVS
    fn create_kvs(dir_string: String) -> Result<Kvs, ErrorCode> {
        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_string.clone()),
        )?;

        kvs.set_value("number", 123.0)?;
        kvs.set_value("bool", true)?;
        kvs.set_value("string", "Hello".to_string())?;
        kvs.set_value("null", ())?;
        kvs.set_value(
            "array",
            vec![
                KvsValue::from(456.0),
                false.into(),
                "Bye".to_string().into(),
            ],
        )?;
        kvs.set_value(
            "object",
            HashMap::from([
                (String::from("sub-number"), KvsValue::from(789.0)),
                ("sub-bool".into(), true.into()),
                ("sub-string".into(), "Hi".to_string().into()),
                ("sub-null".into(), ().into()),
                (
                    "sub-array".into(),
                    KvsValue::from(vec![
                        KvsValue::from(1246.0),
                        false.into(),
                        "Moin".to_string().into(),
                    ]),
                ),
            ]),
        )?;

        Ok(kvs)
    }

    /// Test snapshot rotation
    #[test]
    fn kvs_snapshot_rotation() -> Result<(), ErrorCode> {
        let dir = tempdir()?;
        let dir_string = dir.path().to_string_lossy().to_string();

        let max_count = Kvs::snapshot_max_count();
        let mut kvs = create_kvs(dir_string.clone())?;

        // max count is added twice to make sure we rotate once
        let mut cnts: Vec<usize> = (0..=max_count).collect();
        let mut cnts_post: Vec<usize> = vec![max_count];
        cnts.append(&mut cnts_post);

        // make sure the snapshot count is 0, 0, .., max_count, max_count (rotation)
        for cnt in cnts {
            assert_eq!(kvs.snapshot_count(), cnt);

            // drop the current instance with flush-on-exit enabled and re-open it
            drop(kvs);
            kvs = Kvs::open(
                InstanceId::new(0),
                OpenNeedDefaults::Optional,
                OpenNeedKvs::Required,
                Some(dir_string.clone()),
            )
            .unwrap();
        }

        // restore the oldest snapshot
        assert!(kvs.snapshot_restore(SnapshotId::new(max_count)).is_ok());

        // try to restore a snapshot behind the last one
        assert_eq!(
            kvs.snapshot_restore(SnapshotId::new(max_count + 1)).err(),
            Some(ErrorCode::InvalidSnapshotId)
        );

        Ok(())
    }

    /// Test snapshot recovery
    #[test]
    fn kvs_snapshot_restore() -> Result<(), ErrorCode> {
        let dir = tempdir()?;
        let dir_string = dir.path().to_string_lossy().to_string();

        let max_count = Kvs::snapshot_max_count();
        let mut kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_string.clone()),
        )?;

        // we need a double zero here because after the first flush no snapshot is created
        // and the max count is also added twice to make sure we rotate once
        let mut cnts: Vec<usize> = vec![0];
        let mut cnts_mid: Vec<usize> = (0..=max_count).collect();
        let mut cnts_post: Vec<usize> = vec![max_count];
        cnts.append(&mut cnts_mid);
        cnts.append(&mut cnts_post);

        let mut counter = 0;
        for (idx, _) in cnts.into_iter().enumerate() {
            counter = idx;
            kvs.set_value("counter", counter as f64)?;

            // drop the current instance with flush-on-exit enabled and re-open it
            drop(kvs);
            kvs = Kvs::open(
                InstanceId::new(0),
                OpenNeedDefaults::Optional,
                OpenNeedKvs::Required,
                Some(dir_string.clone()),
            )?;
        }

        // restore snapshots and check `counter` value
        for idx in 1..=max_count {
            kvs.snapshot_restore(SnapshotId::new(idx))?;
            assert_eq!(kvs.get_value_as::<f64>("counter")?, (counter - idx) as f64);
        }

        Ok(())
    }
}
