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

use crate::kvs::{Kvs,InstanceId};
use crate::error_code::ErrorCode;
use crate::kvs_api::KvsApi;

/// Key-value-storage builder
pub struct KvsBuilder<T: KvsApi = Kvs> {
    /// Instance ID
    instance_id: InstanceId,

    /// Need-defaults flag
    need_defaults: bool,

    /// Need-KVS flag
    need_kvs: bool,

    /// Working directory
    dir: Option<String>,

    /// Phantom data for drop check
    _phantom: std::marker::PhantomData<T>,
}


impl<T> KvsBuilder<T>
where
    T: KvsApi,
{
    /// Create a builder to open the key-value-storage
    ///
    /// Only the instance ID must be set. All other settings are using default values until changed
    /// via the builder API.
    ///
    /// # Parameters
    ///   * `instance_id`: Instance ID
    ///
    /// # Return Values
    ///   * KvsBuilder instance
    pub fn new(instance_id: InstanceId) -> Self {
        Self {
            instance_id,
            need_defaults: false,
            need_kvs: false,
            dir: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Configure if defaults must exist when opening the KVS
    ///
    /// # Parameters
    ///   * `flag`: Yes = `true`, no = `false` (default)
    ///
    /// # Return Values
    ///   * KvsBuilder instance
    pub fn need_defaults(mut self, flag: bool) -> Self {
        self.need_defaults = flag;
        self
    }

    /// Configure if KVS must exist when opening the KVS
    ///
    /// # Parameters
    ///   * `flag`: Yes = `true`, no = `false` (default)
    ///
    /// # Return Values
    ///   * KvsBuilder instance
    pub fn need_kvs(mut self, flag: bool) -> Self {
        self.need_kvs = flag;
        self
    }

    /// Set the key-value-storage permanent storage directory
    ///
    /// # Parameters
    ///   * `dir`: Path to permanent storage
    ///
    /// # Return Values
    pub fn dir<P: Into<String>>(mut self, dir: P) -> Self {
        self.dir = Some(dir.into());
        self
    }

    /// Finalize the builder and open the key-value-storage
    ///
    /// Calls `Kvs::open` with the configured settings.
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__default_values`
    ///   * `FEAT_REQ__KVS__multiple_kvs`
    ///   * `FEAT_REQ__KVS__integrity_check`
    ///
    /// # Return Values
    ///   * Ok: KVS instance
    ///   * `ErrorCode::ValidationFailed`: KVS hash validation failed
    ///   * `ErrorCode::JsonParserError`: JSON parser error
    ///   * `ErrorCode::KvsFileReadError`: KVS file read error
    ///   * `ErrorCode::KvsHashFileReadError`: KVS hash file read error
    ///   * `ErrorCode::UnmappedError`: Generic error
    pub fn build(self) -> Result<T, ErrorCode> {
        T::open(
            self.instance_id,
            self.need_defaults.into(),
            self.need_kvs.into(),
            self.dir,
        )
    }
}


// Re-import TryFromKvsValue and KvsValueGet traits if they were moved
// Use only the re-exports from json_value.rs for JSON and conversion traits
pub use crate::json_value::{TryFromKvsValue, KvsValueGet};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use tempdir::TempDir;

    #[must_use]
    fn test_dir() -> (TempDir, String) {
        let temp_dir = TempDir::new("").unwrap();
        let temp_path = temp_dir.path().display().to_string();
        (temp_dir, temp_path)
    }

    #[test]
    fn test_new_kvs_builder() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<Kvs>::new(instance_id.clone()).dir(test_dir().1);

        assert_eq!(builder.instance_id, instance_id);
        assert!(!builder.need_defaults);
        assert!(!builder.need_kvs);
    }

    #[test]
    fn test_need_defaults() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<Kvs>::new(instance_id.clone())
            .dir(test_dir().1)
            .need_defaults(true);

        assert!(builder.need_defaults);
    }

    #[test]
    fn test_need_kvs() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<Kvs>::new(instance_id.clone())
            .dir(test_dir().1)
            .need_kvs(true);

        assert!(builder.need_kvs);
    }

    #[test]
    fn test_build() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<Kvs>::new(instance_id.clone()).dir(test_dir().1);

        builder.build().unwrap();
    }

    #[test]
    fn test_build_with_defaults() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<Kvs>::new(instance_id.clone())
            .dir(test_dir().1)
            .need_defaults(true);

        assert!(builder.build().is_err());
    }

    #[test]
    fn test_build_with_kvs() {
        let instance_id = InstanceId::new(0);
        let temp_dir = test_dir();

        // negative
        let builder = KvsBuilder::<Kvs>::new(instance_id.clone())
            .dir(temp_dir.1.clone())
            .need_kvs(true);
        assert!(builder.build().is_err());

        KvsBuilder::<Kvs>::new(instance_id.clone())
            .dir(temp_dir.1.clone())
            .build()
            .unwrap();

        // positive
        let builder = KvsBuilder::<Kvs>::new(instance_id)
            .dir(temp_dir.1)
            .need_kvs(true);
        builder.build().unwrap();
    }

    #[test]
    fn test_unknown_error_code_from_io_error() {
        let error = std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid input provided");
        assert_eq!(ErrorCode::from(error), ErrorCode::UnmappedError);
    }

    // #[test]
    // fn test_unknown_error_code_from_json_parse_error() {
    //     // Simulate a JSON parse error using the KvsJsonError abstraction
    //     let error = KvsJsonError("test parse error".to_string());
    //     assert_eq!(ErrorCode::from(error), ErrorCode::JsonParserError);
    // }

    // #[test]
    // fn test_unknown_error_code_from_json_generate_error() {
    //     // Simulate a JSON generate error using the KvsJsonError abstraction
    //     let error = KvsJsonError("test generate error".to_string());
    //     assert_eq!(ErrorCode::from(error), ErrorCode::JsonParserError);
    // }

    // #[test]
    // fn test_conversion_failed_from_utf8_error() {
    //     // test from: https://doc.rust-lang.org/std/string/struct.FromUtf8Error.html
    //     let bytes = vec![0, 159];
    //     let error = String::from_utf8(bytes).unwrap_err();
    //     assert_eq!(ErrorCode::from(error), ErrorCode::ConversionFailed);
    // }

    // #[test]
    // fn test_conversion_failed_from_slice_error() {
    //     let bytes = [0x12, 0x34, 0x56, 0x78, 0xab];
    //     let bytes_ptr: &[u8] = &bytes;
    //     let error = TryInto::<[u8; 8]>::try_into(bytes_ptr).unwrap_err();
    //     assert_eq!(ErrorCode::from(error), ErrorCode::ConversionFailed);
    // }

    // #[test]
    // fn test_conversion_failed_from_vec_u8() {
    //     let bytes: Vec<u8> = vec![];
    //     assert_eq!(ErrorCode::from(bytes), ErrorCode::ConversionFailed);
    // }

    // #[test]
    // fn test_mutex_lock_failed_from_poison_error() {
    //     let mutex: Arc<Mutex<HashMap<String, KvsValue>>> = Arc::default();

    //     // test from: https://doc.rust-lang.org/std/sync/struct.PoisonError.html
    //     let c_mutex = Arc::clone(&mutex);
    //     let _ = thread::spawn(move || {
    //         let _unused = c_mutex.lock().unwrap();
    //         panic!();
    //     })
    //     .join();

    //     let error = mutex.lock().unwrap_err();
    //     assert_eq!(ErrorCode::from(error), ErrorCode::MutexLockFailed);
    // }

    // #[test]
    // fn test_flush_on_exit() {
    //     let instance_id = InstanceId::new(0);
    //     let temp_dir = test_dir();
    //     let kvs = KvsBuilder::<Kvs>::new(instance_id.clone())
    //         .dir(temp_dir.1.clone())
    //         .build()
    //         .unwrap();
    //     kvs.flush_on_exit(true);
    // }

    // #[test]
    // fn test_reset() {
    //     let instance_id = InstanceId::new(0);
    //     let temp_dir = test_dir();
    //     let kvs = KvsBuilder::<Kvs>::new(instance_id.clone())
    //         .dir(temp_dir.1.clone())
    //         .build()
    //         .unwrap();
    //     let _ = kvs.set_value("test", KvsValue::F64(1.0));
    //     let result = kvs.reset();
    //     assert!(result.is_ok(), "Expected Ok for reset");
    // }

    // #[test]
    // fn test_get_all_keys() {
    //     let instance_id = InstanceId::new(0);
    //     let temp_dir = test_dir();
    //     let kvs = KvsBuilder::<Kvs>::new(instance_id.clone())
    //         .dir(temp_dir.1.clone())
    //         .build()
    //         .unwrap();
    //     let _ = kvs.set_value("test", KvsValue::F64(1.0));
    //     let keys = kvs.get_all_keys();
    //     assert!(keys.is_ok(), "Expected Ok for get_all_keys");
    //     let keys = keys.unwrap();
    //     assert!(
    //         keys.contains(&"test".to_string()),
    //         "Expected 'test' key in get_all_keys"
    //     );
    // }

    // #[test]
    // fn test_key_exists() {
    //     let instance_id = InstanceId::new(0);
    //     let temp_dir = test_dir();
    //     let kvs = KvsBuilder::<Kvs>::new(instance_id.clone())
    //         .dir(temp_dir.1.clone())
    //         .build()
    //         .unwrap();
    //     let exists = kvs.key_exists("test");
    //     assert!(exists.is_ok(), "Expected Ok for key_exists");
    //     assert!(!exists.unwrap(), "Expected 'test' key to not exist");
    //     let _ = kvs.set_value("test", KvsValue::F64(1.0));
    //     let exists = kvs.key_exists("test");
    //     assert!(exists.is_ok(), "Expected Ok for key_exists after set");
    //     assert!(exists.unwrap(), "Expected 'test' key to exist after set");
    // }

    // #[test]
    // fn test_get_filename() {
    //     let instance_id = InstanceId::new(0);
    //     let temp_dir = test_dir();
    //     let kvs = KvsBuilder::<Kvs>::new(instance_id.clone())
    //         .dir(temp_dir.1.clone())
    //         .build()
    //         .unwrap();
    //     let filename = kvs.get_kvs_filename(SnapshotId::new(0));
    //     assert!(
    //         filename.ends_with("_0.json"),
    //         "Expected filename to end with _0.json"
    //     );
    // }

    // #[test]
    // fn test_get_value() {
    //     let instance_id = InstanceId::new(0);
    //     let temp_dir = test_dir();
    //     let kvs = Arc::new(
    //         KvsBuilder::<Kvs>::new(instance_id.clone())
    //             .dir(temp_dir.1.clone())
    //             .build()
    //             .unwrap(),
    //     );
    //     let _ = kvs.set_value("test", KvsValue::F64(123.0));
    //     let value = (*kvs).get_value_kvs::<f64>("test");
    //     assert_eq!(
    //         value.unwrap(),
    //         123.0,
    //         "Expected to retrieve the inserted value"
    //     );
    // }

    // #[test]
    // fn test_get_inner_value() {
    //     let value = KvsValue::F64(42.0);
    //     let inner = f64::get_inner_value(&value);
    //     assert_eq!(inner, Some(&42.0), "Expected to get inner f64 value");
    // }

    // #[test]
    // fn test_drop() {
    //     let instance_id = InstanceId::new(0);
    //     let temp_dir = test_dir();
    //     let kvs = Arc::new(
    //         KvsBuilder::<Kvs>::new(instance_id.clone())
    //             .dir(temp_dir.1.clone())
    //             .build()
    //             .unwrap(),
    //     );
    //     kvs.flush_on_exit(false);
    //     // Drop is called automatically, but we can check that flush_on_exit is set to false
    //     assert!(
    //         !kvs.flush_on_exit.load(std::sync::atomic::Ordering::Relaxed),
    //         "Expected flush_on_exit to be false"
    //     );
    // }

    // impl<'a> TryFrom<&'a KvsValue> for u64 {
    //     type Error = ErrorCode;

    //     fn try_from(_val: &'a KvsValue) -> Result<u64, Self::Error> {
    //         Err(ErrorCode::ConversionFailed)
    //     }
    // }

    // #[cfg_attr(miri, ignore)]
    // #[test]
    // fn test_get_value_try_from_error() {
    //     let instance_id = InstanceId::new(0);
    //     let temp_dir = test_dir();

    //     std::fs::copy(
    //         "tes/kvs_0_default.json",
    //         format!("{}/kvs_0_default.json", temp_dir.1.clone()),
    //     )
    //     .unwrap();

    //     let kvs = Arc::new(
    //         KvsBuilder::<Kvs>::new(instance_id.clone())
    //             .dir(temp_dir.1.clone())
    //             .need_defaults(true)
    //             .build()
    //             .unwrap(),
    //     );

    //     let _ = kvs.set_value("test", KvsValue::F64(123.0f64));

    //     // stored value: should return ConversionFailed
    //     let result = kvs.get_value::<u64>("test");
    //     assert!(
    //         matches!(result, Err(ErrorCode::ConversionFailed)),
    //         "Expected ConversionFailed for stored value"
    //     );

    //     // default value: should return ConversionFailed
    //     let result = kvs.get_value::<u64>("bool1");
    //     assert!(
    //         matches!(result, Err(ErrorCode::ConversionFailed)),
    //         "Expected ConversionFailed for default value"
    //     );
    // }

    // #[test]
    // fn test_kvs_open_and_set_get_value() {
    //     let instance_id = InstanceId::new(42);
    //     let temp_dir = test_dir();
    //     let kvs = Kvs::open(
    //         instance_id.clone(),
    //         OpenNeedDefaults::Optional,
    //         OpenNeedKvs::Optional,
    //         Some(temp_dir.1.clone()),
    //     )
    //     .unwrap();
    //     let _ = kvs.set_value("direct", KvsValue::String("abc".to_string()));
    //     let value = kvs.get_value_kvs::<String>("direct");
    //     assert_eq!(value.unwrap(), "abc");
    // }

    // #[test]
    // fn test_kvs_reset() {
    //     let instance_id = InstanceId::new(43);
    //     let temp_dir = test_dir();
    //     let kvs = Kvs::open(
    //         instance_id.clone(),
    //         OpenNeedDefaults::Optional,
    //         OpenNeedKvs::Optional,
    //         Some(temp_dir.1.clone()),
    //     )
    //     .unwrap();
    //     let _ = kvs.set_value("reset", KvsValue::F64(1.0));
    //     assert!(kvs.get_value_kvs::<f64>("reset").is_ok());
    //     kvs.reset().unwrap();
    //     assert!(matches!(
    //         kvs.get_value_kvs::<f64>("reset"),
    //         Err(ErrorCode::KeyNotFound)
    //     ));
    // }

    // #[test]
    // fn test_kvs_key_exists_and_get_all_keys() {
    //     let instance_id = InstanceId::new(44);
    //     let temp_dir = test_dir();
    //     let kvs = Kvs::open(
    //         instance_id.clone(),
    //         OpenNeedDefaults::Optional,
    //         OpenNeedKvs::Optional,
    //         Some(temp_dir.1.clone()),
    //     )
    //     .unwrap();
    //     assert!(!kvs.key_exists("foo").unwrap());
    //     let _ = kvs.set_value("foo", KvsValue::Boolean(true));
    //     assert!(kvs.key_exists("foo").unwrap());
    //     let keys = kvs.get_all_keys().unwrap();
    //     assert!(keys.contains(&"foo".to_string()));
    // }

    // #[test]
    // fn test_kvs_remove_key() {
    //     let instance_id = InstanceId::new(45);
    //     let temp_dir = test_dir();
    //     let kvs = Kvs::open(
    //         instance_id.clone(),
    //         OpenNeedDefaults::Optional,
    //         OpenNeedKvs::Optional,
    //         Some(temp_dir.1.clone()),
    //     )
    //     .unwrap();
    //     let _ = kvs.set_value("bar", KvsValue::F64(2.0));
    //     assert!(kvs.key_exists("bar").unwrap());
    //     kvs.remove_key("bar").unwrap();
    //     assert!(!kvs.key_exists("bar").unwrap());
    // }

    // #[test]
    // fn test_kvs_flush_and_snapshot() {
    //     let instance_id = InstanceId::new(46);
    //     let temp_dir = test_dir();
    //     let kvs = Kvs::open(
    //         instance_id.clone(),
    //         OpenNeedDefaults::Optional,
    //         OpenNeedKvs::Optional,
    //         Some(temp_dir.1.clone()),
    //     )
    //     .unwrap();
    //     let _ = kvs.set_value("snap", KvsValue::F64(3.0));
    //     kvs.flush().unwrap();
    //     // After flush, snapshot count should be 0 (no old snapshots yet)
    //     assert_eq!(kvs.snapshot_count(), 0);
    //     // Call flush again to rotate and create a snapshot
    //     kvs.flush().unwrap();
    //     assert!(kvs.snapshot_count() >= 1);
    //     // Restore from snapshot if available
    //     if kvs.snapshot_count() > 0 {
    //         kvs.snapshot_restore(SnapshotId::new(1)).unwrap();
    //     }
    // }
}