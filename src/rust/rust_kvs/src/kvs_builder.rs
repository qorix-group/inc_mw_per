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
    require_defaults: bool,

    /// Need-KVS flag
    require_kvs: bool,

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
            require_defaults: false,
            require_kvs: false,
            dir: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Enable the requirement that defaults must exist when opening the KVS.
    ///
    /// # Return Values
    ///   * KvsBuilder instance with `require_defaults` set to `true`.
    pub fn require_defaults(mut self) -> Self {
        self.require_defaults = true;
        self
    }

    /// Enable the requirement that the KVS must already exist when opening.
    ///
    /// # Return Values
    ///   * KvsBuilder instance with `require_kvs` set to `true`.
    pub fn require_existing_kvs(mut self) -> Self {
        self.require_kvs = true;
        self
    }

	/// Set the key-value-storage permanent storage directory.
	///
	/// # Parameters
	///   * `dir`: Path to permanent storage
	///
	/// # Return Values
	///   * KvsBuilder instance with the directory set.
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
            self.require_defaults.into(),
            self.require_kvs.into(),
            self.dir,
        )
    }
}


// Re-import TryFromKvsValue and KvsValueGet traits if they were moved
pub use crate::kvs_value::{TryFromKvsValue, KvsValueGet};

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    // --- MockKvs for KvsApi trait testing ---
    struct MockKvs;
    impl KvsApi for MockKvs {
        fn open(
            _instance_id: InstanceId,
            _need_defaults: crate::kvs::OpenNeedDefaults,
            _need_kvs: crate::kvs::OpenNeedKvs,
            _dir: Option<String>,
        ) -> Result<Self, ErrorCode> where Self: Sized { Ok(MockKvs) }
        fn reset(&self) -> Result<(), ErrorCode> { Ok(()) }
        fn get_all_keys(&self) -> Result<Vec<String>, ErrorCode> { Ok(vec![]) }
        fn key_exists(&self, _key: &str) -> Result<bool, ErrorCode> { Ok(false) }
        fn get_value<T>(&self, _key: &str) -> Result<T, ErrorCode>
        where
            for<'a> T: TryFrom<&'a crate::kvs_value::KvsValue> + Clone,
            for<'a> <T as TryFrom<&'a crate::kvs_value::KvsValue>>::Error: std::fmt::Debug,
        { Err(ErrorCode::KeyNotFound) }
        fn get_default_value(&self, _key: &str) -> Result<crate::kvs_value::KvsValue, ErrorCode> { Err(ErrorCode::KeyNotFound) }
        fn is_value_default(&self, _key: &str) -> Result<bool, ErrorCode> { Ok(false) }
        fn set_value<S: Into<String>, J: Into<crate::kvs_value::KvsValue>>(&self, _key: S, _value: J) -> Result<(), ErrorCode> { Ok(()) }
        fn remove_key(&self, _key: &str) -> Result<(), ErrorCode> { Ok(()) }
        fn flush_on_exit(&self, _flush_on_exit: bool) {}
        fn flush(&self) -> Result<(), ErrorCode> { Ok(()) }
        fn snapshot_count(&self) -> usize { 0 }
        fn snapshot_max_count() -> usize where Self: Sized { 0 }
        fn snapshot_restore(&self, _id: crate::kvs::SnapshotId) -> Result<(), ErrorCode> { Ok(()) }
        fn get_kvs_filename(&self, _id: crate::kvs::SnapshotId) -> String { String::new() }
        fn get_hash_filename(&self, _id: crate::kvs::SnapshotId) -> String { String::new() }
        fn get_value_kvs<T>(&self, _key: &str) -> Result<T, ErrorCode>
        where
            T: crate::kvs_value::TryFromKvsValue + Clone,
        { Err(ErrorCode::KeyNotFound) }
    }

    #[must_use]
    fn test_dir() -> (TempDir, String) {
        let temp_dir = TempDir::new("").unwrap();
        let temp_path = temp_dir.path().display().to_string();
        (temp_dir, temp_path)
    }

    #[test]
    fn test_new_kvs_builder() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<MockKvs>::new(instance_id.clone()).dir(test_dir().1);
        let result = builder.build();
        assert!(result.is_ok(), "KvsBuilder<MockKvs> should build successfully");
        let builder = KvsBuilder::<MockKvs>::new(instance_id.clone()).require_defaults();
        assert!(builder.require_defaults);
        let builder = KvsBuilder::<MockKvs>::new(instance_id.clone()).require_existing_kvs();
        assert!(builder.require_kvs);
    }

    #[test]
    // Test that require_defaults is false by default
    fn test_no_require_defaults() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<MockKvs>::new(instance_id.clone())
            .dir(test_dir().1);
        assert!(!builder.require_defaults);
    }

    #[test]
    // Test that require_kvs is false by default
    fn test_no_require_existing_kvs() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<MockKvs>::new(instance_id.clone())
            .dir(test_dir().1);
        assert!(!builder.require_kvs);
    }

    #[test]
    // Test building with default settings and directory
    fn test_build() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<MockKvs>::new(instance_id.clone()).dir(test_dir().1);
        builder.build().unwrap();
    }

    #[test]
    // Test that require_defaults sets the flag and build fails for real Kvs
    fn test_build_with_defaults() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<Kvs>::new(instance_id.clone())
            .dir(test_dir().1)
            .require_defaults();

        assert!(builder.require_defaults);
        assert!(builder.build().is_err());
    }

    #[test]
    // Test that require_existing_kvs sets the flag and build fails for real Kvs
    fn test_build_with_require_existing_kvs() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<Kvs>::new(instance_id.clone())
            .dir(test_dir().1)
            .require_existing_kvs();

        assert!(builder.require_kvs);
        assert!(builder.build().is_err());
    }

    #[test]
    // Test building Kvs with and without require_existing_kvs flag
    fn test_build_with_kvs() {
        let instance_id = InstanceId::new(0);
        let temp_dir = test_dir();

        // negative
        let builder = KvsBuilder::<Kvs>::new(instance_id.clone())
            .dir(temp_dir.1.clone())
            .require_existing_kvs();
        assert!(builder.build().is_err());

        KvsBuilder::<Kvs>::new(instance_id.clone())
            .dir(temp_dir.1.clone())
            .build()
            .unwrap();

    }

    #[test]
    // Test setting a specific directory for Kvs
    fn test_dir_positive() {
        let instance_id = InstanceId::new(0);
        let dir_path = "/tmp/test_dir_positive";
        let builder = KvsBuilder::<MockKvs>::new(instance_id.clone()).dir(dir_path);
        assert_eq!(builder.dir.as_deref(), Some(dir_path));
    }

    #[test]
    // Test that the directory is not set by default
    fn test_dir_negative() {
        let instance_id = InstanceId::new(0);
        let builder = KvsBuilder::<MockKvs>::new(instance_id.clone());
        assert_eq!(builder.dir, None);
    }

    #[test]
    // Test chaining all builder methods
    fn test_builder_chaining_all_methods() {
        let instance_id = InstanceId::new(123);
        let dir_path = "/tmp/test_chain";
        let builder = KvsBuilder::<MockKvs>::new(instance_id.clone())
            .require_defaults()
            .require_existing_kvs()
            .dir(dir_path);
        assert!(builder.require_defaults);
        assert!(builder.require_kvs);
        assert_eq!(builder.dir.as_deref(), Some(dir_path));
    }


}