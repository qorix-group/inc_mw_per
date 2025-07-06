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

extern crate alloc;

//core and alloc libs
use alloc::string::FromUtf8Error;
use core::fmt;
use core::array::TryFromSliceError;


//std dependencies
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{
    atomic::{self, AtomicBool},
    Mutex, MutexGuard, PoisonError,
};



//external crates
use adler32::RollingAdler32;

use crate::kvs_value::KvsValue;
use crate::kvs_api::KvsApi;
use crate::error_code::ErrorCode;

//json dependencies
use crate::json_value::{KvsJson, TinyJson, KvsJsonError};
use crate::json_value::TryFromKvsValue;


/// Maximum number of snapshots
///
/// Feature: `FEAT_REQ__KVS__snapshots`
const KVS_MAX_SNAPSHOTS: usize = 3;

/// Instance ID
#[derive(Clone, Debug, PartialEq)]
pub struct InstanceId(usize);

/// Snapshot ID
pub struct SnapshotId(usize);


impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl InstanceId {
    /// Create a new instance ID
    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

impl SnapshotId {
    /// Create a new Snapshot ID
    pub fn new(id: usize) -> Self {
        SnapshotId(id)
    }
}


/// Verify-Hash flag
#[derive(PartialEq)]
enum OpenJsonVerifyHash {
    /// No: Parse the file without the hash
    No,

    /// Yes: Parse the file with the hash
    Yes,
}

/// Key-value-storage data
pub struct Kvs<J: KvsJson + Default = TinyJson> {
    /// Storage data
    ///
    /// Feature: `FEAT_REQ__KVS__thread_safety` (Mutex)
    kvs: Mutex<HashMap<String, KvsValue>>,

    /// Optional default values
    ///
    /// Feature: `FEAT_REQ__KVS__default_values`
    default: HashMap<String, KvsValue>,

    /// Filename prefix
    filename_prefix: String,

    /// Flush on exit flag
    flush_on_exit: AtomicBool,
    /// Marker for generic JSON backend
    _json_backend: std::marker::PhantomData<J>,
}


/// Need-Defaults flag
pub enum OpenNeedDefaults {
    /// Optional: Open defaults only if available
    Optional,

    /// Required: Defaults must be available
    Required,
}

/// Need-KVS flag
pub enum OpenNeedKvs {
    /// Optional: Use an empty KVS if no KVS is available
    Optional,

    /// Required: KVS must be already exist
    Required,
}

/// Need-File flag
#[derive(PartialEq)]
enum OpenJsonNeedFile {
    /// Optional: If the file doesn't exist, start with empty data
    Optional,

    /// Required: The file must already exist
    Required,
}

impl From<bool> for OpenNeedDefaults {
    fn from(flag: bool) -> OpenNeedDefaults {
        if flag {
            OpenNeedDefaults::Required
        } else {
            OpenNeedDefaults::Optional
        }
    }
}

impl From<bool> for OpenNeedKvs {
    fn from(flag: bool) -> OpenNeedKvs {
        if flag {
            OpenNeedKvs::Required
        } else {
            OpenNeedKvs::Optional
        }
    }
}

impl From<OpenNeedDefaults> for OpenJsonNeedFile {
    fn from(val: OpenNeedDefaults) -> OpenJsonNeedFile {
        match val {
            OpenNeedDefaults::Optional => OpenJsonNeedFile::Optional,
            OpenNeedDefaults::Required => OpenJsonNeedFile::Required,
        }
    }
}

impl From<OpenNeedKvs> for OpenJsonNeedFile {
    fn from(val: OpenNeedKvs) -> OpenJsonNeedFile {
        match val {
            OpenNeedKvs::Optional => OpenJsonNeedFile::Optional,
            OpenNeedKvs::Required => OpenJsonNeedFile::Required,
        }
    }
}



impl From<std::io::Error> for ErrorCode {
    fn from(cause: std::io::Error) -> Self {
        let kind = cause.kind();
        match kind {
            std::io::ErrorKind::NotFound => ErrorCode::FileNotFound,
            _ => {
                eprintln!("error: unmapped error: {kind}");
                ErrorCode::UnmappedError
            }
        }
    }
}

impl From<KvsJsonError> for ErrorCode {
    fn from(cause: KvsJsonError) -> Self {
        eprintln!("error: JSON operation error: {cause:#?}");
        ErrorCode::JsonParserError
    }
}

impl From<FromUtf8Error> for ErrorCode {
    fn from(cause: FromUtf8Error) -> Self {
        eprintln!("error: UTF-8 conversion failed: {cause:#?}");
        ErrorCode::ConversionFailed
    }
}

impl From<TryFromSliceError> for ErrorCode {
    fn from(cause: TryFromSliceError) -> Self {
        eprintln!("error: try_into from slice failed: {cause:#?}");
        ErrorCode::ConversionFailed
    }
}

impl From<Vec<u8>> for ErrorCode {
    fn from(cause: Vec<u8>) -> Self {
        eprintln!("error: try_into from u8 vector failed: {:#?}", cause);
        ErrorCode::ConversionFailed
    }
}

impl From<PoisonError<MutexGuard<'_, HashMap<std::string::String, KvsValue>>>> for ErrorCode {
    fn from(cause: PoisonError<MutexGuard<'_, HashMap<std::string::String, KvsValue>>>) -> Self {
        eprintln!("error: Mutex locking failed: {cause:#?}");
        ErrorCode::MutexLockFailed
    }
}


impl<J: KvsJson + Default> Kvs<J> {
    /// Open and parse a JSON file
    ///
    /// Return an empty hash when no file was found.
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__integrity_check`
    ///
    /// # Parameters
    ///   * `need_file`: fail if file doesn't exist
    ///   * `verify_hash`: content is verified against a hash file
    ///
    /// # Return Values
    ///   * `Ok`: KVS data as `HashMap<String, KvsValue>`
    ///   * `ErrorCode::ValidationFailed`: KVS hash validation failed
    ///   * `ErrorCode::JsonParserError`: JSON parser error
    ///   * `ErrorCode::KvsFileReadError`: KVS file read error
    ///   * `ErrorCode::KvsHashFileReadError`: KVS hash file read error
    ///   * `ErrorCode::UnmappedError`: Generic error
    fn open_json<T>(
        filename_prefix: &str,
        need_file: T,
        verify_hash: OpenJsonVerifyHash,
    ) -> Result<HashMap<String, KvsValue>, ErrorCode>
    where
        T: Into<OpenJsonNeedFile>,
    {
        let filename_json = format!("{filename_prefix}.json");
        let filename_hash = format!("{filename_prefix}.hash");
        match fs::read_to_string(&filename_json) {
            Ok(data) => {
                if verify_hash == OpenJsonVerifyHash::Yes {
                    // data exists, read hash file
                    match fs::read(&filename_hash) {
                        Ok(hash) => {
                            let hash_kvs = RollingAdler32::from_buffer(data.as_bytes()).hash();
                            if u32::from_be_bytes(hash.try_into()?) != hash_kvs {
                                eprintln!(
                                    "error: KVS data corrupted ({filename_json}, {filename_hash})"
                                );
                                Err(ErrorCode::ValidationFailed)
                            } else {
                                println!("JSON data has valid hash");
                                let json_val = J::parse(&data)?;
                                let kvs_val = J::to_kvs_value(json_val);
                                if let KvsValue::Object(map) = kvs_val {
                                    println!("parsing file {filename_json}");
                                    Ok(map)
                                } else {
                                    Err(ErrorCode::JsonParserError)
                                }
                            }
                        }
                        Err(err) => {
                            eprintln!(
                                "error: hash file {filename_hash} could not be read: {err:#?}"
                            );
                            Err(ErrorCode::KvsHashFileReadError)
                        }
                    }
                } else {
                    let json_val = J::parse(&data)?;
                    let kvs_val = J::to_kvs_value(json_val);
                    if let KvsValue::Object(map) = kvs_val {
                        Ok(map)
                    } else {
                        Err(ErrorCode::JsonParserError)
                    }
                }
            }
            Err(err) => {
                if need_file.into() == OpenJsonNeedFile::Required {
                    eprintln!("error: file {filename_json} could not be read: {err:#?}");
                    Err(ErrorCode::KvsFileReadError)
                } else {
                    println!("file {filename_json} not found, using empty data");
                    Ok(HashMap::new())
                }
            }
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
            let hash_old = format!("{}_{}.hash", self.filename_prefix, idx - 1);
            let hash_new = format!("{}_{}.hash", self.filename_prefix, idx);
            let snap_old = format!("{}_{}.json", self.filename_prefix, idx - 1);
            let snap_new = format!("{}_{}.json", self.filename_prefix, idx);

            println!("rotating: {snap_old} -> {snap_new}");

            let res = fs::rename(hash_old, hash_new);
            if let Err(err) = res {
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(err.into());
                } else {
                    continue;
                }
            }

            let res = fs::rename(snap_old, snap_new);
            if let Err(err) = res {
                return Err(err.into());
            }
        }

        Ok(())
    }
}

impl<J: KvsJson + Default> KvsApi for Kvs<J> {
    /// Open the key-value-storage
    ///
    /// Checks and opens a key-value-storage. Flush on exit is enabled by default and can be
    /// controlled with [`flush_on_exit`](Self::flush_on_exit).
    ///
    /// # Features
    ///   * `FEAT_REQ__KVS__default_values`
    ///   * `FEAT_REQ__KVS__multiple_kvs`
    ///   * `FEAT_REQ__KVS__integrity_check`
    ///
    /// # Parameters
    ///   * `instance_id`: Instance ID
    ///   * `need_defaults`: Fail when no default file was found
    ///   * `need_kvs`: Fail when no KVS file was found
    ///
    /// # Return Values
    ///   * Ok: KVS instance
    ///   * `ErrorCode::ValidationFailed`: KVS hash validation failed
    ///   * `ErrorCode::JsonParserError`: JSON parser error
    ///   * `ErrorCode::KvsFileReadError`: KVS file read error
    ///   * `ErrorCode::KvsHashFileReadError`: KVS hash file read error
    ///   * `ErrorCode::UnmappedError`: Generic error
    fn open(
        instance_id: InstanceId,
        need_defaults: OpenNeedDefaults,
        need_kvs: OpenNeedKvs,
        dir: Option<String>,
    ) -> Result<Kvs<J>, ErrorCode> {
        let dir = if let Some(dir) = dir {
            format!("{dir}/")
        } else {
            "".to_string()
        };
        let filename_default = format!("{dir}kvs_{instance_id}_default");
        let filename_prefix = format!("{dir}kvs_{instance_id}");
        let filename_kvs = format!("{filename_prefix}_0");

        let default = Kvs::<J>::open_json(&filename_default, need_defaults, OpenJsonVerifyHash::No)?;
        let kvs = Kvs::<J>::open_json(&filename_kvs, need_kvs, OpenJsonVerifyHash::Yes)?;

        println!("opened KVS: instance '{instance_id}'");
        println!("max snapshot count: {KVS_MAX_SNAPSHOTS}");

        Ok(Kvs {
            kvs: Mutex::new(kvs),
            default,
            filename_prefix,
            flush_on_exit: AtomicBool::new(true),
            _json_backend: std::marker::PhantomData,
        })
    }

    /// Control the flush on exit behaviour
    ///
    /// # Parameters
    ///   * `flush_on_exit`: Flag to control flush-on-exit behaviour
    fn flush_on_exit(&self, flush_on_exit: bool) {
        self.flush_on_exit
            .store(flush_on_exit, atomic::Ordering::Relaxed);
    }

    /// Resets a key-value-storage to its initial state
    ///
    /// # Return Values
    ///   * Ok: Reset of the KVS was successful
    ///   * `ErrorCode::MutexLockFailed`: Mutex locking failed
    fn reset(&self) -> Result<(), ErrorCode> {
        *self.kvs.lock()? = HashMap::new();
        Ok(())
    }

    /// Get list of all keys
    ///
    /// # Return Values
    ///   * Ok: List of all keys
    ///   * `ErrorCode::MutexLockFailed`: Mutex locking failed
    fn get_all_keys(&self) -> Result<Vec<String>, ErrorCode> {
        Ok(self.kvs.lock()?.keys().map(|x| x.to_string()).collect())
    }

    /// Get the assigned value for a given key using TryFromKvsValue
    fn get_value_kvs<T>(&self, key: &str) -> Result<T, ErrorCode>
    where
        T: TryFromKvsValue + Clone {
        let kvs = self.kvs.lock()?;
        if let Some(value) = kvs.get(key) {
            T::try_from_kvs_value(value)
        } else if let Some(value) = self.default.get(key) {
            T::try_from_kvs_value(value)
        } else {
            Err(ErrorCode::KeyNotFound)
        }
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
        Ok(self.kvs.lock()?.contains_key(key))
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
    fn get_value<T>(&self, key: &str) -> Result<T, ErrorCode>
    where
        for<'a> T: TryFrom<&'a KvsValue> + std::clone::Clone,
        for<'a> <T as TryFrom<&'a KvsValue>>::Error: std::fmt::Debug,
    {
        let kvs = self.kvs.lock()?;

        if let Some(value) = kvs.get(key) {
            match T::try_from(value) {
                Ok(value) => Ok(value),
                Err(err) => {
                    eprintln!(
                        "error: get_value could not convert KvsValue from KVS store: {err:#?}"
                    );
                    Err(ErrorCode::ConversionFailed)
                }
            }
        } else if let Some(value) = self.default.get(key) {
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
        if let Some(value) = self.default.get(key) {
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
        if self.kvs.lock()?.contains_key(key) {
            Ok(false)
        } else if self.default.contains_key(key) {
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
        self.kvs.lock()?.insert(key.into(), value.into());
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
        if self.kvs.lock()?.remove(key).is_some() {
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
        let map: HashMap<String, KvsValue> = self
            .kvs
            .lock()? 
            .clone();
        let kvs_val = KvsValue::Object(map);
        let backend_json_val = J::from_kvs_value(&kvs_val);
        let json_str = J::stringify(&backend_json_val)?;
        let buf = json_str.as_bytes();

        self.snapshot_rotate()?;

        let hash = RollingAdler32::from_buffer(buf).hash();

        let filename_json = format!("{}_0.json", self.filename_prefix);
        fs::write(&filename_json, buf)?;

        let filename_hash = format!("{}_0.hash", self.filename_prefix);
        fs::write(filename_hash, hash.to_be_bytes()).ok();

        Ok(())
    }

    /// Get the count of snapshots
    ///
    /// # Return Values
    ///   * usize: Count of found snapshots
    fn snapshot_count(&self) -> usize {
        let mut count = 0;

        for idx in 0..=KVS_MAX_SNAPSHOTS {
            if !Path::new(&format!("{}_{}.json", self.filename_prefix, idx)).exists() {
                break;
            }

            // skip current KVS but make sure it exists before search for snapshots
            if idx == 0 {
                continue;
            }

            count = idx;
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
    fn snapshot_restore(&self, id: SnapshotId) -> Result<(), ErrorCode> {
        // fail if the snapshot ID is the current KVS
        if id.0 == 0 {
            eprintln!("error: tried to restore current KVS as snapshot");
            return Err(ErrorCode::InvalidSnapshotId);
        }

        if self.snapshot_count() < id.0 {
            eprintln!("error: tried to restore a non-existing snapshot");
            return Err(ErrorCode::InvalidSnapshotId);
        }

        let kvs = Self::open_json(
            &format!("{}_{}", self.filename_prefix, id.0),
            OpenJsonNeedFile::Required,
            OpenJsonVerifyHash::Yes,
        )?;
        *self.kvs.lock()? = kvs;

        Ok(())
    }

    /// Return the KVS-filename for a given snapshot ID
    ///
    /// # Parameters
    ///   * `id`: Snapshot ID to get the filename for
    ///
    /// # Return Values
    ///   * String: Filename for ID
    fn get_kvs_filename(&self, id: SnapshotId) -> String {
        format!("{}_{}.json", self.filename_prefix, id)
    }

    /// Return the hash-filename for a given snapshot ID
    ///
    /// # Parameters
    ///   * `id`: Snapshot ID to get the hash filename for
    ///
    /// # Return Values
    ///   * String: Hash filename for ID
    fn get_hash_filename(&self, id: SnapshotId) -> String {
        format!("{}_{}.hash", self.filename_prefix, id)
    }
}

impl<J: KvsJson + Default> Drop for Kvs<J> {
    fn drop(&mut self) {
        if self.flush_on_exit.load(atomic::Ordering::Relaxed) {
            self.flush().ok();
        }
    }
}

// Implement Default for TinyJson so it can be used as the default backend
impl Default for TinyJson {
    fn default() -> Self {
        TinyJson {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::marker::PhantomData;
    use crate::kvs_value::KvsValue;
    use crate::json_value::KvsJson;
    use crate::json_value::KvsJsonError;
    use core::convert::TryFrom;

    // Mock JSON backend
    struct MockJson;
    impl KvsJson for MockJson {
        type Value = String;
        fn parse(s: &str) -> Result<Self::Value, KvsJsonError> { Ok(s.to_string()) }
        fn stringify(val: &Self::Value) -> Result<String, KvsJsonError> { Ok(val.clone()) }
        fn get_object(_val: &Self::Value) -> Option<&HashMap<String, Self::Value>> { None }
        fn get_array(_val: &Self::Value) -> Option<&Vec<Self::Value>> { None }
        fn get_f64(_val: &Self::Value) -> Option<f64> { None }
        fn get_bool(_val: &Self::Value) -> Option<bool> { None }
        fn get_string(val: &Self::Value) -> Option<&str> { Some(val.as_str()) }
        fn is_null(_val: &Self::Value) -> bool { false }
        fn to_kvs_value(_val: Self::Value) -> KvsValue { KvsValue::Null }
        fn from_kvs_value(_val: &KvsValue) -> Self::Value { "mocked".to_string() }
    }

    impl Default for MockJson {
        fn default() -> Self { MockJson }
    }

    // Implement TryFrom<&KvsValue> for i32 for test context
    impl<'a> TryFrom<&'a KvsValue> for i32 {
        type Error = crate::error_code::ErrorCode;
        fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
            match value {
                KvsValue::I32(i) => Ok(*i),
                KvsValue::U32(u) => Ok(*u as i32),
                KvsValue::F64(f) => Ok(*f as i32),
                KvsValue::String(s) => s.parse::<i32>().map_err(|_| crate::error_code::ErrorCode::ConversionFailed),
                _ => Err(crate::error_code::ErrorCode::ConversionFailed),
            }
        }
    }

    type TestKvs = Kvs<MockJson>;

    fn new_test_kvs() -> TestKvs {
        TestKvs {
            kvs: Mutex::new(HashMap::new()),
            default: HashMap::new(),
            filename_prefix: "test".to_string(),
            flush_on_exit: AtomicBool::new(false),
            _json_backend: PhantomData,
        }
    }

    #[test]
    fn test_set_and_get_value() {
        let kvs = new_test_kvs();
        kvs.set_value("foo", 123).unwrap();
        let val: i32 = kvs.get_value("foo").unwrap();
        assert_eq!(val, 123);
    }

    #[test]
    fn test_key_exists() {
        let kvs = new_test_kvs();
        kvs.set_value("bar", 1).unwrap();
        assert!(kvs.key_exists("bar").unwrap());
        assert!(!kvs.key_exists("baz").unwrap());
    }

    #[test]
    fn test_remove_key() {
        let kvs = new_test_kvs();
        kvs.set_value("x", 1).unwrap();
        assert!(kvs.remove_key("x").is_ok());
        assert!(kvs.remove_key("x").is_err());
    }

    #[test]
    fn test_get_all_keys() {
        let kvs = new_test_kvs();
        kvs.set_value("a", 1).unwrap();
        kvs.set_value("b", 2).unwrap();
        let mut keys = kvs.get_all_keys().unwrap();
        keys.sort();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn test_reset() {
        let kvs = new_test_kvs();
        kvs.set_value("foo", 1).unwrap();
        kvs.reset().unwrap();
        assert!(kvs.get_all_keys().unwrap().is_empty());
    }
}