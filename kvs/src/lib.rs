//! Copyright (c) 2024 Contributors to the Eclipse Foundation
//!
//! See the NOTICE file(s) distributed with this work for additional
//! information regarding copyright ownership.
//!
//! This program and the accompanying materials are made available under the
//! terms of the Apache License Version 2.0 which is available at
//! <https://www.apache.org/licenses/LICENSE-2.0>
//!
//! SPDX-License-Identifier: Apache-2.0
//!
//! # Key-Value-Store API and Implementation
//!
//! ## Introduction
//!
//! This crate provides a Key-Value-Store using [TinyJSON](https://crates.io/crates/tinyjson) to
//! persist the data. To validate the stored data a hash is build and verified using the
//! [Adler32](https://crates.io/crates/adler32) crate. No other direct dependencies are used
//! besides the Rust `std` library.
//!
//! The key-value-store is opened or initialized with [`Kvs::open`] and automatically flushed on
//! exit by default. This can be controlled by [`Kvs::flush_on_exit`]. It is possible to manually
//! flush the KVS by calling [`Kvs::flush`].
//!
//! All `TinyJSON` provided datatypes can be used:
//!   * `Number`: `f64`
//!   * `Boolean`: `bool`
//!   * `String`: `String`
//!   * `Null`: `()`
//!   * `Array`: `Vec<JsonValue>`
//!   * `Object`: `HashMap<String, JsonValue>`
//!
//! Note: JSON arrays are not restricted to only contain values of the same type.
//!
//! Writing a value to the KVS can be done by calling [`Kvs::set_value`] with the `key` as first
//! and a `JsonValue` as second parameter. Either `JsonValue::Number(123.0)` or `123.0` can be
//! used as there will be an auto-Into performed when calling the function.
//!
//! To read a value call [`Kvs::get_value::<T>`](Kvs::get_value) with the `key` as first
//! parameter. `T` represents the type to read and can be `f64`, `bool`, `String`, `()`,
//! `Vec<JsonValue>`, `HashMap<String, JsonValue` or `JsonValue`. Also `let value: f64 =
//! kvs.get_value()` can be used.
//!
//! If a `key` isn't available in the KVS a lookup into the defaults store will be performed and
//! if the `value` is found the default will be returned. The default value isn't stored when
//! [`Kvs::flush`] is called unless it's explicitly written with [`Kvs::set_value`]. So when
//! defaults change always the latest values will be returned. If that is an unwanted behaviour
//! it's better to remove the default value and write the value permanently when the KVS is
//! initialized. To check whether a value has a default call [`Kvs::get_default_value`] and to
//! see if the value wasn't written yet and will return the default call
//! [`Kvs::is_value_default`].
//!
//!
//! ## Example Usage
//!
//! ```
//! use rust_kvs::{ErrorCode, InstanceId, Kvs};
//! use std::collections::HashMap;
//! use tinyjson::JsonValue;
//!
//! fn main() -> Result<(), ErrorCode> {
//!     let kvs = Kvs::open(InstanceId::new(0), false, false)?;
//!
//!     kvs.set_value("number", 123.0)?;
//!     kvs.set_value("bool", true)?;
//!     kvs.set_value("string", "First".to_string())?;
//!     kvs.set_value("null", ())?;
//!     kvs.set_value(
//!         "array",
//!         vec![
//!             JsonValue::from(456.0),
//!             false.into(),
//!             "Second".to_string().into(),
//!         ],
//!     )?;
//!     kvs.set_value(
//!         "object",
//!         HashMap::from([
//!             (String::from("sub-number"), JsonValue::from(789.0)),
//!             ("sub-bool".into(), true.into()),
//!             ("sub-string".into(), "Third".to_string().into()),
//!             ("sub-null".into(), ().into()),
//!             (
//!                 "sub-array".into(),
//!                 JsonValue::from(vec![
//!                     JsonValue::from(1246.0),
//!                     false.into(),
//!                     "Fourth".to_string().into(),
//!                 ]),
//!             ),
//!         ]),
//!     )?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Feature Coverage
//!
//! Feature and requirement definition:
//!   * [Features/Persistency/Key-Value-Store](https://github.com/eclipse-score/score/blob/ulhu_persistency_kvs/docs/features/persistency/key-value-storage/index.rst#specification)
//!   * [Requirements/Stakeholder](https://github.com/eclipse-score/score/blob/ulhu_persistency_kvs/docs/requirements/stakeholder/index.rst)
//!
//! Supported features and requirements:
//!   * `FEAT_REQ__KVS__maximum_size`
//!   * `FEAT_REQ__KVS__thread_safety`
//!   * `FEAT_REQ__KVS__supported_datatypes_keys`
//!   * `FEAT_REQ__KVS__supported_datatypes_values`
//!   * `FEAT_REQ__KVS__default_values`
//!   * `FEAT_REQ__KVS__update_mechanism`: JSON format-flexibility
//!   * `FEAT_REQ__KVS__snapshots`
//!   * `FEAT_REQ__KVS__default_value_reset`
//!   * `FEAT_REQ__KVS__default_value_retrieval`
//!   * `FEAT_REQ__KVS__persistency`
//!   * `FEAT_REQ__KVS__integrity_check`
//!   * `STKH_REQ__30`: JSON storage format
//!   * `STKH_REQ__8`: Defaults stored in JSON format
//!   * `STKH_REQ__12`: Support storing data on non-volatile memory
//!   * `STKH_REQ__13`: POSIX portability
//!
//! Currently unsupported features:
//!   * `FEAT_REQ__KVS__cpp_rust_interoperability`
//!   * `FEAT_REQ__KVS__versioning`: JSON version ID
//!   * `FEAT_REQ__KVS__tooling`: Get/set CLI, JSON editor
//!   * `STKH_REQ__350`: Safe key-value-store
//!
//! Additional info:
//!   * Feature `FEAT_REQ__KVS__supported_datatypes_keys` is matched by the Rust standard which
//!     defines that `String` and `str` are always valid UTF-8.
//!   * Feature `FEAT_REQ__KVS__supported_datatypes_values` is matched by using the same types that
//!     the IPC will use for the Rust implementation.
//!
//! ## Todos
//!
//!   * Store the current working directory in the KVS struct to make sure snapshots are created at
//!     the same place as the KVS was opened in case of the application changes the working
//!     directory
#![allow(unused)]
#![forbid(unsafe_code)]

use adler32::{adler32, RollingAdler32};
use std::array::TryFromSliceError;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::Path;
use std::string::FromUtf8Error;
use std::sync::{
    atomic::{self, AtomicBool},
    Arc, Mutex, MutexGuard, PoisonError,
};
use tinyjson::{JsonGenerateError, JsonGenerator, JsonParseError, JsonValue, UnexpectedValue};

mod kvs_async;
pub use kvs_async::*;

/// Define the maximum count of elements that can be stored in the KVS
///
/// Feature: `FEAT_REQ__KVS__maximum_size`
const KVS_MAX_SIZE: usize = 1000;

/// Maximum number of snapshots
///
/// Feature: `FEAT_REQ__KVS__snapshots`
const KVS_MAX_SNAPSHOTS: usize = 3;

/// Instance ID
pub struct InstanceId(usize);

/// Snapshot ID
pub struct SnapshotId(usize);

/// Runtime Error Codes
#[derive(Debug, PartialEq)]
pub enum ErrorCode {
    /// Error that was not yet mapped
    UnmappedError,

    /// File not found
    FileNotFound,

    /// JSON parser error
    JsonParserError,

    /// JSON generator error
    JsonGeneratorError,

    /// Physical storage failure
    PhysicalStorageFailure,

    /// Integrity corrupted
    IntegrityCorrupted,

    /// Validation failed
    ValidationFailed,

    /// Encryption failed
    EncryptionFailed,

    /// Resource is busy
    ResourceBusy,

    /// Out of storage space
    OutOfStorageSpace,

    /// Quota exceeded
    QuotaExceeded,

    /// Authentication failed
    AuthenticationFailed,

    /// Key not found
    KeyNotFound,

    /// Serialization failed
    SerializationFailed,

    /// Invalid snapshot ID
    InvalidSnapshotId,

    /// Conversion failed
    ConversionFailed,

    /// Mutex failed
    MutexLockFailed,
}

/// Key-value-store value
#[derive(Clone)]
pub struct KvsValue<T> {
    /// Key-value-store instance
    kvs: Kvs,

    /// Key name
    key: String,

    /// Data type
    data_type: std::marker::PhantomData<T>,
}

/// Key-value-store instance data
struct KvsData {
    /// Store data
    ///
    /// Feature: `FEAT_REQ__KVS__thread_safety` (Mutex)
    kvs: Mutex<HashMap<String, JsonValue>>,

    /// Optional default values
    ///
    /// Feature: `FEAT_REQ__KVS__default_values`
    default: HashMap<String, JsonValue>,

    /// Filename prefix
    fn_pre: String,

    /// Instance ID
    inst_id: InstanceId,

    /// Flush on exit flag
    flush_on_exit: AtomicBool,
}

/// Key-value-store instance
#[derive(Clone)]
pub struct Kvs {
    /// Key-value-store data
    pub(crate) data: Arc<KvsData>,
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

impl From<JsonParseError> for ErrorCode {
    fn from(cause: JsonParseError) -> Self {
        eprintln!(
            "error: JSON parser error: line = {}, column = {}",
            cause.line(),
            cause.column()
        );
        ErrorCode::JsonParserError
    }
}

impl From<JsonGenerateError> for ErrorCode {
    fn from(cause: JsonGenerateError) -> Self {
        eprintln!("error: JSON generator error: msg = {}", cause.message());
        ErrorCode::JsonGeneratorError
    }
}

impl From<FromUtf8Error> for ErrorCode {
    fn from(cause: FromUtf8Error) -> Self {
        eprintln!("error: UTF-8 conversion failed: {:#?}", cause);
        ErrorCode::ConversionFailed
    }
}

impl From<TryFromSliceError> for ErrorCode {
    fn from(cause: TryFromSliceError) -> Self {
        eprintln!("error: try_into from slice failed: {:#?}", cause);
        ErrorCode::ConversionFailed
    }
}

impl From<Vec<u8>> for ErrorCode {
    fn from(cause: Vec<u8>) -> Self {
        eprintln!("error: try_into from u8 vector failed: {:#?}", cause);
        ErrorCode::ConversionFailed
    }
}

impl From<PoisonError<MutexGuard<'_, HashMap<std::string::String, JsonValue>>>> for ErrorCode {
    fn from(cause: PoisonError<MutexGuard<'_, HashMap<std::string::String, JsonValue>>>) -> Self {
        eprintln!("error: Mutex locking failed: {:#?}", cause);
        ErrorCode::MutexLockFailed
    }
}

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

impl Kvs {
    /// Open the key-value-store
    ///
    /// Checks and opens a key-value-store. Flush on exit is enabled by default and can be
    /// controlled with [`flush_on_exit`](Self::flush_on_exit).
    ///
    /// Parameter:
    ///   * `inst_id`: Instance ID
    ///   * `need_defaults`: Fail when no default file was found
    ///   * `need_kvs`: Fail when no KVS file was found
    ///
    /// Feature:
    ///   * `FEAT_REQ__KVS__default_values`
    ///   * `FEAT_REQ__KVS__multiple_kvs`
    ///   * `FEAT_REQ__KVS__integrity_check`
    pub fn open(
        inst_id: InstanceId,
        need_defaults: bool,
        need_kvs: bool,
    ) -> Result<Kvs, ErrorCode> {
        let fn_default = format!("kvs_{inst_id}_default");
        let fn_pre = format!("kvs_{inst_id}");
        let fn_kvs = format!("{fn_pre}_0");

        let default = Self::open_json(&fn_default, need_defaults, false)?;
        let kvs = Self::open_json(&fn_kvs, need_kvs, true)?;

        println!("opened KVS: instance '{inst_id}'");
        println!("max snapshot count: {KVS_MAX_SNAPSHOTS}");

        Ok(Self {
            data: Arc::new(KvsData {
                kvs: Mutex::new(kvs),
                default,
                fn_pre,
                inst_id,
                flush_on_exit: AtomicBool::new(true),
            }),
        })
    }

    /// Control the flush on exit behaviour
    pub fn flush_on_exit(self, flush_on_exit: bool) {
        self.data
            .flush_on_exit
            .store(flush_on_exit, atomic::Ordering::Relaxed);
    }

    /// Open and parse a JSON file
    ///
    /// Return an empty hash when no file was found.
    ///
    /// Features:
    ///   * `FEAT_REQ__KVS__integrity_check`
    ///
    /// Parameter:
    ///   * `need_file`: fail if file doesn't exist
    ///   * `need_hash`: content is verified against a hash file
    fn open_json(
        fn_pre: &str,
        need_file: bool,
        need_hash: bool,
    ) -> Result<HashMap<String, JsonValue>, ErrorCode> {
        let fn_json = format!("{fn_pre}.json");
        let fn_hash = format!("{fn_pre}.hash");
        match fs::read_to_string(&fn_json) {
            Ok(data) => {
                if need_hash {
                    // data exists, read hash file
                    match fs::read(&fn_hash) {
                        Ok(hash) => {
                            let hash_kvs = RollingAdler32::from_buffer(data.as_bytes()).hash();
                            if u32::from_be_bytes(hash.try_into()?) != hash_kvs {
                                eprintln!("error: KVS data corrupted ({fn_json}, {fn_hash})");
                                Err(ErrorCode::ValidationFailed)
                            } else {
                                println!("JSON data has valid hash");
                                let data: JsonValue = data.parse()?;
                                println!("parsing file {fn_json}");
                                Ok(data
                                    .get::<HashMap<_, _>>()
                                    .ok_or(ErrorCode::JsonParserError)?
                                    .clone())
                            }
                        }
                        Err(err) => {
                            eprintln!("error: hash file {fn_hash} not found: {err:#?}");
                            Err(ErrorCode::FileNotFound)
                        }
                    }
                } else {
                    Ok(data
                        .parse::<JsonValue>()?
                        .get::<HashMap<_, _>>()
                        .ok_or(ErrorCode::JsonParserError)?
                        .clone())
                }
            }
            Err(_) => {
                if need_file {
                    eprintln!("error: file {fn_json} not found");
                    Err(ErrorCode::FileNotFound)
                } else {
                    println!("file {fn_json} not found, using empty data");
                    Ok(HashMap::new())
                }
            }
        }
    }

    /// Resets a key-value-store to its initial state
    pub fn reset(&self) -> Result<(), ErrorCode> {
        *self.data.kvs.lock()? = HashMap::new();
        Ok(())
    }

    /// Get list of all keys
    pub fn get_all_keys(&self) -> Result<Vec<String>, ErrorCode> {
        Ok(self
            .data
            .kvs
            .lock()?
            .keys()
            .map(|x| x.to_string())
            .collect())
    }

    /// Check if a key exists
    pub fn key_exists(&self, key: &str) -> Result<bool, ErrorCode> {
        Ok(self.data.kvs.lock()?.contains_key(key))
    }

    /// Get the assigned value for a given key
    ///
    /// See [Variants](https://docs.rs/tinyjson/latest/tinyjson/enum.JsonValue.html#variants) for
    /// supported value types.
    ///
    /// Features:
    ///   * `FEAT_REQ__KVS__default_values`
    pub fn get_value<T: TryFrom<JsonValue>>(&self, key: &str) -> Result<T, ErrorCode>
    where
        <T as TryFrom<JsonValue>>::Error: std::fmt::Debug,
    {
        if let Some(value) = self.data.kvs.lock()?.get(key) {
            match T::try_from(value.clone()) {
                Ok(value) => Ok(value),
                Err(err) => {
                    eprintln!(
                        "error: get_value could not convert JsonValue from KVS store: {err:#?}"
                    );
                    Err(ErrorCode::ConversionFailed)
                }
            }
        } else if let Some(value) = self.data.default.get(key) {
            // check if key has a default value
            match T::try_from(value.clone()) {
                Ok(value) => Ok(value),
                Err(err) => {
                    eprintln!(
                        "error: get_value could not convert JsonValue from default store: {err:#?}"
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
    /// Features:
    ///   * `FEAT_REQ__KVS__default_values`
    ///   * `FEAT_REQ__KVS__default_value_retrieval`
    pub fn get_default_value(&self, key: &str) -> Result<JsonValue, ErrorCode> {
        if let Some(value) = self.data.default.get(key) {
            Ok(value.clone())
        } else {
            Err(ErrorCode::KeyNotFound)
        }
    }

    /// Return if the value wasn't set yet and uses its default value
    ///
    /// Features:
    ///   * `FEAT_REQ__KVS__default_values`
    pub fn is_value_default(&self, key: &str) -> Result<bool, ErrorCode> {
        if self.data.kvs.lock()?.contains_key(key) {
            Ok(false)
        } else if self.data.default.contains_key(key) {
            Ok(true)
        } else {
            Err(ErrorCode::KeyNotFound)
        }
    }

    /// Assign a value to a given key
    pub fn set_value<S: Into<String>, J: Into<JsonValue>>(
        &self,
        key: S,
        value: J,
    ) -> Result<(), ErrorCode> {
        self.data.kvs.lock()?.insert(key.into(), value.into());
        Ok(())
    }

    /// Remove a key
    pub fn remove_key(&self, key: &str) -> Result<(), ErrorCode> {
        if self.data.kvs.lock()?.remove(key).is_some() {
            Ok(())
        } else {
            Err(ErrorCode::KeyNotFound)
        }
    }

    /// Flush the in-memory key-value-store to the persistent store
    ///
    /// Features:
    ///   * `FEAT_REQ__KVS__snapshots`
    ///   * `FEAT_REQ__KVS__persistency`
    ///   * `FEAT_REQ__KVS__integrity_check`
    pub fn flush(&self) -> Result<(), ErrorCode> {
        let json = JsonValue::from(self.data.kvs.lock()?.clone());
        let mut buf = Vec::new();
        let mut gen = JsonGenerator::new(&mut buf).indent("  ");
        gen.generate(&json)?;

        self.snapshot_rotate()?;

        let hash = RollingAdler32::from_buffer(&buf).hash();

        let fn_json = format!("{}_0.json", self.data.fn_pre);
        let data = String::from_utf8(buf)?;
        fs::write(fn_json, &data)?;

        let fn_hash = format!("{}_0.hash", self.data.fn_pre);
        fs::write(fn_hash, hash.to_be_bytes());

        Ok(())
    }

    /// Get the count of snapshots
    pub fn snapshot_count(&self) -> Result<usize, ErrorCode> {
        let mut count = 0;

        for idx in 0..=KVS_MAX_SNAPSHOTS {
            if !Path::new(&format!("{}_{}.json", self.data.fn_pre, idx)).exists() {
                break;
            }

            // skip current KVS but make sure it exists before search for snapshots
            if idx == 0 {
                continue;
            }

            count = idx;
        }

        Ok(count)
    }

    /// Return maximum snapshot count
    pub fn snapshot_max_count() -> Result<usize, ErrorCode> {
        Ok(KVS_MAX_SNAPSHOTS)
    }

    /// Recover key-value-store from snapshot
    ///
    /// Restore a previously created KVS snapshot.
    ///
    /// Parameter:
    ///   * `id`: Snapshot ID
    ///
    /// Features:
    ///   * `FEAT_REQ__KVS__snapshots`
    pub fn snapshot_restore(&self, id: SnapshotId) -> Result<(), ErrorCode> {
        // fail if the snapshot ID is the current KVS
        if id.0 == 0 {
            eprintln!("error: tried to restore current KVS as snapshot");
            return Err(ErrorCode::InvalidSnapshotId);
        }

        if self.snapshot_count()? < id.0 {
            eprintln!("error: tried to restore a non-existing snapshot");
            return Err(ErrorCode::InvalidSnapshotId);
        }

        let kvs = Self::open_json(&format!("{}_{}", self.data.fn_pre, id.0), true, true)?;
        *self.data.kvs.lock()? = kvs;

        Ok(())
    }

    /// Rotate snapshots
    ///
    /// Features:
    ///   * `FEAT_REQ__KVS__snapshots`
    fn snapshot_rotate(&self) -> Result<(), ErrorCode> {
        for idx in (1..=KVS_MAX_SNAPSHOTS).rev() {
            let hash_old = format!("{}_{}.hash", self.data.fn_pre, idx - 1);
            let hash_new = format!("{}_{}.hash", self.data.fn_pre, idx);
            let snap_old = format!("{}_{}.json", self.data.fn_pre, idx - 1);
            let snap_new = format!("{}_{}.json", self.data.fn_pre, idx);

            println!("rotating: {snap_old} -> {snap_new}");

            let res = fs::rename(hash_old, hash_new);
            if let Err(err) = res {
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(err.into());
                }
            }

            let res = fs::rename(snap_old, snap_new);
            if let Err(err) = res {
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(err.into());
                }
            }
        }

        Ok(())
    }

    /// Return the KVS-filename for a given snapshot ID
    pub fn get_kvs_filename(&self, id: SnapshotId) -> String {
        format!("{}_{}.json", self.data.fn_pre, id)
    }

    /// Return the hash-filename for a given snapshot ID
    pub fn get_hash_filename(&self, id: SnapshotId) -> String {
        format!("{}_{}.hash", self.data.fn_pre, id)
    }

    /// Get a value object
    pub fn get_value_object<T>(&self, key: &str) -> Result<KvsValue<T>, ErrorCode> {
        Ok(KvsValue {
            kvs: self.clone(),
            key: key.to_string(),
            data_type: std::marker::PhantomData::<T>,
        })
    }
}

impl Drop for Kvs {
    fn drop(&mut self) {
        if self.data.flush_on_exit.load(atomic::Ordering::Relaxed) {
            let _ = self.flush();
        }
    }
}

impl<T: TryFrom<JsonValue>> KvsValue<T> {
    /// Get the name of the values key
    pub fn get_key_name(&self) -> String {
        self.key.clone()
    }

    /// Get the value
    ///
    /// See [Kvs::get_value] for details.
    pub fn get(&self) -> Result<T, ErrorCode>
    where
        <T as TryFrom<JsonValue>>::Error: std::fmt::Debug,
    {
        self.kvs.get_value(&self.key)
    }

    /// Set a value
    ///
    /// See [Kvs::set_value] for details.
    pub fn set<J: Into<JsonValue>>(&self, value: J) -> Result<(), ErrorCode> {
        self.kvs.set_value(&self.key, value)
    }

    /// Get default value
    ///
    /// See [Kvs::get_default_value] for details.
    pub fn get_default(&self) -> Result<JsonValue, ErrorCode> {
        self.kvs.get_default_value(&self.key)
    }

    /// Return if the value wasn't set yet and uses its default value
    ///
    /// See [Kvs::is_value_default] for details.
    pub fn is_default(&self) -> Result<bool, ErrorCode> {
        self.kvs.is_value_default(&self.key)
    }

    /// Check if this value key exists
    ///
    /// See [Kvs::key_exists] for details.
    pub fn exists(&self) -> Result<bool, ErrorCode> {
        self.kvs.key_exists(&self.key)
    }

    /// Remove value key from key-value-store
    ///
    /// See [Kvs::remove_key] for details.
    pub fn remove(&self) -> Result<(), ErrorCode> {
        self.kvs.remove_key(&self.key)
    }
}
