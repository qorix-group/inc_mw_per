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
//! ## KVS Async API Implementation
#![allow(unused)]
#![forbid(unsafe_code)]

use crate::{KvsData, InstanceId, KvsValue, ErrorCode, SnapshotId};
use std::path::Path;
use tinyjson::{JsonGenerator, JsonValue};
use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Mutex};
use adler32::RollingAdler32;
use std::fs;
use std::collections::HashMap;

/// Key-value-store instance
#[derive(Clone)]
pub struct KvsAsync {
    /// Key-value-store data
    pub(crate) data: Arc<KvsData>,
}

impl KvsAsync {
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
    pub async fn open(
        inst_id: InstanceId,
        need_defaults: bool,
        need_kvs: bool,
    ) -> Result<KvsAsync, ErrorCode> {
        Ok(Self {
            data: Arc::new(KvsData {
                kvs: Mutex::new(HashMap::new()),
                default: HashMap::new(),
                fn_pre: String::new(),
                inst_id: InstanceId(0),
                flush_on_exit: AtomicBool::new(true),
            }),
        })
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
    pub async fn flush(&self) -> Result<(), ErrorCode> {
        Ok(())
    }

    /// Get the count of snapshots
    pub async fn snapshot_count(&self) -> Result<usize, ErrorCode> {
        Ok(0)
    }

    /// Return maximum snapshot count
    pub fn snapshot_max_count() -> Result<usize, ErrorCode> {
        Ok(0)
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
    pub async fn snapshot_restore(&self, id: SnapshotId) -> Result<(), ErrorCode> {
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
            kvs: KvsType::Async(self.clone()),
            key: key.to_string(),
            data_type: std::marker::PhantomData::<T>,
        })
    }
}

impl Drop for KvsAsync {
    fn drop(&mut self) {
        if self.data.flush_on_exit.load(atomic::Ordering::Relaxed) {
            let _ = self.flush();
        }
    }
}
