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

use crate::error_code::ErrorCode;
use crate::kvs::{InstanceId, OpenNeedDefaults, OpenNeedKvs, SnapshotId};
use crate::kvs_value::KvsValue;

// The KvsApi trait defines the interface for a Key-Value Storage (KVS) API.
// It provides methods for opening a KVS instance, managing key-value pairs, and handling snapshots
pub trait KvsApi {
    fn open(
        instance_id: InstanceId,
        need_defaults: OpenNeedDefaults,
        need_kvs: OpenNeedKvs,
        dir: Option<String>,
    ) -> Result<Self, ErrorCode>
    where
        Self: Sized;

    fn reset(&mut self) -> Result<(), ErrorCode>;
    fn get_all_keys(&self) -> Result<Vec<String>, ErrorCode>;
    fn key_exists(&self, key: &str) -> Result<bool, ErrorCode>;
    fn get_value<T>(&self, key: &str) -> Result<T, ErrorCode>
    where
        for<'a> T: TryFrom<&'a KvsValue> + Clone,
        for<'a> <T as TryFrom<&'a KvsValue>>::Error: std::fmt::Debug;
    fn get_default_value(&self, key: &str) -> Result<KvsValue, ErrorCode>;
    fn is_value_default(&self, key: &str) -> Result<bool, ErrorCode>;
    fn set_value<S: Into<String>, J: Into<KvsValue>>(
        &mut self,
        key: S,
        value: J,
    ) -> Result<(), ErrorCode>;
    fn remove_key(&mut self, key: &str) -> Result<(), ErrorCode>;
    fn flush_on_exit(&mut self, flush_on_exit: bool);
    fn flush(&mut self) -> Result<(), ErrorCode>;
    fn snapshot_count(&self) -> usize;
    fn snapshot_max_count() -> usize
    where
        Self: Sized;
    fn snapshot_restore(&mut self, id: SnapshotId) -> Result<(), ErrorCode>;
    fn get_kvs_filename(&self, id: SnapshotId) -> String;
    fn get_hash_filename(&self, id: SnapshotId) -> String;
}
