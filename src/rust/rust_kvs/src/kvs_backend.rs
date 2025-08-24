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
use crate::kvs_api::{InstanceId, SnapshotId};
use crate::kvs_value::KvsMap;
use std::path::{Path, PathBuf};

/// KVS backend interface.
pub trait KvsBackend {
    /// Load KVS for given instance ID and snapshot ID.
    fn load_kvs(
        &self,
        instance_id: &InstanceId,
        snapshot_id: &SnapshotId,
    ) -> Result<KvsMap, ErrorCode>;

    /// Load defaults for given instance ID.
    fn load_defaults(&self, instance_id: &InstanceId) -> Result<KvsMap, ErrorCode>;

    /// Flush the KvsMap to persistent storage.
    /// Snapshots are rotated and current state is stored as first (0).
    fn flush(&self, kvs_map: &KvsMap, instance_id: &InstanceId) -> Result<(), ErrorCode>;

    /// Count snapshots available in given directory and with given instance ID.
    fn snapshot_count(&self, instance_id: &InstanceId) -> usize;

    /// Max number of snapshots.
    fn snapshot_max_count() -> usize;

    /// Restore snapshot with given ID.
    fn snapshot_restore(
        &self,
        instance_id: &InstanceId,
        snapshot_id: &SnapshotId,
    ) -> Result<KvsMap, ErrorCode>;
}

/// KVS path resolver interface.
pub trait KvsPathResolver {
    /// Create an instance with working directory.
    fn new(working_dir: &Path) -> Self;

    /// Get KVS file name.
    fn kvs_file_name(&self, instance_id: &InstanceId, snapshot_id: &SnapshotId) -> String;

    /// Get KVS file path in working directory.
    fn kvs_file_path(&self, instance_id: &InstanceId, snapshot_id: &SnapshotId) -> PathBuf;

    /// Get hash file name.
    fn hash_file_name(&self, instance_id: &InstanceId, snapshot_id: &SnapshotId) -> String;

    /// Get hash file path in working directory.
    fn hash_file_path(&self, instance_id: &InstanceId, snapshot_id: &SnapshotId) -> PathBuf;

    /// Get defaults file name.
    fn defaults_file_name(&self, instance_id: &InstanceId) -> String;

    /// Get defaults file path in working directory.
    fn defaults_file_path(&self, instance_id: &InstanceId) -> PathBuf;
}
