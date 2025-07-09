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

//! # Verify KVS Open with missing Checksum

use rust_kvs::error_code::ErrorCode;
use rust_kvs::kvs::{InstanceId, Kvs, SnapshotId};
use rust_kvs::kvs_api::KvsApi;
use rust_kvs::kvs_builder::KvsBuilder;
use rust_kvs::kvs_value::KvsValue;

mod common;
use crate::common::TempDir;

/// Create a KVS, close it, delete checksum and try to reopen it.
#[test]
fn kvs_checksum_missing() -> Result<(), ErrorCode> {
    let dir = TempDir::create()?;
    dir.set_current_dir()?;

    // Also create a default file for completeness
    let default_json = r#"{
  "string1": "Hello",
  "bool1": false,
  "number1": 987
}"#;
    std::fs::write("kvs_0_default.json", default_json)?;

    let mut kvs = KvsBuilder::<Kvs>::new(InstanceId::new(0)).build()?;

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

    // remember hash filename
    let hash_filename = kvs.get_hash_filename(SnapshotId::new(0));

    // drop the current instance with flush-on-exit enabled and reopen storage
    drop(kvs);

    // delete the checksum
    std::fs::remove_file(hash_filename)?;

    // opening must fail because of the missing checksum file
    let kvs = KvsBuilder::<Kvs>::new(InstanceId::new(0))
        .require_existing_kvs()
        .build();

    assert_eq!(kvs.err(), Some(ErrorCode::KvsHashFileReadError));

    Ok(())
}
