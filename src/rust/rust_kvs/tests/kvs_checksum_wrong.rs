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

//! # Verify KVS Open with wrong Checksum

use rust_kvs::prelude::*;
use std::collections::HashMap;
use std::env::set_current_dir;
use tempfile::tempdir;
use tinyjson::JsonValue;

/// Create a KVS, close it, modify checksum and try to reopen it.
#[test]
fn kvs_checksum_wrong() -> Result<(), ErrorCode> {
    let dir = tempdir()?;
    set_current_dir(dir.path())?;

    let json = JsonValue::Object(HashMap::from([
        ("number".to_string(), 123.0.into()),
        ("bool".to_string(), true.into()),
        ("string".to_string(), "Hello".to_string().into()),
        ("null".to_string(), ().into()),
        (
            "array".to_string(),
            vec![456.0.into(), false.into(), "Bye".to_string().into()].into(),
        ),
    ]));
    let json_str = json.stringify().unwrap();
    let json_path = dir.path().join("kvs_0_0.json");
    std::fs::write(json_path, json_str)?;

    // remember hash filename
    let hash_path = dir.path().join("kvs_0_0.hash");

    // modify the checksum
    std::fs::write(hash_path, vec![0x12, 0x34, 0x56, 0x78])?;

    // opening must fail because of the missing checksum file
    let kvs = KvsBuilder::<Kvs>::new(InstanceId::new(0))
        .need_defaults(false)
        .need_kvs(true)
        .build();

    assert_eq!(kvs.err(), Some(ErrorCode::ValidationFailed));

    Ok(())
}
