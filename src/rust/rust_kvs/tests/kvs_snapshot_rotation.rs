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

//! # Verify Snapshot Rotation

use rust_kvs::prelude::*;
use std::collections::HashMap;
use std::env::set_current_dir;
use tempfile::tempdir;

/// Create an example KVS
fn create_kvs(kvs_provider: &KvsProvider) -> Result<Kvs, ErrorCode> {
    let kvs = kvs_provider.get(KvsParameters::new(InstanceId(0)).kvs_load(KvsLoad::Optional))?;

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
    set_current_dir(dir.path())?;
    let kvs_provider = KvsProvider::new(None);

    let max_count = Kvs::snapshot_max_count();
    let mut kvs = create_kvs(&kvs_provider)?;

    // max count is added twice to make sure we rotate once
    let mut cnts: Vec<usize> = (0..=max_count).collect();
    let mut cnts_post: Vec<usize> = vec![max_count];
    cnts.append(&mut cnts_post);

    // make sure the snapshot count is 0, 0, .., max_count, max_count (rotation)
    for cnt in cnts {
        assert_eq!(kvs.snapshot_count(), cnt);

        // drop the current instance with flush-on-exit enabled and re-open it
        drop(kvs);
        kvs = kvs_provider.get(KvsParameters::new(InstanceId(0)).kvs_load(KvsLoad::Required))?;
    }

    // restore the oldest snapshot
    assert!(kvs.snapshot_restore(&SnapshotId(max_count)).is_ok());

    // try to restore a snapshot behind the last one
    assert_eq!(
        kvs.snapshot_restore(&SnapshotId(max_count + 1)).err(),
        Some(ErrorCode::InvalidSnapshotId)
    );

    Ok(())
}
