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

//! # Verify File Check for non-existing KVS File

use rust_kvs::prelude::*;
use std::env::set_current_dir;
use tempfile::tempdir;

/// Start with no KVS and check if the `kvs_load_mode` flag is working
#[test]
fn kvs_check_needs_kvs() -> Result<(), ErrorCode> {
    let dir = tempdir()?;
    set_current_dir(dir.path())?;
    let kvs_provider = KvsProvider::new(None);

    let kvs = kvs_provider.get(
        KvsParameters::new(InstanceId(0))
            .defaults(Defaults::Optional)
            .kvs_load(KvsLoad::Required),
    );

    assert_eq!(kvs.err(), Some(ErrorCode::FileNotFound));

    Ok(())
}
