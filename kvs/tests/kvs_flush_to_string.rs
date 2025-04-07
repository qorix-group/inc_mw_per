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
//! # Verify KVS Flush to String Functionality

use rust_kvs::{ErrorCode, InstanceId, KvsBuilder};

mod common;
use crate::common::TempDir;

/// JSON data as string
const JSON_DATA: &str = r#"{
  "number": 123
}"#;

/// Test the flush to string functionality
///
/// Only one element is tested as the order is not guaranteed.
#[test]
fn kvs_flush_to_string() -> Result<(), ErrorCode> {
    let dir = TempDir::create()?;
    dir.set_current_dir()?;

    let kvs = KvsBuilder::create(InstanceId::new(0))?
        .need_defaults(false)
        .need_kvs(false)
        .flush_on_exit(false)
        .build()?;

    kvs.set_value("number", 123.0)?;

    let data = kvs.flush_to_string()?;
    assert_eq!(data, JSON_DATA);

    Ok(())
}
