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
//! # Verify KVS Base Functionality without Defaults

use rust_kvs::{ErrorCode, InstanceId, Kvs, KvsValue};
use rust_kvs_derive::KvsValue;

mod common;
use crate::common::TempDir;

#[derive(KvsValue)]
struct KvsAccess {
    /// Number in KVS
    number: KvsValue<f64>,

    /// Bool in KVS
    flag: KvsValue<bool>,

    /// Non-KVS value
    _text: String,
}

/// Verify the struct derive macro
#[test]
fn kvs_derive() -> Result<(), ErrorCode> {
    let dir = TempDir::create()?;
    dir.set_current_dir()?;

    let kvs = Kvs::open(InstanceId::new(0), false, false)?;
    let data = KvsAccess::create(&kvs)?;

    // use an existing value
    kvs.set_value("number", 123.0)?;
    assert_eq!(data.number.get()?, 123.0);
    assert!(data.number.set(234.0).is_ok());
    assert_eq!(data.number.get()?, 234.0);
    assert_eq!(kvs.get_value::<f64>("number")?, 234.0);

    // use a non-existing value without a default
    assert_eq!(data.flag.get().err(), Some(ErrorCode::KeyNotFound));
    assert!(data.flag.set(true).is_ok());
    assert!(data.flag.get()?);
    assert!(kvs.get_value::<bool>("flag")?);

    Ok(())
}
