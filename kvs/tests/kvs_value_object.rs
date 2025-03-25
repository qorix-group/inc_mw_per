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
//! # Verify KVS Value Object Functionality

use rust_kvs::{ErrorCode, InstanceId, Kvs};

mod common;
use crate::common::TempDir;

/// Verify the value object handling
#[test]
fn kvs_value_object() -> Result<(), ErrorCode> {
    let dir = TempDir::create()?;
    dir.set_current_dir()?;

    let kvs = Kvs::open(InstanceId::new(0), false, false)?;

    // use an existing value
    kvs.set_value("number", 123.0)?;
    let obj_num = kvs.get_value_object::<f64>("number")?;
    assert_eq!(obj_num.get()?, 123.0);
    assert!(obj_num.set(234.0).is_ok());
    assert_eq!(obj_num.get()?, 234.0);
    assert_eq!(kvs.get_value::<f64>("number")?, 234.0);

    // use a non-existing value without a default
    let obj_bool = kvs.get_value_object::<bool>("bool")?;
    assert_eq!(obj_bool.get().err(), Some(ErrorCode::KeyNotFound));
    assert!(obj_bool.set(true).is_ok());
    assert!(obj_bool.get()?);
    assert!(kvs.get_value::<bool>("bool")?);

    Ok(())
}
