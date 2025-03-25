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
//! # Verify KVS Default Value Functionality

use rust_kvs::{ErrorCode, InstanceId, Kvs};
use std::collections::HashMap;
use tinyjson::{JsonGenerator, JsonValue};

mod common;
use crate::common::TempDir;

/// Test default values
///   * Default file must exist
///   * Default value must be returned when key isn't set
///   * Key must report that default is used
///   * Key must be returned when it was written and report it
///   * Change in default must be returned when key isn't set
///   * Change in default must be ignored when key was once set
#[test]
fn kvs_without_defaults() -> Result<(), ErrorCode> {
    let dir = TempDir::create()?;
    dir.set_current_dir()?;

    // create defaults file
    let defaults: HashMap<String, JsonValue> = HashMap::from([
        ("number1".to_string(), JsonValue::from(123.0)),
        ("bool1".to_string(), true.into()),
        ("string1".to_string(), "Hello".to_string().into()),
    ]);

    let json = JsonValue::from(defaults);
    let mut buf = Vec::new();
    let mut gen = JsonGenerator::new(&mut buf).indent("  ");
    gen.generate(&json)?;

    let data = String::from_utf8(buf)?;
    std::fs::write("kvs_0_default.json", &data)?;

    // create KVS
    let kvs = Kvs::open(InstanceId::new(0), true, false)?;

    kvs.set_value("number2", 345.0)?;
    kvs.set_value("bool2", false)?;
    kvs.set_value("string2", "Ola".to_string())?;

    assert_eq!(kvs.get_value::<f64>("number1")?, 123.0);
    assert_eq!(kvs.get_value::<f64>("number2")?, 345.0);

    assert!(kvs.get_value::<bool>("bool1")?);
    assert!(!kvs.get_value::<bool>("bool2")?);

    assert_eq!(kvs.get_value::<String>("string1")?, "Hello".to_string());
    assert_eq!(kvs.get_value::<String>("string2")?, "Ola".to_string());

    assert!(kvs.is_value_default("number1")?);
    assert!(!kvs.is_value_default("number2")?);

    assert!(kvs.is_value_default("bool1")?);
    assert!(!kvs.is_value_default("bool2")?);

    assert!(kvs.is_value_default("string1")?);
    assert!(!kvs.is_value_default("string2")?);

    // write same-as-default-value into `bool1`
    kvs.set_value("bool1", true)?;

    // write not-same-as-default into `string1`
    kvs.set_value("string1", "Bonjour".to_string())?;

    // drop the current instance with flush-on-exit enabled and reopen storage
    drop(kvs);

    let kvs = Kvs::open(InstanceId::new(0), false, true)?;

    assert!(kvs.get_value::<bool>("bool1")?);
    assert!(!kvs.is_value_default("bool1")?);

    assert_eq!(kvs.get_value::<String>("string1")?, "Bonjour".to_string());
    assert!(!kvs.is_value_default("string1")?);

    // drop the current instance with flush-on-exit enabled and reopen storage
    drop(kvs);

    // change default of `number1` and `bool1`
    let defaults: HashMap<String, JsonValue> = HashMap::from([
        ("number1".to_string(), JsonValue::from(987.0)),
        ("bool1".to_string(), false.into()),
        ("string1".to_string(), "Hello".to_string().into()),
    ]);

    let json = JsonValue::from(defaults);
    let mut buf = Vec::new();
    let mut gen = JsonGenerator::new(&mut buf).indent("  ");
    gen.generate(&json)?;

    let data = String::from_utf8(buf)?;
    std::fs::write("kvs_0_default.json", &data)?;

    let kvs = Kvs::open(InstanceId::new(0), false, true)?;

    assert_eq!(kvs.get_value::<f64>("number1")?, 987.0);
    assert!(kvs.is_value_default("number1")?);

    assert!(kvs.get_value::<bool>("bool1")?);
    assert!(!kvs.is_value_default("bool1")?);

    Ok(())
}
