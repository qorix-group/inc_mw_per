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

//! # Verify KVS Base Functionality without Defaults

use rust_kvs::error_code::ErrorCode;
use rust_kvs::kvs::{InstanceId, Kvs};
use rust_kvs::kvs_api::KvsApi;
use rust_kvs::kvs_builder::KvsBuilder;
use rust_kvs::kvs_value::KvsValue;
use std::collections::HashMap;

use crate::common::TempDir;

mod common;
/// Create a key-value-storage without defaults via builder
#[test]
fn kvs_without_defaults_builder() -> Result<(), ErrorCode> {
    let dir = TempDir::create()?;
    dir.set_current_dir()?;

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

    // drop the current instance with flush-on-exit enabled and reopen storage
    drop(kvs);

    let builder = KvsBuilder::<Kvs>::new(InstanceId::new(0));
    let builder = builder.require_existing_kvs();
    let kvs = builder.build()?;

    assert_eq!(kvs.get_value::<f64>("number")?, 123.0);
    assert!(kvs.get_value::<bool>("bool")?);
    assert_eq!(kvs.get_value::<String>("string")?, "Hello");
    assert_eq!(kvs.get_value::<()>("null"), Ok(()));

    let json_array = kvs.get_value::<Vec<KvsValue>>("array")?;
    assert_eq!(f64::try_from(&json_array[0]), Ok(456.0));
    assert_eq!(bool::try_from(&json_array[1]), Ok(false));
    assert_eq!(String::try_from(&json_array[2]), Ok("Bye".to_string()));

    let json_map = kvs.get_value::<HashMap<String, KvsValue>>("object")?;
    assert_eq!(f64::try_from(&json_map["sub-number"]), Ok(789.0));
    assert_eq!(bool::try_from(&json_map["sub-bool"]), Ok(true));
    assert_eq!(
        String::try_from(&json_map["sub-string"]),
        Ok("Hi".to_string())
    );
    assert_eq!(<()>::try_from(&json_map["sub-null"]), Ok(()));

    if let KvsValue::Array(sub_arr) = &json_map["sub-array"] {
        assert_eq!(f64::try_from(&sub_arr[0]), Ok(1246.0));
        assert_eq!(bool::try_from(&sub_arr[1]), Ok(false));
        assert_eq!(String::try_from(&sub_arr[2]), Ok("Moin".to_string()));
    } else {
        panic!("sub-array is not an array");
    }

    // test for non-existent values
    assert_eq!(
        kvs.get_value::<String>("non-existent").err(),
        Some(ErrorCode::KeyNotFound)
    );

    Ok(())
}
