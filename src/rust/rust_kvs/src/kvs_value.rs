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


use std::collections::HashMap;

/// Key-value-storage value
#[derive(Clone, Debug)]
pub enum KvsValue {
    /// 32-bit signed integer
    I32(i32),
    /// 32-bit unsigned integer
    U32(u32),
    /// 64-bit signed integer
    I64(i64),
    /// 64-bit unsigned integer
    U64(u64),
    /// 64-bit float
    F64(f64),

    /// Boolean
    Boolean(bool),

    /// String
    String(String),

    /// Null
    Null,

    /// Array
    Array(Vec<KvsValue>),

    /// Object
    Object(HashMap<String, KvsValue>),
}



// Ergonomic From<T> for KvsValue impls for all basic types and collections
impl From<f64> for KvsValue {
    fn from(val: f64) -> Self {
        KvsValue::F64(val)
    }
}
impl From<i32> for KvsValue {
    fn from(val: i32) -> Self {
        KvsValue::I32(val)
    }
}
impl From<u32> for KvsValue {
    fn from(val: u32) -> Self {
        KvsValue::U32(val)
    }
}
impl From<i64> for KvsValue {
    fn from(val: i64) -> Self {
        KvsValue::I64(val)
    }
}
impl From<u64> for KvsValue {
    fn from(val: u64) -> Self {
        KvsValue::U64(val)
    }
}
impl From<bool> for KvsValue {
    fn from(val: bool) -> Self {
        KvsValue::Boolean(val)
    }
}
impl From<String> for KvsValue {
    fn from(val: String) -> Self {
        KvsValue::String(val)
    }
}
impl From<&str> for KvsValue {
    fn from(val: &str) -> Self {
        KvsValue::String(val.to_string())
    }
}
impl From<()> for KvsValue {
    fn from(_: ()) -> Self {
        KvsValue::Null
    }
}
impl From<Vec<KvsValue>> for KvsValue {
    fn from(val: Vec<KvsValue>) -> Self {
        KvsValue::Array(val)
    }
}
impl From<std::collections::HashMap<String, KvsValue>> for KvsValue {
    fn from(val: std::collections::HashMap<String, KvsValue>) -> Self {
        KvsValue::Object(val)
    }
}