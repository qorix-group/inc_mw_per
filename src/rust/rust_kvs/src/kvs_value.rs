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



/// Key-value-storage value
#[derive(Clone, Debug, PartialEq)]
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
    Object(KvsMap),
}

/// Key-value storage map type
pub type KvsMap = std::collections::HashMap<String, KvsValue>;

// Ergonomic From<T> implementations for KvsValue allow automatic conversion from basic Rust types
// to the KvsValue enum. This enables easy and type-safe insertion of values into the key-value store.
// For example, you can write KvsValue::from(42) or use .into() on a supported type.

// Convert f64 to KvsValue::F64
impl From<f64> for KvsValue {
    fn from(val: f64) -> Self {
        KvsValue::F64(val)
    }
}
// Convert i32 to KvsValue::I32
impl From<i32> for KvsValue {
    fn from(val: i32) -> Self {
        KvsValue::I32(val)
    }
}
// Convert u32 to KvsValue::U32
impl From<u32> for KvsValue {
    fn from(val: u32) -> Self {
        KvsValue::U32(val)
    }
}
// Convert i64 to KvsValue::I64
impl From<i64> for KvsValue {
    fn from(val: i64) -> Self {
        KvsValue::I64(val)
    }
}
// Convert u64 to KvsValue::U64
impl From<u64> for KvsValue {
    fn from(val: u64) -> Self {
        KvsValue::U64(val)
    }
}
// Convert bool to KvsValue::Boolean
impl From<bool> for KvsValue {
    fn from(val: bool) -> Self {
        KvsValue::Boolean(val)
    }
}
// Convert String to KvsValue::String
impl From<String> for KvsValue {
    fn from(val: String) -> Self {
        KvsValue::String(val)
    }
}
// Convert &str to KvsValue::String
impl From<&str> for KvsValue {
    fn from(val: &str) -> Self {
        KvsValue::String(val.to_string())
    }
}
// Convert unit type () to KvsValue::Null
impl From<()> for KvsValue {
    fn from(_: ()) -> Self {
        KvsValue::Null
    }
}
// Convert Vec<KvsValue> to KvsValue::Array
impl From<Vec<KvsValue>> for KvsValue {
    fn from(val: Vec<KvsValue>) -> Self {
        KvsValue::Array(val)
    }
}
// Convert HashMap<String, KvsValue> to KvsValue::Object
impl From<KvsMap> for KvsValue {
    fn from(val: KvsMap) -> Self {
        KvsValue::Object(val)
    }
}

// Trait for extracting inner values from KvsValue
pub trait KvsValueGet {
    fn get_inner_value(val: &KvsValue) -> Option<&Self>;
}

macro_rules! impl_kvs_get_inner_value {
    ($to:ty, $variant:ident) => {
        impl KvsValueGet for $to {
            fn get_inner_value(v: &KvsValue) -> Option<&$to> {
                match v {
                    KvsValue::$variant(n) => Some(n),
                    _ => None,
                }
            }
        }
    };
}
impl_kvs_get_inner_value!(f64, F64);
impl_kvs_get_inner_value!(i32, I32);
impl_kvs_get_inner_value!(u32, U32);
impl_kvs_get_inner_value!(i64, I64);
impl_kvs_get_inner_value!(u64, U64);

// TryFrom<&KvsValue> for all supported types
use std::convert::TryFrom;

impl<'a> TryFrom<&'a KvsValue> for i32 {
    type Error = &'static str;
    fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
        match value {
            KvsValue::I32(n) => Ok(*n),
            _ => Err("KvsValue is not an i32"),
        }
    }
}
impl<'a> TryFrom<&'a KvsValue> for u32 {
    type Error = &'static str;
    fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
        match value {
            KvsValue::U32(n) => Ok(*n),
            _ => Err("KvsValue is not a u32"),
        }
    }
}
impl<'a> TryFrom<&'a KvsValue> for i64 {
    type Error = &'static str;
    fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
        match value {
            KvsValue::I64(n) => Ok(*n),
            _ => Err("KvsValue is not an i64"),
        }
    }
}
impl<'a> TryFrom<&'a KvsValue> for u64 {
    type Error = &'static str;
    fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
        match value {
            KvsValue::U64(n) => Ok(*n),
            _ => Err("KvsValue is not a u64"),
        }
    }
}
impl<'a> TryFrom<&'a KvsValue> for f64 {
    type Error = &'static str;
    fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
        match value {
            KvsValue::F64(n) => Ok(*n),
            _ => Err("KvsValue is not an f64"),
        }
    }
}
impl<'a> TryFrom<&'a KvsValue> for bool {
    type Error = &'static str;
    fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
        match value {
            KvsValue::Boolean(b) => Ok(*b),
            _ => Err("KvsValue is not a bool"),
        }
    }
}
impl<'a> TryFrom<&'a KvsValue> for String {
    type Error = &'static str;
    fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
        match value {
            KvsValue::String(s) => Ok(s.clone()),
            _ => Err("KvsValue is not a String"),
        }
    }
}
impl<'a> TryFrom<&'a KvsValue> for Vec<KvsValue> {
    type Error = &'static str;
    fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
        match value {
            KvsValue::Array(arr) => Ok(arr.clone()),
            _ => Err("KvsValue is not an Array"),
        }
    }
}
impl<'a> TryFrom<&'a KvsValue> for std::collections::HashMap<String, KvsValue> {
    type Error = &'static str;
    fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
        match value {
            KvsValue::Object(map) => Ok(map.clone()),
            _ => Err("KvsValue is not an Object"),
        }
    }
}
impl<'a> TryFrom<&'a KvsValue> for KvsValue {
    type Error = &'static str;
    fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
        Ok(value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_f64() {
        let v = KvsValue::from(1.23f64);
        assert!(matches!(v, KvsValue::F64(x) if x == 1.23));
    }

    #[test]
    fn test_from_i32() {
        let v = KvsValue::from(-42i32);
        assert!(matches!(v, KvsValue::I32(x) if x == -42));
    }

    #[test]
    fn test_from_u32() {
        let v = KvsValue::from(42u32);
        assert!(matches!(v, KvsValue::U32(x) if x == 42));
    }

    #[test]
    fn test_from_i64() {
        let v = KvsValue::from(-123456789i64);
        assert!(matches!(v, KvsValue::I64(x) if x == -123456789));
    }

    #[test]
    fn test_from_u64() {
        let v = KvsValue::from(123456789u64);
        assert!(matches!(v, KvsValue::U64(x) if x == 123456789));
    }

    #[test]
    fn test_from_bool() {
        let v = KvsValue::from(true);
        assert!(matches!(v, KvsValue::Boolean(true)));
    }

    #[test]
    fn test_from_string() {
        let v = KvsValue::from(String::from("hello"));
        assert!(matches!(v, KvsValue::String(ref s) if s == "hello"));
    }

    #[test]
    fn test_from_str() {
        let v = KvsValue::from("world");
        assert!(matches!(v, KvsValue::String(ref s) if s == "world"));
    }

    #[test]
    fn test_from_unit() {
        let v = KvsValue::from(());
        assert!(matches!(v, KvsValue::Null));
    }

    #[test]
    fn test_from_vec() {
        let v = KvsValue::from(vec![KvsValue::from(1i32), KvsValue::from(2i32)]);
        assert!(matches!(v, KvsValue::Array(ref arr) if arr.len() == 2));
    }

    #[test]
    fn test_from_kvsmap() {
        let mut map = KvsMap::new();
        map.insert("a".to_string(), KvsValue::from(1i32));
        let v = KvsValue::from(map.clone());
        if let KvsValue::Object(ref obj) = v {
            assert!(obj.contains_key("a"));
            assert!(matches!(obj.get("a"), Some(KvsValue::I32(1))));
        } else {
            panic!("Expected KvsValue::Object");
        }
    }
}