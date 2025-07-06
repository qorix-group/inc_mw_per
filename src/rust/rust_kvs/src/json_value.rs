// Abstraction for JSON handling in KVS
// This module provides a trait for JSON operations and an implementation using tinyjson

use std::collections::HashMap;

pub type JsonValue = tinyjson::JsonValue;

// Implementation for tinyjson
use tinyjson::{JsonParseError, JsonGenerateError};

pub struct TinyJson;

impl KvsJson for TinyJson {
    type Value = JsonValue;
    fn parse(s: &str) -> Result<Self::Value, KvsJsonError> {
        s.parse().map_err(|e: JsonParseError| KvsJsonError(format!("parse error: {:?}", e)))
    }
    fn stringify(val: &Self::Value) -> Result<String, KvsJsonError> {
        val.stringify().map_err(|e: JsonGenerateError| KvsJsonError(format!("stringify: {}", e.message())))
    }
    fn get_object(val: &Self::Value) -> Option<&HashMap<String, Self::Value>> {
        val.get::<HashMap<String, JsonValue>>()
    }
    fn get_array(val: &Self::Value) -> Option<&Vec<Self::Value>> {
        val.get::<Vec<JsonValue>>()
    }
    fn get_f64(val: &Self::Value) -> Option<f64> {
        val.get::<f64>().copied()
    }
    fn get_bool(val: &Self::Value) -> Option<bool> {
        val.get::<bool>().copied()
    }
    fn get_string(val: &Self::Value) -> Option<&str> {
        val.get::<String>().map(|s| s.as_str())
    }
    fn is_null(val: &Self::Value) -> bool {
        matches!(val, JsonValue::Null)
    }
    fn to_kvs_value(val: Self::Value) -> KvsValue {
        KvsValue::from(&val)
    }
    fn from_kvs_value(val: &KvsValue) -> Self::Value {
        JsonValue::from(val)
    }
}

// Error type for trait compatibility
#[derive(Debug)]
pub struct KvsJsonError(pub String);

impl std::fmt::Display for KvsJsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for KvsJsonError {}

// Conversion between KvsValue and JsonValue
use crate::kvs_value::KvsValue;
use crate::error_code::ErrorCode;


pub trait KvsJson {
    type Value;
    fn parse(s: &str) -> Result<Self::Value, KvsJsonError>;
    fn stringify(val: &Self::Value) -> Result<String, KvsJsonError>;
    fn get_object(val: &Self::Value) -> Option<&HashMap<String, Self::Value>>;
    fn get_array(val: &Self::Value) -> Option<&Vec<Self::Value>>;
    fn get_f64(val: &Self::Value) -> Option<f64>;
    fn get_bool(val: &Self::Value) -> Option<bool>;
    fn get_string(val: &Self::Value) -> Option<&str>;
    fn is_null(val: &Self::Value) -> bool;
    fn to_kvs_value(val: Self::Value) -> KvsValue;
    /// Convert a &KvsValue into the backend's JSON value type
    fn from_kvs_value(val: &KvsValue) -> Self::Value;
}

// Custom trait for conversion from &KvsValue
pub trait TryFromKvsValue: Sized {
    fn try_from_kvs_value(val: &KvsValue) -> Result<Self, ErrorCode>;
}

macro_rules! impl_tryfrom_kvsvalue {
    ($t:ty, $variant:ident) => {
        impl TryFromKvsValue for $t {
            fn try_from_kvs_value(val: &KvsValue) -> Result<$t, ErrorCode> {
                if let KvsValue::$variant(inner) = val {
                    Ok(inner.clone())
                } else {
                    Err(ErrorCode::ConversionFailed)
                }
            }
        }
    };
}
impl_tryfrom_kvsvalue!(f64, F64);
impl_tryfrom_kvsvalue!(i32, I32);
impl_tryfrom_kvsvalue!(u32, U32);
impl_tryfrom_kvsvalue!(i64, I64);
impl_tryfrom_kvsvalue!(u64, U64);
impl_tryfrom_kvsvalue!(bool, Boolean);
impl_tryfrom_kvsvalue!(String, String);
impl_tryfrom_kvsvalue!(Vec<KvsValue>, Array);
impl_tryfrom_kvsvalue!(HashMap<String, KvsValue>, Object);

impl TryFromKvsValue for () {
    fn try_from_kvs_value(val: &KvsValue) -> Result<(), ErrorCode> {
        if let KvsValue::Null = val {
            Ok(())
        } else {
            Err(ErrorCode::ConversionFailed)
        }
    }
}

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
