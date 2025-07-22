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

use crate::error_code::ErrorCode;
use crate::kvs_api::{InstanceId, SnapshotId};
use crate::kvs_backend::{KvsBackend, KvsPathResolver};
use crate::kvs_value::{KvsMap, KvsValue};
use std::fs;
use std::path::{Path, PathBuf};

// for creating jsonvalue obj
use std::collections::HashMap;

use tinyjson::{JsonGenerateError, JsonParseError, JsonValue};

// Example of how KvsValue is stored in the JSON file (t-tagged format):
// {
//   "my_int": { "t": "i32", "v": 42 },
//   "my_float": { "t": "f64", "v": 3.1415 },
//   "my_bool": { "t": "bool", "v": true },
//   "my_string": { "t": "str", "v": "hello" },
//   "my_array": { "t": "arr", "v": [ ... ] },
//   "my_object": { "t": "obj", "v": { ... } },
//   "my_null": { "t": "null", "v": null }
// }

/// Backend-specific JsonValue -> KvsValue conversion.
impl From<JsonValue> for KvsValue {
    fn from(val: JsonValue) -> KvsValue {
        match val {
            JsonValue::Object(mut obj) => {
                // Type-tagged: { "t": ..., "v": ... }
                if let (Some(JsonValue::String(type_str)), Some(value)) =
                    (obj.remove("t"), obj.remove("v"))
                {
                    return match (type_str.as_str(), value) {
                        ("i32", JsonValue::Number(v)) => KvsValue::I32(v as i32),
                        ("u32", JsonValue::Number(v)) => KvsValue::U32(v as u32),
                        ("i64", JsonValue::Number(v)) => KvsValue::I64(v as i64),
                        ("u64", JsonValue::Number(v)) => KvsValue::U64(v as u64),
                        ("f64", JsonValue::Number(v)) => KvsValue::F64(v),
                        ("bool", JsonValue::Boolean(v)) => KvsValue::Boolean(v),
                        ("str", JsonValue::String(v)) => KvsValue::String(v),
                        ("null", JsonValue::Null) => KvsValue::Null,
                        ("arr", JsonValue::Array(v)) => {
                            KvsValue::Array(v.into_iter().map(KvsValue::from).collect())
                        }
                        ("obj", JsonValue::Object(v)) => KvsValue::Object(
                            v.into_iter().map(|(k, v)| (k, KvsValue::from(v))).collect(),
                        ),
                        // Remaining types can be handled with Null.
                        _ => KvsValue::Null,
                    };
                }
                // If not a t-tagged object, treat as a map of key-value pairs (KvsMap)
                let map: KvsMap = obj
                    .into_iter()
                    .map(|(k, v)| (k, KvsValue::from(v)))
                    .collect();
                KvsValue::Object(map)
            }
            // Remaining types can be handled with Null.
            _ => KvsValue::Null,
        }
    }
}

/// Backend-specific KvsValue -> JsonValue conversion.
impl From<KvsValue> for JsonValue {
    fn from(val: KvsValue) -> JsonValue {
        let mut obj = HashMap::new();
        match val {
            KvsValue::I32(n) => {
                obj.insert("t".to_string(), JsonValue::String("i32".to_string()));
                obj.insert("v".to_string(), JsonValue::Number(n as f64));
            }
            KvsValue::U32(n) => {
                obj.insert("t".to_string(), JsonValue::String("u32".to_string()));
                obj.insert("v".to_string(), JsonValue::Number(n as f64));
            }
            KvsValue::I64(n) => {
                obj.insert("t".to_string(), JsonValue::String("i64".to_string()));
                obj.insert("v".to_string(), JsonValue::Number(n as f64));
            }
            KvsValue::U64(n) => {
                obj.insert("t".to_string(), JsonValue::String("u64".to_string()));
                obj.insert("v".to_string(), JsonValue::Number(n as f64));
            }
            KvsValue::F64(n) => {
                obj.insert("t".to_string(), JsonValue::String("f64".to_string()));
                obj.insert("v".to_string(), JsonValue::Number(n));
            }
            KvsValue::Boolean(b) => {
                obj.insert("t".to_string(), JsonValue::String("bool".to_string()));
                obj.insert("v".to_string(), JsonValue::Boolean(b));
            }
            KvsValue::String(s) => {
                obj.insert("t".to_string(), JsonValue::String("str".to_string()));
                obj.insert("v".to_string(), JsonValue::String(s));
            }
            KvsValue::Null => {
                obj.insert("t".to_string(), JsonValue::String("null".to_string()));
                obj.insert("v".to_string(), JsonValue::Null);
            }
            KvsValue::Array(arr) => {
                obj.insert("t".to_string(), JsonValue::String("arr".to_string()));
                obj.insert(
                    "v".to_string(),
                    JsonValue::Array(arr.into_iter().map(JsonValue::from).collect()),
                );
            }
            KvsValue::Object(map) => {
                obj.insert("t".to_string(), JsonValue::String("obj".to_string()));
                obj.insert(
                    "v".to_string(),
                    JsonValue::Object(
                        map.into_iter()
                            .map(|(k, v)| (k, JsonValue::from(v)))
                            .collect(),
                    ),
                );
            }
        }
        JsonValue::Object(obj)
    }
}

/// tinyjson::JsonParseError -> ErrorCode::JsonParseError
impl From<JsonParseError> for ErrorCode {
    fn from(cause: JsonParseError) -> Self {
        eprintln!(
            "error: JSON parser error: line = {}, column = {}",
            cause.line(),
            cause.column()
        );
        ErrorCode::JsonParserError
    }
}

/// tinyjson::JsonGenerateError -> ErrorCode::JsonGenerateError
impl From<JsonGenerateError> for ErrorCode {
    fn from(cause: JsonGenerateError) -> Self {
        eprintln!("error: JSON generator error: msg = {}", cause.message());
        ErrorCode::JsonGeneratorError
    }
}

/// KVS backend implementation based on TinyJSON.
pub struct JsonBackend;

impl JsonBackend {
    fn parse(s: &str) -> Result<JsonValue, ErrorCode> {
        s.parse()
            .map_err(|_e: JsonParseError| crate::error_code::ErrorCode::JsonParserError)
    }

    fn stringify(val: &JsonValue) -> Result<String, ErrorCode> {
        val.stringify()
            .map_err(|_e: JsonGenerateError| crate::error_code::ErrorCode::JsonParserError)
    }
}

/// Check path have correct extension.
fn check_extension(path: &Path, extension: &str) -> bool {
    let ext = path.extension();
    ext.is_some_and(|ep| ep.to_str().is_some_and(|es| es == extension))
}

impl KvsBackend for JsonBackend {
    fn load_kvs(kvs_path: &Path, hash_path: Option<&PathBuf>) -> Result<KvsMap, ErrorCode> {
        if !check_extension(kvs_path, "json") {
            return Err(ErrorCode::KvsFileReadError);
        }
        if hash_path.is_some_and(|p| !check_extension(p, "hash")) {
            return Err(ErrorCode::KvsHashFileReadError);
        }

        // Load KVS file and parse from string to `JsonValue`.
        let json_str = fs::read_to_string(kvs_path)?;
        let json_value = Self::parse(&json_str)?;

        // Perform hash check.
        if let Some(hash_path) = hash_path {
            match fs::read(hash_path) {
                Ok(hash_bytes) => {
                    let hash_kvs = adler32::RollingAdler32::from_buffer(json_str.as_bytes()).hash();
                    if hash_bytes.len() == 4 {
                        let file_hash = u32::from_be_bytes([
                            hash_bytes[0],
                            hash_bytes[1],
                            hash_bytes[2],
                            hash_bytes[3],
                        ]);
                        if hash_kvs != file_hash {
                            return Err(ErrorCode::ValidationFailed);
                        }
                    } else {
                        return Err(ErrorCode::ValidationFailed);
                    }
                }
                Err(_) => return Err(ErrorCode::KvsHashFileReadError),
            };
        }

        // Cast from `JsonValue` to `KvsValue`.
        let kvs_value = KvsValue::from(json_value);
        if let KvsValue::Object(kvs_map) = kvs_value {
            Ok(kvs_map)
        } else {
            Err(ErrorCode::JsonParserError)
        }
    }

    fn save_kvs(
        kvs_map: &KvsMap,
        kvs_path: &Path,
        hash_path: Option<&PathBuf>,
    ) -> Result<(), ErrorCode> {
        // Validate extensions.
        if !check_extension(kvs_path, "json") {
            return Err(ErrorCode::KvsFileReadError);
        }
        if hash_path.is_some_and(|p| !check_extension(p, "hash")) {
            return Err(ErrorCode::KvsHashFileReadError);
        }

        // Cast from `KvsValue` to `JsonValue`.
        let kvs_value = KvsValue::Object(kvs_map.clone());
        let json_value = JsonValue::from(kvs_value);

        // Stringify `JsonValue` and save to KVS file.
        let json_str = Self::stringify(&json_value)?;
        fs::write(kvs_path, &json_str)?;

        // Generate hash and save to hash file.
        if let Some(hash_path) = hash_path {
            let hash = adler32::RollingAdler32::from_buffer(json_str.as_bytes()).hash();
            fs::write(hash_path, hash.to_be_bytes())?
        }

        Ok(())
    }
}

/// KVS backend path resolver for `JsonBackend`.
impl KvsPathResolver for JsonBackend {
    fn kvs_file_name(instance_id: &InstanceId, snapshot_id: &SnapshotId) -> String {
        format!("kvs_{instance_id}_{snapshot_id}.json")
    }

    fn kvs_file_path(
        working_dir: &Path,
        instance_id: &InstanceId,
        snapshot_id: &SnapshotId,
    ) -> PathBuf {
        working_dir.join(Self::kvs_file_name(instance_id, snapshot_id))
    }

    fn hash_file_name(instance_id: &InstanceId, snapshot_id: &SnapshotId) -> String {
        format!("kvs_{instance_id}_{snapshot_id}.hash")
    }

    fn hash_file_path(
        working_dir: &Path,
        instance_id: &InstanceId,
        snapshot_id: &SnapshotId,
    ) -> PathBuf {
        working_dir.join(Self::hash_file_name(instance_id, snapshot_id))
    }

    fn defaults_file_name(instance_id: &InstanceId) -> String {
        format!("kvs_{instance_id}_default.json")
    }

    fn defaults_file_path(working_dir: &Path, instance_id: &InstanceId) -> PathBuf {
        working_dir.join(Self::defaults_file_name(instance_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_error_code_from_json_parse_error() {
        let error = tinyjson::JsonParser::new("[1, 2, 3".chars())
            .parse()
            .unwrap_err();
        assert_eq!(ErrorCode::from(error), ErrorCode::JsonParserError);
    }

    #[test]
    fn test_unknown_error_code_from_json_generate_error() {
        let data: JsonValue = JsonValue::Number(f64::INFINITY);
        let error = data.stringify().unwrap_err();
        assert_eq!(ErrorCode::from(error), ErrorCode::JsonGeneratorError);
    }
}

#[cfg(test)]
mod path_backend_tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_kvs_file_name() {
        let instance_id = InstanceId(123);
        let snapshot_id = SnapshotId(2);
        let exp_name = format!("kvs_{instance_id}_{snapshot_id}.json");
        let act_name = JsonBackend::kvs_file_name(&instance_id, &snapshot_id);
        assert_eq!(exp_name, act_name);
    }

    #[test]
    fn test_kvs_file_path() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        let instance_id = InstanceId(123);
        let snapshot_id = SnapshotId(2);
        let exp_name = dir_path.join(format!("kvs_{instance_id}_{snapshot_id}.json"));
        let act_name = JsonBackend::kvs_file_path(dir_path, &instance_id, &snapshot_id);
        assert_eq!(exp_name, act_name);
    }
    #[test]
    fn test_hash_file_name() {
        let instance_id = InstanceId(123);
        let snapshot_id = SnapshotId(2);
        let exp_name = format!("kvs_{instance_id}_{snapshot_id}.hash");
        let act_name = JsonBackend::hash_file_name(&instance_id, &snapshot_id);
        assert_eq!(exp_name, act_name);
    }

    #[test]
    fn test_hash_file_path() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        let instance_id = InstanceId(123);
        let snapshot_id = SnapshotId(2);
        let exp_name = dir_path.join(format!("kvs_{instance_id}_{snapshot_id}.hash"));
        let act_name = JsonBackend::hash_file_path(dir_path, &instance_id, &snapshot_id);
        assert_eq!(exp_name, act_name);
    }

    #[test]
    fn test_defaults_file_name() {
        let instance_id = InstanceId(123);
        let exp_name = format!("kvs_{instance_id}_default.json");
        let act_name = JsonBackend::defaults_file_name(&instance_id);
        assert_eq!(exp_name, act_name);
    }

    #[test]
    fn test_defaults_file_path() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        let instance_id = InstanceId(123);
        let exp_name = dir_path.join(format!("kvs_{instance_id}_default.json"));
        let act_name = JsonBackend::defaults_file_path(dir_path, &instance_id);
        assert_eq!(exp_name, act_name);
    }
}
