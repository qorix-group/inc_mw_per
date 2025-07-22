use crate::error_code::ErrorCode;
use crate::kvs_api::{InstanceId, SnapshotId};
use crate::kvs_backend::{KvsBackend, KvsPathResolver};
use crate::kvs_value::{KvsMap, KvsValue};
use std::fs;
use std::path::{Path, PathBuf};

use tinyjson::{JsonGenerateError, JsonParseError, JsonValue};

/// Backend-specific JsonValue -> KvsValue conversion.
impl From<JsonValue> for KvsValue {
    fn from(val: JsonValue) -> KvsValue {
        match val {
            JsonValue::Number(n) => KvsValue::Number(n),
            JsonValue::Boolean(b) => KvsValue::Boolean(b),
            JsonValue::String(s) => KvsValue::String(s),
            JsonValue::Null => KvsValue::Null,
            JsonValue::Array(arr) => KvsValue::Array(arr.into_iter().map(KvsValue::from).collect()),
            JsonValue::Object(obj) => KvsValue::Object(
                obj.into_iter()
                    .map(|(k, v)| (k.clone(), KvsValue::from(v)))
                    .collect(),
            ),
        }
    }
}

/// Backend-specific KvsValue -> JsonValue conversion.
impl From<KvsValue> for JsonValue {
    fn from(val: KvsValue) -> JsonValue {
        match val {
            KvsValue::Number(n) => JsonValue::Number(n),
            KvsValue::Boolean(b) => JsonValue::Boolean(b),
            KvsValue::String(s) => JsonValue::String(s),
            KvsValue::Null => JsonValue::Null,
            KvsValue::Array(arr) => {
                JsonValue::Array(arr.into_iter().map(JsonValue::from).collect())
            }
            KvsValue::Object(map) => JsonValue::Object(
                map.into_iter()
                    .map(|(k, v)| (k.clone(), JsonValue::from(v)))
                    .collect(),
            ),
        }
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
