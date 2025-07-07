use crate::kvs_value::{KvsMap, KvsValue};
use crate::json_value::{KvsJson, TinyJson};
use std::fs;

/// Trait for persisting and loading KvsMap from storage
pub trait PersistKvs {
    /// Load a KvsMap from a JSON file using the backend
    fn get_kvs_from_file(filename: &str, kvs: &mut KvsMap) -> Result<(), String>;
    /// Persist a KvsMap to a JSON file using the backend
    fn persist_kvs_to_file(kvs: &KvsMap, filename: &str) -> Result<(), String>;
}

#[derive(Default)]
/// Generic implementation for any backend that implements KvsJson
pub struct DefaultPersistKvs<J: KvsJson = TinyJson> {
    _marker: std::marker::PhantomData<J>,
}

impl<J: KvsJson> PersistKvs for DefaultPersistKvs<J> {
    fn get_kvs_from_file(filename: &str, kvs: &mut KvsMap) -> Result<(), String> {
        let data = fs::read_to_string(filename)
            .map_err(|e| format!("file read error: {e}"))?;
        let json_val = J::parse(&data)
            .map_err(|e| format!("json parse error: {e}"))?;
        let kvs_val = J::to_kvs_value(json_val);
        if let KvsValue::Object(map) = kvs_val {
            kvs.clear();
            kvs.extend(map);
            Ok(())
        } else {
            Err("top-level JSON is not an object".to_string())
        }
    }

    fn persist_kvs_to_file(kvs: &KvsMap, filename: &str) -> Result<(), String> {
        let kvs_val = KvsValue::Object(kvs.clone());
        let json_val = J::from_kvs_value(&kvs_val);
        let json_str = J::stringify(&json_val)
            .map_err(|e| format!("json stringify error: {e}"))?;
        fs::write(filename, json_str)
            .map_err(|e| format!("file write error: {e}"))?;
        Ok(())
    }
}
