//! Example: Using rust_kvs with a custom binary backend (C++-style, not JSON)

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Custom value type, similar to KeyValueStorageData in C++
#[derive(Debug, Clone, PartialEq)]
enum KvsStorageData {
    Int(i32),
    Str(String),
    Bool(bool),
    // Add more types as needed
    Object(HashMap<String, KvsStorageData>),
    // ...
}

/// Backend trait for pluggable storage
trait KvsBackend {
    fn get(&self, key: &str) -> Option<KvsStorageData>;
    fn set(&mut self, key: String, value: KvsStorageData);
    fn remove(&mut self, key: &str);
    fn flush(&self);
}

/// In-memory binary backend (C++-style)
struct BinaryBackend {
    map: Arc<Mutex<HashMap<String, KvsStorageData>>>,
}

impl BinaryBackend {
    fn new() -> Self {
        Self { map: Arc::new(Mutex::new(HashMap::new())) }
    }
}

impl KvsBackend for BinaryBackend {
    fn get(&self, key: &str) -> Option<KvsStorageData> {
        self.map.lock().unwrap().get(key).cloned()
    }
    fn set(&mut self, key: String, value: KvsStorageData) {
        self.map.lock().unwrap().insert(key, value);
    }
    fn remove(&mut self, key: &str) {
        self.map.lock().unwrap().remove(key);
    }
    fn flush(&self) {
        // Write to disk or persistent storage (not implemented)
    }
}

/// Example API using the backend
struct Kvs<B: KvsBackend> {
    backend: B,
}

impl<B: KvsBackend> Kvs<B> {
    fn new(backend: B) -> Self {
        Self { backend }
    }
    fn set_value(&mut self, key: &str, value: KvsStorageData) {
        self.backend.set(key.to_string(), value);
    }
    fn get_value(&self, key: &str) -> Option<KvsStorageData> {
        self.backend.get(key)
    }
    fn remove_key(&mut self, key: &str) {
        self.backend.remove(key);
    }
    fn flush(&self) {
        self.backend.flush();
    }
}

fn main() {
    let mut kvs = Kvs::new(BinaryBackend::new());
    kvs.set_value("foo", KvsStorageData::Int(123));
    kvs.set_value("bar", KvsStorageData::Str("hello".to_string()));
    println!("foo: {:?}", kvs.get_value("foo"));
    println!("bar: {:?}", kvs.get_value("bar"));
    kvs.remove_key("foo");
    println!("foo after remove: {:?}", kvs.get_value("foo"));
    kvs.flush();
}
