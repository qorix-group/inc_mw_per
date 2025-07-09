// Basic example of using KvsBuilder to create and use a KVS instance
use rust_kvs::kvs::{InstanceId, Kvs};
use rust_kvs::kvs_api::KvsApi;
use rust_kvs::kvs_builder::KvsBuilder;

fn type_mismatch_demo(kvs: &mut Kvs) {
    // i32 stored, try to get as f64
    kvs.set_value("mismatch_i32", 123_i32)
        .expect("Failed to set i32");
    match kvs.get_value::<f64>("mismatch_i32") {
        Ok(val) => println!("Unexpectedly got f64: {}", val),
        Err(e) => println!("Type mismatch (i32 as f64) as expected: {:?}", e),
    }
    // u32 stored, try to get as i64
    kvs.set_value("mismatch_u32", 456_u32)
        .expect("Failed to set u32");
    match kvs.get_value::<i64>("mismatch_u32") {
        Ok(val) => println!("Unexpectedly got i64: {}", val),
        Err(e) => println!("Type mismatch (u32 as i64) as expected: {:?}", e),
    }
    // i64 stored, try to get as bool
    kvs.set_value("mismatch_i64", 789_i64)
        .expect("Failed to set i64");
    match kvs.get_value::<bool>("mismatch_i64") {
        Ok(val) => println!("Unexpectedly got bool: {}", val),
        Err(e) => println!("Type mismatch (i64 as bool) as expected: {:?}", e),
    }
    // u64 stored, try to get as String
    kvs.set_value("mismatch_u64", 101112_u64)
        .expect("Failed to set u64");
    match kvs.get_value::<String>("mismatch_u64") {
        Ok(val) => println!("Unexpectedly got String: {}", val),
        Err(e) => println!("Type mismatch (u64 as String) as expected: {:?}", e),
    }
    // f64 stored, try to get as i32
    kvs.set_value("mismatch_f64", 3.1415_f64)
        .expect("Failed to set f64");
    match kvs.get_value::<i32>("mismatch_f64") {
        Ok(val) => println!("Unexpectedly got i32: {}", val),
        Err(e) => println!("Type mismatch (f64 as i32) as expected: {:?}", e),
    }
    // bool stored, try to get as f64
    kvs.set_value("mismatch_bool", true)
        .expect("Failed to set bool");
    match kvs.get_value::<f64>("mismatch_bool") {
        Ok(val) => println!("Unexpectedly got f64: {}", val),
        Err(e) => println!("Type mismatch (bool as f64) as expected: {:?}", e),
    }
    // String stored, try to get as u32
    kvs.set_value("mismatch_string", "hello world")
        .expect("Failed to set string");
    match kvs.get_value::<u32>("mismatch_string") {
        Ok(val) => println!("Unexpectedly got u32: {}", val),
        Err(e) => println!("Type mismatch (String as u32) as expected: {:?}", e),
    }
    // null stored, try to get as i32
    kvs.set_value("mismatch_null", ())
        .expect("Failed to set null");
    match kvs.get_value::<i32>("mismatch_null") {
        Ok(val) => println!("Unexpectedly got i32: {}", val),
        Err(e) => println!("Type mismatch (null as i32) as expected: {:?}", e),
    }
    // array stored, try to get as bool
    kvs.set_value("mismatch_array", vec![1_i32.into(), 2_i32.into()])
        .expect("Failed to set array");
    match kvs.get_value::<bool>("mismatch_array") {
        Ok(val) => println!("Unexpectedly got bool: {}", val),
        Err(e) => println!("Type mismatch (array as bool) as expected: {:?}", e),
    }
    // object stored, try to get as f64
    use std::collections::HashMap;
    let mut obj = HashMap::new();
    obj.insert("field1".to_string(), 1_i32.into());
    kvs.set_value("mismatch_object", obj)
        .expect("Failed to set object");
    match kvs.get_value::<f64>("mismatch_object") {
        Ok(val) => println!("Unexpectedly got f64: {}", val),
        Err(e) => println!("Type mismatch (object as f64) as expected: {:?}", e),
    }
}

fn main() {
    // Ensure the storage directory exists
    std::fs::create_dir_all("./kvs_data").expect("Failed to create kvs_data directory");

    // Create a new KVS instance with instance ID 1
    let instance_id = InstanceId::new(1);
    let mut kvs = KvsBuilder::<Kvs>::new(instance_id)
        .dir("./kvs_data") // Set directory for storage
        .build()
        .expect("Failed to build KVS");

    // Set a value
    kvs.set_value("my_key", 42).expect("Failed to set value");

    // Get a value as i32 (requires TryFromKvsValue for i32 is implemented)
    let value_i32 = kvs
        .get_value::<i32>("my_key")
        .expect("Failed to get value as i32");
    println!("my_key (via get_value::<i32>) = {}", value_i32);

    // Remove a key
    // kvs.remove_key("my_key").expect("Failed to remove key");

    // List all keys
    let keys = kvs.get_all_keys().expect("Failed to get all keys");
    println!("All keys: {:?}", keys);

    // Set values for all supported types
    kvs.set_value("i32_key", 123_i32)
        .expect("Failed to set i32");
    kvs.set_value("u32_key", 456_u32)
        .expect("Failed to set u32");
    kvs.set_value("i64_key", 789_i64)
        .expect("Failed to set i64");
    kvs.set_value("u64_key", 101112_u64)
        .expect("Failed to set u64");
    kvs.set_value("f64_key", 3.1415_f64)
        .expect("Failed to set f64");
    kvs.set_value("bool_key", true).expect("Failed to set bool");
    kvs.set_value("string_key", "hello world")
        .expect("Failed to set string");
    kvs.set_value("null_key", ()).expect("Failed to set null");
    kvs.set_value("array_key", vec![1_i32.into(), 2_i32.into(), 3_i32.into()])
        .expect("Failed to set array");
    use std::collections::HashMap;
    let mut obj = HashMap::new();
    obj.insert("field1".to_string(), 1_i32.into());
    obj.insert("field2".to_string(), 2_i32.into());
    kvs.set_value("object_key", obj)
        .expect("Failed to set object");

    // Explicitly flush to ensure file is created
    kvs.flush().expect("Failed to flush KVS");

    // Call the type mismatch demo
    type_mismatch_demo(&mut kvs);

    // Clean up: remove the directory and all its contents
    std::fs::remove_dir_all("./kvs_data").expect("Failed to remove kvs_data directory");
}
