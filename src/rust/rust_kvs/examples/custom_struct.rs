//! Example: Using rust_kvs with a custom user-defined struct

use rust_kvs::kvs::{InstanceId, Kvs};
use rust_kvs::kvs_builder::KvsBuilder;
use rust_kvs::kvs_api::KvsApi;
use rust_kvs::kvs_value::KvsValue;

use std::convert::TryFrom;

#[derive(Debug, Clone, PartialEq)]
struct MyStruct {
    a: i32,
    b: String,
}

// Implement conversion from &KvsValue to MyStruct
impl<'a> TryFrom<&'a KvsValue> for MyStruct {
    type Error = String;
    fn try_from(value: &'a KvsValue) -> Result<Self, Self::Error> {
        if let KvsValue::Object(ref map) = value {
            let a = map.get("a")
                .and_then(|v| i32::try_from(v).ok())
                .ok_or("Missing or invalid 'a'")?;
            let b = map.get("b")
                .and_then(|v| String::try_from(v).ok())
                .ok_or("Missing or invalid 'b'")?;
            Ok(MyStruct { a, b })
        } else {
            Err("Expected object".to_string())
        }
    }
}

// Implement conversion from MyStruct to KvsValue
impl Into<KvsValue> for MyStruct {
    fn into(self) -> KvsValue {
        let mut map = std::collections::HashMap::new();
        map.insert("a".to_string(), KvsValue::from(self.a));
        map.insert("b".to_string(), KvsValue::from(self.b));
        KvsValue::Object(map)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {

    // Ensure the storage directory exists
    std::fs::create_dir_all("./kvs_data").expect("Failed to create kvs_data directory");
    // Create a new KVS instance with instance ID 1
    let instance_id = InstanceId::new(1);
    let kvs = KvsBuilder::<Kvs>::new(instance_id)
        .dir("./kvs_data") // Optionally set directory
        .need_defaults(false)
        .need_kvs(false)
        .build()
        .map_err(|e| format!("KVS build error: {:?}", e))?;

    let my_struct = MyStruct { a: 42, b: "hello".to_string() };
    kvs.set_value("my_key", my_struct.clone())
        .map_err(|e| format!("Set value error: {:?}", e))?;
    let loaded: MyStruct = kvs.get_value("my_key")
        .map_err(|e| format!("Get value error: {:?}", e))?;
    assert_eq!(my_struct, loaded);
    println!("Loaded struct: {:?}", loaded);
    Ok(())
}
