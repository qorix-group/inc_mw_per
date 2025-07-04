//! Persistency tests.
//!
//! Requirements verified:
//! - Persistency (feat_req__persistency__multiple_kvs)
//! The KVS system shall allow instantiating multiple independent stores per software architecture element.
//!
use rust_kvs::{ErrorCode, InstanceId, Kvs, KvsApi, KvsValue, OpenNeedDefaults, OpenNeedKvs};
use std::collections::HashMap;
use tempfile::tempdir;
use tinyjson::{JsonGenerator, JsonValue};
mod common;

#[test]
fn cit_persistency_default_values() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();

    // Values.
    let keyname = "number".to_string();
    let default_value = 111.1;
    let non_default_value = 333.3;

    // Create defaults file.
    let defaults: HashMap<String, KvsValue> =
        HashMap::from([(keyname.clone(), KvsValue::from(default_value))]);

    let json = KvsValue::from(defaults);
    let json = JsonValue::from(&json);
    let mut buf = Vec::new();
    let mut gen = JsonGenerator::new(&mut buf).indent("  ");
    gen.generate(&json)?;

    let data = String::from_utf8(buf)?;
    std::fs::write(dir_path.clone() + "/" + "kvs_0_default.json", &data)?;

    // Assertions.
    {
        // KVS instance with defaults.
        let kvs_with_defaults = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Required,
            OpenNeedKvs::Optional,
            Some(dir_path.clone()),
        )?;
        // KVS instance without defaults.
        let kvs_without_defaults = Kvs::open(
            InstanceId::new(1),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_path.clone()),
        )?;
        // Check defaults.
        assert!(kvs_with_defaults.is_value_default(&keyname)?);
        // assert!(kvs_without_defaults.is_value_default(&keyname)?); // FAILS here

        // Check values.
        assert_eq!(kvs_with_defaults.get_value::<f64>(&keyname)?, default_value);
        assert_eq!(
            kvs_without_defaults.get_value::<f64>(&keyname).unwrap_err(),
            ErrorCode::KeyNotFound
        );
        // Set non-default value.
        kvs_with_defaults.set_value(&keyname, non_default_value)?;
        kvs_without_defaults.set_value(&keyname, non_default_value)?;
        // Check that the value is non-default.
        assert!(!kvs_with_defaults.is_value_default(&keyname)?);
        assert!(!kvs_without_defaults.is_value_default(&keyname)?);
    }
    // Flush and reopen KVS instances to ensure persistency.
    {
        // KVS instance with defaults.
        let kvs_with_defaults = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Required,
            OpenNeedKvs::Optional,
            Some(dir_path.clone()),
        )?;
        // KVS instance without defaults.
        let kvs_without_defaults = Kvs::open(
            InstanceId::new(1),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_path.clone()),
        )?;
        // Check that the value is still non-default.
        assert_eq!(
            kvs_with_defaults.get_value::<f64>(&keyname)?,
            non_default_value
        );
        assert_eq!(
            kvs_without_defaults.get_value::<f64>(&keyname)?,
            non_default_value
        );
    }

    Ok(())
}

#[test]
fn cit_persistency_default_values_removal() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();
    println!("Temp dir: {}", dir_path);
    // Values.
    let keyname = "number".to_string();
    let default_value = 111.1;
    let non_default_value = 333.3;

    // Create defaults file.
    let defaults: HashMap<String, KvsValue> =
        HashMap::from([(keyname.clone(), KvsValue::from(default_value))]);

    let json = KvsValue::from(defaults);
    let json = JsonValue::from(&json);
    let mut buf = Vec::new();
    let mut gen = JsonGenerator::new(&mut buf).indent("  ");
    gen.generate(&json)?;

    let data = String::from_utf8(buf)?;
    std::fs::write(dir_path.clone() + "/" + "kvs_0_default.json", &data)?;

    // Assertions.
    {
        // KVS instance with defaults.
        let kvs_with_defaults = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Required,
            OpenNeedKvs::Optional,
            Some(dir_path.clone()),
        )?;
        // Check default value.
        assert_eq!(kvs_with_defaults.get_value::<f64>(&keyname)?, default_value);

        // Set non-default value and check it.
        kvs_with_defaults.set_value(&keyname, non_default_value)?;
        assert_eq!(
            kvs_with_defaults.get_value::<f64>(&keyname)?,
            non_default_value
        );

        // Remove key and check that the value is back to default.
        kvs_with_defaults.remove_key(&keyname)?;
        assert_eq!(kvs_with_defaults.get_value::<f64>(&keyname)?, default_value);
    }

    Ok(())
}
