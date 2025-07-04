//! Persistency tests.
//!
//! Requirements verified:
//! - Persistency (feat_req__persistency__persistency)
//! The KVS system shall persist stored data and provide an API to explicitly trigger persistence.
//! - Store persistent data (feat_req__persistency__persist_data)
//! The KVS shall support storing and loading its data to and from persistent storage.

use rust_kvs::{ErrorCode, InstanceId, Kvs, KvsApi, KvsValue, OpenNeedDefaults, OpenNeedKvs};
use std::collections::HashMap;
use tempfile::tempdir;

mod common;
use crate::common::{cmp_array, cmp_object};

/// Flush on exit is enabled by default.
/// Data will be flushed on `kvs` being dropped.
#[test]
fn cit_persistency_flush_on_exit_enabled() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();

    // Values.
    let kv_number = ("number", 123.4);
    let kv_bool = ("bool", true);
    let kv_str = ("str", "abcd".to_string());
    let kv_null = ("null", ());
    let hashmap = HashMap::from([("sub-number".to_string(), KvsValue::from(789.0))]);
    let kv_obj = ("obj", hashmap.clone());
    let array = vec![
        KvsValue::from(321.0),
        KvsValue::from(false),
        KvsValue::from("dbca".to_string()),
        KvsValue::from(()),
        KvsValue::from(vec![]),
        KvsValue::from(hashmap),
    ];
    let kv_array = ("array", array);

    {
        // First KVS run.
        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_path.clone()),
        )?;

        // Set value of each type.
        kvs.set_value(kv_number.0, kv_number.1)?;
        kvs.set_value(kv_bool.0, kv_bool.1)?;
        kvs.set_value(kv_str.0, kv_str.1.clone())?;
        kvs.set_value(kv_null.0, kv_null.1)?;
        kvs.set_value(kv_obj.0, kv_obj.1.clone())?;
        kvs.set_value(kv_array.0, kv_array.1.clone())?;
    }

    // Assertions.
    {
        // Second KVS run.
        // KVS file is expected to exist.
        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Required,
            Some(dir_path),
        )?;

        // Compare each value.
        assert_eq!(kvs.get_value::<f64>(kv_number.0)?, kv_number.1);
        assert_eq!(kvs.get_value::<bool>(kv_bool.0)?, kv_bool.1);
        assert_eq!(kvs.get_value::<String>(kv_str.0)?, kv_str.1);
        assert_eq!(kvs.get_value::<()>(kv_null.0)?, kv_null.1);
        assert!(cmp_object(
            kvs.get_value::<HashMap<String, KvsValue>>(kv_obj.0)?,
            kv_obj.1
        ));
        assert!(cmp_array(
            kvs.get_value::<Vec<KvsValue>>(kv_array.0)?,
            kv_array.1
        ));
    }

    Ok(())
}

#[test]
fn cit_persistency_flush_on_exit_disabled_drop_data() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();

    // Values.
    let kv_number = ("number", 123.4);
    let kv_bool = ("bool", true);
    let kv_str = ("str", "abcd".to_string());
    let kv_null = ("null", ());
    let hashmap = HashMap::from([("sub-number".to_string(), KvsValue::from(789.0))]);
    let kv_obj = ("obj", hashmap.clone());
    let array = vec![
        KvsValue::from(321.0),
        KvsValue::from(false),
        KvsValue::from("dbca".to_string()),
        KvsValue::from(()),
        KvsValue::from(vec![]),
        KvsValue::from(hashmap),
    ];
    let kv_array = ("array", array);

    {
        // First KVS run.
        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_path.clone()),
        )?;
        kvs.flush_on_exit(false);

        // Set value of each type.
        kvs.set_value(kv_number.0, kv_number.1)?;
        kvs.set_value(kv_bool.0, kv_bool.1)?;
        kvs.set_value(kv_str.0, kv_str.1.clone())?;
        kvs.set_value(kv_null.0, kv_null.1)?;
        kvs.set_value(kv_obj.0, kv_obj.1.clone())?;
        kvs.set_value(kv_array.0, kv_array.1.clone())?;
    }

    // Assertions.
    {
        // Second KVS run.
        // KVS file is expected to not to exist.
        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_path),
        )?;

        // Make sure no keys are defined.
        assert!(kvs.get_all_keys()?.is_empty());
    }

    Ok(())
}

#[test]
fn cit_persistency_flush_on_exit_disabled_manual_flush() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();

    // Values.
    let kv_number = ("number", 123.4);
    let kv_bool = ("bool", true);
    let kv_str = ("str", "abcd".to_string());
    let kv_null = ("null", ());
    let hashmap = HashMap::from([("sub-number".to_string(), KvsValue::from(789.0))]);
    let kv_obj = ("obj", hashmap.clone());
    let array = vec![
        KvsValue::from(321.0),
        KvsValue::from(false),
        KvsValue::from("dbca".to_string()),
        KvsValue::from(()),
        KvsValue::from(vec![]),
        KvsValue::from(hashmap),
    ];
    let kv_array = ("array", array);

    {
        // First KVS run.
        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_path.clone()),
        )?;
        kvs.flush_on_exit(false);

        // Set value of each type.
        kvs.set_value(kv_number.0, kv_number.1)?;
        kvs.set_value(kv_bool.0, kv_bool.1)?;
        kvs.set_value(kv_str.0, kv_str.1.clone())?;
        kvs.set_value(kv_null.0, kv_null.1)?;
        kvs.set_value(kv_obj.0, kv_obj.1.clone())?;
        kvs.set_value(kv_array.0, kv_array.1.clone())?;

        // Explicitly flush.
        kvs.flush()?;
    }

    // Assertions.
    {
        // Second KVS run.
        // KVS file is expected to exist.
        let kvs = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Required,
            Some(dir_path),
        )?;

        // Compare each value.
        assert_eq!(kvs.get_value::<f64>(kv_number.0)?, kv_number.1);
        assert_eq!(kvs.get_value::<bool>(kv_bool.0)?, kv_bool.1);
        assert_eq!(kvs.get_value::<String>(kv_str.0)?, kv_str.1);
        assert_eq!(kvs.get_value::<()>(kv_null.0)?, kv_null.1);
        assert!(cmp_object(
            kvs.get_value::<HashMap<String, KvsValue>>(kv_obj.0)?,
            kv_obj.1
        ));
        assert!(cmp_array(
            kvs.get_value::<Vec<KvsValue>>(kv_array.0)?,
            kv_array.1
        ));
    }

    Ok(())
}
