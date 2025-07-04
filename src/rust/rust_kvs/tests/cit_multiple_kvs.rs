//! Persistency tests.
//!
//! Requirements verified:
//! - Persistency (feat_req__persistency__multiple_kvs)
//! The KVS system shall allow instantiating multiple independent stores per software architecture element.

use rust_kvs::{ErrorCode, InstanceId, Kvs, KvsApi, OpenNeedDefaults, OpenNeedKvs};
use tempfile::tempdir;

mod common;

#[test]
fn cit_persistency_multiple_instances() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();

    // Values.
    let keyname = "number".to_string();
    let value1 = 111.1;
    let value2 = 222.2;

    {
        // Create first KVS instance.
        let kvs1 = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_path.clone()),
        )?;
        // Create second KVS instance.
        let kvs2 = Kvs::open(
            InstanceId::new(1),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Optional,
            Some(dir_path.clone()),
        )?;

        // Set values to both KVS instances.
        kvs1.set_value(&keyname, value1)?;
        kvs2.set_value(&keyname, value2)?;
    }

    // Assertions.
    {
        // Second KVS run.
        let kvs1 = Kvs::open(
            InstanceId::new(0),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Required,
            Some(dir_path.clone()),
        )?;
        let kvs2 = Kvs::open(
            InstanceId::new(1),
            OpenNeedDefaults::Optional,
            OpenNeedKvs::Required,
            Some(dir_path.clone()),
        )?;

        // Compare values, ensure they are not mixed up.
        assert_eq!(kvs1.get_value::<f64>(&keyname)?, value1);
        assert_ne!(kvs1.get_value::<f64>(&keyname)?, value2);

        assert_eq!(kvs2.get_value::<f64>(&keyname)?, value2);
        assert_ne!(kvs2.get_value::<f64>(&keyname)?, value1);
    }

    Ok(())
}
