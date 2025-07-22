//! Snapshots tests.
//!
//! Requirements verified:
//! - Snapshots (feat_req__persistency__snapshots)
//!   The KVS system shall support explicit creation of snapshots identified by unique IDs and allow rollback to previous snapshots.
//!   Snapshots shall also be deletable.

use rust_kvs::prelude::*;
use std::cmp::min;
use std::path::PathBuf;
use tempfile::tempdir;

/// Initialize KVS object with set number of snapshots.
fn init_kvs(
    kvs_provider: &KvsProvider,
    instance_id: InstanceId,
    num_snapshots: usize,
) -> Result<Kvs, ErrorCode> {
    let kvs = kvs_provider.get(KvsParameters::new(instance_id))?;

    // Add snapshots.
    for i in 1..=num_snapshots {
        kvs.set_value("counter", i as f64)?;
        kvs.flush()?;

        assert_eq!(kvs.snapshot_count(), min(i, Kvs::snapshot_max_count()));
    }
    assert!(kvs.snapshot_count() <= Kvs::snapshot_max_count());

    Ok(kvs)
}

#[test]
fn cit_snapshots_snapshot_count_first_flush() -> Result<(), ErrorCode> {
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();
    let kvs_provider = KvsProvider::new(Some(dir_path));

    let kvs = kvs_provider.get(KvsParameters::new(InstanceId(0)))?;
    kvs.set_value("counter", 1.0)?;

    // Not flushed yet - no snapshots.
    assert_eq!(kvs.snapshot_count(), 0);

    // Flush.
    kvs.flush()?;

    // Flushed once - one snapshot.
    assert_eq!(kvs.snapshot_count(), 1);

    Ok(())
}

#[test]
fn cit_snapshots_snapshot_count_full() -> Result<(), ErrorCode> {
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();
    let kvs_provider = KvsProvider::new(Some(dir_path));

    // Create snapshots - one more than max count.
    for counter in 0..=Kvs::snapshot_max_count() {
        let kvs_load = if counter > 0 {
            KvsLoad::Required
        } else {
            KvsLoad::Optional
        };
        let kvs = kvs_provider.get(KvsParameters::new(InstanceId(0)).kvs_load(kvs_load))?;
        kvs.set_value("counter", counter as f64)?;

        assert_eq!(kvs.snapshot_count(), counter);
    }

    // Check if at max.
    {
        let kvs =
            kvs_provider.get(KvsParameters::new(InstanceId(0)).kvs_load(KvsLoad::Required))?;
        assert_eq!(kvs.snapshot_count(), Kvs::snapshot_max_count());
    }

    Ok(())
}

#[test]
fn cit_snapshots_snapshot_max_count() -> Result<(), ErrorCode> {
    // Value is constant.
    assert_eq!(Kvs::snapshot_max_count(), 3);
    Ok(())
}

#[test]
fn cit_snapshots_snapshot_restore_previous_snapshot() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();
    let kvs_provider = KvsProvider::new(Some(dir_path));

    // Arrange.
    let instance_id = InstanceId(0);
    let num_snapshots = 4;
    let kvs = init_kvs(&kvs_provider, instance_id.clone(), num_snapshots)?;

    // Assert.
    kvs.snapshot_restore(&SnapshotId(3))?;
    assert_eq!(kvs.get_value_as::<f64>("counter")?, 1.0);
    Ok(())
}

#[test]
fn cit_snapshots_snapshot_restore_current_snapshot() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();
    let kvs_provider = KvsProvider::new(Some(dir_path));

    // Arrange.
    let instance_id = InstanceId(0);
    let num_snapshots = 2;
    let kvs = init_kvs(&kvs_provider, instance_id.clone(), num_snapshots)?;

    // Assert.
    let result = kvs.snapshot_restore(&SnapshotId(0));
    assert!(result.is_err_and(|e| e == ErrorCode::InvalidSnapshotId));
    Ok(())
}

#[test]
fn cit_snapshots_snapshot_restore_nonexisting_snapshot() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();
    let kvs_provider = KvsProvider::new(Some(dir_path));

    // Arrange.
    let instance_id = InstanceId(0);
    let num_snapshots = 2;
    let kvs = init_kvs(&kvs_provider, instance_id.clone(), num_snapshots)?;

    // Assert.
    let result = kvs.snapshot_restore(&SnapshotId(3));
    assert!(result.is_err_and(|e| e == ErrorCode::InvalidSnapshotId));
    Ok(())
}

#[test]
fn cit_snapshots_get_kvs_filename_existing_snapshot() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();
    let kvs_provider = KvsProvider::new(Some(dir_path.clone()));

    // Arrange.
    let instance_id = InstanceId(0);
    let num_snapshots = 2;
    let kvs = init_kvs(&kvs_provider, instance_id.clone(), num_snapshots)?;

    // Assert.
    let last_snapshot_index = num_snapshots - 1;
    let expected = PathBuf::from(dir_string).join(format!(
        "kvs_{}_{}.json",
        instance_id.clone(),
        last_snapshot_index
    ));
    let actual = kvs.get_kvs_file_path(&SnapshotId(last_snapshot_index))?;
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn cit_snapshots_get_kvs_filename_nonexisting_snapshot() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();
    let kvs_provider = KvsProvider::new(Some(dir_path));

    // Arrange.
    let instance_id = InstanceId(0);
    let num_snapshots = 2;
    let kvs = init_kvs(&kvs_provider, instance_id.clone(), num_snapshots)?;

    // Assert.
    let invalid_snapshot_index = num_snapshots;
    let result = kvs.get_kvs_file_path(&SnapshotId(invalid_snapshot_index));
    assert!(result.is_err_and(|e| e == ErrorCode::FileNotFound));
    Ok(())
}

#[test]
fn cit_snapshots_get_hash_filename_existing_snapshot() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
    let dir_path = dir.path().to_string_lossy().to_string();
    let kvs_provider = KvsProvider::new(Some(dir_path.clone()));

    // Arrange.
    let instance_id = InstanceId(0);
    let num_snapshots = 2;
    let kvs = init_kvs(&kvs_provider, instance_id.clone(), num_snapshots)?;

    // Assert.
    let last_snapshot_index = num_snapshots - 1;
    let expected = PathBuf::from(dir_string).join(format!(
        "kvs_{}_{}.hash",
        instance_id.clone(),
        last_snapshot_index
    ));
    let actual = kvs.get_hash_file_path(&SnapshotId(last_snapshot_index))?;
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn cit_snapshots_get_hash_filename_nonexisting_snapshot() -> Result<(), ErrorCode> {
    // Temp directory.
    let dir = tempdir()?;
<<<<<<< HEAD
    let dir_string = dir.path().to_string_lossy().to_string();
=======
    let dir_path = dir.path().to_string_lossy().to_string();
    let kvs_provider = KvsProvider::new(Some(dir_path));
>>>>>>> 5961a62 (impl: KVS pool - provider-based)

    // Arrange.
    let instance_id = InstanceId(0);
    let num_snapshots = 2;
<<<<<<< HEAD
    let kvs = init_kvs(instance_id, dir_string.clone(), num_snapshots)?;
=======
    let kvs = init_kvs(&kvs_provider, instance_id.clone(), num_snapshots)?;
>>>>>>> 5961a62 (impl: KVS pool - provider-based)

    // Assert.
    let invalid_snapshot_index = num_snapshots;
    let result = kvs.get_hash_file_path(&SnapshotId(invalid_snapshot_index));
    assert!(result.is_err_and(|e| e == ErrorCode::FileNotFound));
    Ok(())
}
