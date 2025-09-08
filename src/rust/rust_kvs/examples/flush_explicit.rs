//! Example based on `basic.rs`.
//!
//! To run:
//!
//! ```bash
//! cd <REPO_ROOT>
//! cargo run --example flush_explicit
//! ```

use rust_kvs::prelude::*;
use tempfile::tempdir;

fn main() -> Result<(), ErrorCode> {
    // Temporary directory.
    let dir = tempdir()?;
    let dir_string = dir.path().to_string_lossy().to_string();

    // Instance ID for KVS object instances.
    let instance_id = InstanceId(0);

    {
        // Build KVS instance for given instance ID and temporary directory.
        // `kvs_load` is explicitly set to `KvsLoad::Optional`, but this is the default value.
        // KVS files are not required.
        let builder = KvsBuilder::<Kvs>::new(instance_id)
            .dir(dir_string.clone())
            .kvs_load(KvsLoad::Optional);
        let kvs = builder.build()?;

        // `flush_on_exit` is explicitly disabled to prevent erroneous behavior.
        kvs.set_flush_on_exit(FlushOnExit::No);

        // Set some value - irrelevant.
        kvs.set_value("number", 123.0)?;

        // Explicitly flush, then remove one KVS snapshot file.
        kvs.flush()?;
        let snapshot_id = SnapshotId(0);
        let kvs_path = kvs.get_kvs_filename(snapshot_id)?;
        let hash_path = kvs.get_hash_filename(snapshot_id)?;
        std::fs::remove_file(kvs_path.clone())?;

        // Explicitly flush again.
        // This flush is expected to fail, but decision can be made how to handle.
        match kvs.flush() {
            Ok(_) => panic!("This shouldn't happen in this case"),
            Err(_) => {
                // Try handling the error by removing existing files and trying again.
                let _ = std::fs::remove_file(kvs_path);
                let _ = std::fs::remove_file(hash_path);

                // Flush again.
                // TODO: this will also fail due to a snapshot rotation bug.
                kvs.flush()?;
            }
        }
    }

    println!("Execution proceeds like nothing happened - but this is okay");
    Ok(())
}
