//! Example based on `basic.rs`.
//!
//! To run:
//!
//! ```bash
//! cd <REPO_ROOT>
//! cargo run --example flush_on_exit_abort
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

        // `flush_on_exit` is explicitly enabled to enable erroneous behavior.
        kvs.set_flush_on_exit(FlushOnExit::Yes);

        // Set some value - irrelevant.
        kvs.set_value("number", 123.0)?;

        // Explicitly flush, then remove one KVS snapshot file.
        kvs.flush()?;
        let kvs_path = kvs.get_kvs_filename(SnapshotId(0))?;
        std::fs::remove_file(kvs_path)?;

        // Flush happens on `kvs` going out of scope.
        // Panic inside panic cause execution to abort - no unwind possible for internal error.
        panic!("Something wrong happened");
    }

    // This should never be reached.
    panic!("Unreachable")
}
