//! WARNING
//! This file is not meant to be merged!
//! It's just an example of a selectable backend.

use crate::error_code::ErrorCode;
use crate::kvs_backend::KvsBackend;
use crate::kvs_value::KvsMap;
use std::path::PathBuf;

pub struct MockBackend;

impl KvsBackend for MockBackend {
    fn load_kvs(
        _source_path: PathBuf,
        _verify_hash: bool,
        _hash_source: Option<PathBuf>,
    ) -> Result<KvsMap, ErrorCode> {
        Ok(KvsMap::new())
    }

    fn save_kvs(
        _kvs: &KvsMap,
        _destination_path: PathBuf,
        _add_hash: bool,
    ) -> Result<(), ErrorCode> {
        Err(ErrorCode::UnmappedError)
    }
}
