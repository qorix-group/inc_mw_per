// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// <https://www.apache.org/licenses/LICENSE-2.0>
//
// SPDX-License-Identifier: Apache-2.0

//! # Common Testcase Functionality
use rust_kvs::KvsValue;
use std::collections::HashMap;
use std::iter::zip;

use rust_kvs::ErrorCode;
use std::time::SystemTime;
use std::{
    fmt,
    path::{Path, PathBuf},
};

pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a temporary directory based on the current timestamp in nanoseconds
    ///
    /// The directory will be removed when the handle is dropped.
    pub fn create() -> Result<TempDir, ErrorCode> {
        let mut path = std::env::temp_dir();
        path.push(format!("{:016x}", Self::get_nanos()));
        std::fs::create_dir(&path)?;
        Ok(TempDir { path })
    }

    /// Set the generated dir to the current working dir
    pub fn set_current_dir(&self) -> Result<(), ErrorCode> {
        Ok(std::env::set_current_dir(&self.path)?)
    }

    /// Return the current timestamp in nanoseconds
    fn get_nanos() -> u128 {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        time.as_nanos()
    }
}

impl std::fmt::Display for TempDir {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.path.display())
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.path.clone());
    }
}

fn cmp_match(left: &KvsValue, right: &KvsValue) -> bool {
    return match (left, right) {
        (KvsValue::Number(l), KvsValue::Number(r)) => l == r,
        (KvsValue::Boolean(l), KvsValue::Boolean(r)) => l == r,
        (KvsValue::String(l), KvsValue::String(r)) => l == r,
        (KvsValue::Null, KvsValue::Null) => true,
        (KvsValue::Array(l), KvsValue::Array(r)) => {
            if l.len() != r.len() {
                return false;
            }
            for (lv, rv) in zip(l, r) {
                if !cmp_match(lv, rv) {
                    return false;
                }
            }
            true
        }
        (KvsValue::Object(l), KvsValue::Object(r)) => {
            if l.len() != r.len() {
                return false;
            }
            if l.keys().ne(r.keys()) {
                return false;
            }
            let keys = l.keys();
            for k in keys {
                if !cmp_match(&l[k], &r[k]) {
                    return false;
                }
            }
            true
        }
        (_, _) => false,
    };
}

pub fn cmp_object(left: HashMap<String, KvsValue>, right: HashMap<String, KvsValue>) -> bool {
    cmp_match(&KvsValue::from(left), &KvsValue::from(right))
}

pub fn cmp_array(left: Vec<KvsValue>, right: Vec<KvsValue>) -> bool {
    cmp_match(&KvsValue::from(left), &KvsValue::from(right))
}
