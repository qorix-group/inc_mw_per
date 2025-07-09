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

/// Runtime Error Codes
#[derive(Debug, PartialEq)]
pub enum ErrorCode {
    /// Error that was not yet mapped
    UnmappedError,

    /// File not found
    FileNotFound,

    /// KVS file read error
    KvsFileReadError,

    /// KVS hash file read error
    KvsHashFileReadError,

    /// JSON parser error
    JsonParserError,

    /// JSON generator error
    JsonGeneratorError,

    /// Physical storage failure
    PhysicalStorageFailure,

    /// Integrity corrupted
    IntegrityCorrupted,

    /// Validation failed
    ValidationFailed,

    /// Encryption failed
    EncryptionFailed,

    /// Resource is busy
    ResourceBusy,

    /// Out of storage space
    OutOfStorageSpace,

    /// Quota exceeded
    QuotaExceeded,

    /// Authentication failed
    AuthenticationFailed,

    /// Key not found
    KeyNotFound,

    /// Serialization failed
    SerializationFailed,

    /// Invalid snapshot ID
    InvalidSnapshotId,

    /// Conversion failed
    ConversionFailed,

    /// Mutex failed
    MutexLockFailed,
}
