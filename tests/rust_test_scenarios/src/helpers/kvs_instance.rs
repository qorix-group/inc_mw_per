//! KVS instance test helpers.

use rust_kvs::prelude::{ErrorCode, Kvs, KvsApi, KvsBuilder, OpenNeedDefaults, OpenNeedKvs};

use crate::helpers::kvs_parameters::KvsParameters;

/// Create KVS instance based on provided parameters.
pub fn kvs_instance(kvs_parameters: KvsParameters) -> Result<Kvs, ErrorCode> {
    let mut builder = KvsBuilder::new(kvs_parameters.instance_id);

    if let Some(flag) = kvs_parameters.need_defaults {
        builder = builder.need_defaults(match flag {
            OpenNeedDefaults::Optional => false,
            OpenNeedDefaults::Required => true,
        });
    }

    if let Some(flag) = kvs_parameters.need_kvs {
        builder = builder.need_kvs(match flag {
            OpenNeedKvs::Optional => false,
            OpenNeedKvs::Required => true,
        });
    }

    if let Some(dir) = kvs_parameters.dir {
        builder = builder.dir(dir.to_string_lossy().to_string());
    }

    let kvs: Kvs = builder.build()?;

    if let Some(flag) = kvs_parameters.flush_on_exit {
        kvs.set_flush_on_exit(flag);
    }

    Ok(kvs)
}
