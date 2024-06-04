#![feature(array_chunks)]
#![feature(map_try_insert)]
#![feature(async_closure)]
#![feature(type_alias_impl_trait)]
#![feature(async_fn_traits)]
#![feature(iter_array_chunks)]
#![feature(int_roundings)]
#![feature(iter_advance_by)]
#![feature(try_trait_v2)]

pub mod fs;
pub mod utils;

use std::{path::PathBuf, time::Duration};

use fs::{async_fs::AsyncFs, fs_config::MountOption};
use fs::client::TlsConfig;
use fs::tikv_fs::TiFs;
use fuser::MountOption as FuseMountOption;

use tokio::fs::{metadata, read_to_string};
use tracing::debug;

const DEFAULT_TLS_CONFIG_PATH: &str = "~/.tifs/tls.toml";

fn default_tls_config_path() -> anyhow::Result<PathBuf> {
    Ok(DEFAULT_TLS_CONFIG_PATH.parse()?)
}

pub async fn mount_tifs_daemonize<F>(
    mountpoint: String,
    endpoints: Vec<&str>,
    options: Vec<MountOption>,
    make_daemon: F,
) -> anyhow::Result<()>
where
    F: FnOnce() -> anyhow::Result<()>,
{
    let mut fuse_options = vec![
        FuseMountOption::FSName(format!("tifs:{}", endpoints.join(","))),
        FuseMountOption::AllowOther,
        FuseMountOption::DefaultPermissions,
    ];

    #[cfg(target_os = "linux")]
    fuse_options.push(FuseMountOption::AutoUnmount);

    fuse_options.extend(MountOption::collect_builtin(options.iter()));

    let tls_cfg_path = options
        .iter()
        .find_map(|opt| {
            if let MountOption::Tls(path) = opt {
                Some(path.parse().map_err(Into::into))
            } else {
                None
            }
        })
        .unwrap_or_else(default_tls_config_path)?;

    let client_cfg: tikv_client::Config = if metadata(&tls_cfg_path).await.is_ok() {
        let client_cfg_contents = read_to_string(tls_cfg_path).await?;
        toml::from_str::<TlsConfig>(&client_cfg_contents)?.into()
    } else {
        Default::default()
    };

    let mut client_cfg = client_cfg.with_default_keyspace();
    client_cfg.timeout = Duration::from_secs(1);
    debug!("use tikv client config: {:?}", client_cfg);

    let fs_impl = TiFs::construct(endpoints, client_cfg, options).await?;

    make_daemon()?;

    fuser::mount2(AsyncFs(fs_impl.clone()), mountpoint, &fuse_options)?;

    Ok(())
}

pub async fn mount_tifs(
    mountpoint: String,
    endpoints: Vec<&str>,
    options: Vec<MountOption>,
) -> anyhow::Result<()> {
    mount_tifs_daemonize(mountpoint, endpoints, options, || Ok(())).await
}
