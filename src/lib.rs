#![feature(array_chunks)]
#![feature(map_try_insert)]
#![feature(async_closure)]
#![feature(type_alias_impl_trait)]
#![feature(async_fn_traits)]
#![feature(iter_array_chunks)]
#![feature(int_roundings)]
#![feature(iter_advance_by)]
#![feature(try_trait_v2)]
#![feature(closure_lifetime_binder)]
#![feature(step_trait)]

pub mod fs;
pub mod utils;
pub mod local_storage;
pub mod hash_fs;

use fs::{async_fs::AsyncFs, fs_config::MountOption};
use fuser::MountOption as FuseMountOption;

pub async fn mount_tifs_daemonize<F>(
    mount_point: String,
    pd_endpoints: Vec<&str>,
    options: Vec<MountOption>,
    make_daemon: F,
) -> anyhow::Result<()>
where
    F: FnOnce() -> anyhow::Result<()>,
{
    let mut fuse_options = vec![
        FuseMountOption::FSName(format!("tifs:{}", pd_endpoints.join(","))),
        FuseMountOption::AllowOther,
        FuseMountOption::DefaultPermissions,
    ];

    #[cfg(target_os = "linux")]
    fuse_options.push(FuseMountOption::AutoUnmount);

    fuse_options.extend(MountOption::collect_builtin(options.iter()));

    let fs_impl = fs::tikv_fs::TiFs::construct(pd_endpoints, options).await?;

    make_daemon()?;

    fuser::mount2(AsyncFs(fs_impl.clone()), mount_point, &fuse_options)?;

    Ok(())
}

pub async fn mount_tifs(
    mount_point: String,
    endpoints: Vec<&str>,
    options: Vec<MountOption>,
) -> anyhow::Result<()> {
    mount_tifs_daemonize(mount_point, endpoints, options, || Ok(())).await
}
