#![feature(array_chunks)]
#![feature(map_try_insert)]
#![feature(type_alias_impl_trait)]
#![feature(async_fn_traits)]
#![feature(iter_array_chunks)]
#![feature(int_roundings)]
#![feature(try_trait_v2)]
#![feature(closure_lifetime_binder)]
#![feature(step_trait)]

pub mod fs;
pub mod utils;
pub mod local_storage;

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
    let fs_impl = fs::tikv_fs::TiFs::construct_direct_tikv(pd_endpoints.clone(), options.clone()).await?;

    fuse_mount_daemonize(mount_point, options, make_daemon, fs_impl).await?;

    Ok(())
}

pub async fn fuse_mount_daemonize<F, FS: fs::async_fs::AsyncFileSystem + 'static>(
    mount_point: String,
    options: Vec<MountOption>,
    make_daemon: F,
    fs_impl: std::sync::Arc<FS>,
) -> anyhow::Result<()>
where
    F: FnOnce() -> anyhow::Result<()>,
{
    let mut fuse_options = vec![
        FuseMountOption::FSName(format!("tifs:{}", uuid::Uuid::new_v4())),
        FuseMountOption::AllowOther,
        FuseMountOption::DefaultPermissions,
    ];

    #[cfg(target_os = "linux")]
    fuse_options.push(FuseMountOption::AutoUnmount);

    fuse_options.extend(MountOption::collect_builtin(options.iter()));

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
