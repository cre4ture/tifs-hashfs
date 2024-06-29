#![feature(duration_constructors)]
#![feature(type_alias_impl_trait)]

use std::{str::FromStr, sync::Arc};
use clap::{Arg, ArgAction};

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .try_init()
                .map_err(|err| anyhow::anyhow!("fail to init tracing subscriber: {}", err))?;

    let matches = clap::builder::Command::new("hashfs-tikv-server")
        .version(clap::crate_version!())
        .author("Hexi Lee")
        .arg(
            Arg::new("device")
                .value_name("ENDPOINTS")
                .required(true)
                .help("all pd endpoints of the tikv cluster, separated by commas (e.g. tifs:127.0.0.1:2379)")
                .index(1)
                .action(ArgAction::Append)
        )
        .arg(
            Arg::new("mount-point")
                .value_name("MOUNT_POINT")
                .required(true)
                .help("act as a client, and mount FUSE at given path")
                .index(2)
        )
        .arg(
            Arg::new("tracer")
                .value_name("TRACER")
                .long("tracer")
                .short('t')
                .help("the tracer <logger | jaeger>, logger by default")
        )
        .arg(
            Arg::new("jaeger-collector")
                .value_name("JAEGER_COLLECTOR")
                .long("jaeger-collector")
                .short('c')
                .help("the jaeger collector endpoint (e.g. tifs:127.0.0.1:14268)")
        )
        .arg(
            Arg::new("jaeger-agent")
                .value_name("JAEGER_AGENT")
                .long("jaeger-agent")
                .short('a')
                .help("the jaeger agent endpoint (e.g. tifs:127.0.0.1:6831)")
        )
        .arg(
            Arg::new("options")
                .value_name("OPTION")
                .long("option")
                .short('o')
                .help("filesystem mount options")
                .action(ArgAction::Append)
        )
        .arg(
            Arg::new("foreground")
                .long("foreground")
                .short('f')
                .help("foreground operation")
        )
        .arg(
            Arg::new("serve")
                .long("serve")
                .help("run in server mode (implies --foreground)")
                .hide(true)
        )
        .arg(
            Arg::new("logfile")
                .long("log-file")
                .value_name("LOGFILE")
                .help("log file in server mode (ignored if --foreground is present)")
        )
        .get_matches();

    let endpoints = matches.get_many::<String>("device").map(|v|{
        v.into_iter().cloned().collect::<Vec<_>>()
    }).unwrap_or(Vec::new());

    // "hash-fs:http://[::1]:50051"
    let pd_endpoints = endpoints.into_iter()
        .filter_map(|s|s.strip_prefix("hash-fs:").map(String::from))
        .collect::<Vec<_>>();

    let options_str = matches.get_many::<String>("options")
    .map(|v|{
        v.into_iter().cloned().collect::<Vec<_>>()
    }).unwrap_or(Vec::new());

    let options = tifs::fs::fs_config::MountOption::to_vec(
        options_str.iter().map(|s|s.as_str()));

    if pd_endpoints.len() == 0 {
        return Err(anyhow::anyhow!("missing endpoint parameter"));
    }

    let endpoint_str = pd_endpoints.first().unwrap();
    let grpc_endpoint = tonic::transport::Endpoint::from_str(endpoint_str)
        .map_err(|err| anyhow::anyhow!("parsing endpoint {} failed: Err: {:?}", endpoint_str, err))?;
    let mut client = rust_grpc_example::grpc::greeter::greeter_client::GreeterClient::connect(grpc_endpoint.clone()).await?;

    let request = tonic::Request::new(rust_grpc_example::grpc::greeter::HelloRequest {
        name: "Tonic".into(),
    });

    println!("Sending request to gRPC Server...");
    let response = client.say_hello(request).await?;

    println!("RESPONSE={:?}", response);

    let hash_fs_client = Arc::new(rust_grpc_example::hash_fs::client::HashFsClient::new(grpc_endpoint));

    let fs = tifs::fs::tikv_fs::TiFs::construct_hash_fs_client(pd_endpoints, options.clone(), hash_fs_client).await?;

    let mount_point = std::fs::canonicalize(
        &matches
            .get_one::<String>("mount-point")
            .ok_or_else(|| anyhow::anyhow!("mount-point is required"))?,
    )?
    .to_string_lossy()
    .to_string();

    tifs::fuse_mount_daemonize(mount_point, options, ||Ok(()), fs).await?;

    anyhow::Ok(())
}
