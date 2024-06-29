use clap::ArgAction;
use clap::{crate_version, builder::Arg};

use rust_grpc_example::grpc::hash_fs::hash_fs_server::HashFsServer;
use tifs::fs::fs_config::{self};
use tonic::transport::Server;

use rust_grpc_example::grpc::greeter::greeter_server::GreeterServer;

// Runtime to run our server
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .try_init()
                .map_err(|err| anyhow::anyhow!("fail to init tracing subscriber: {}", err))?;

    let matches = clap::builder::Command::new("hashfs-tikv-server")
        .version(crate_version!())
        .author("Hexi Lee, Ulrich Hornung")
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

    let pd_endpoints = endpoints.into_iter()
        .filter_map(|s|s.strip_prefix("tifs:").map(String::from))
        .collect::<Vec<_>>();

    let options_str = matches.get_many::<String>("options")
    .map(|v|{
        v.into_iter().cloned().collect::<Vec<_>>()
    }).unwrap_or(Vec::new());

    let options = fs_config::MountOption::to_vec(
        options_str.iter().map(|s|s.as_str()));

    let addr = "[::]:50051".parse()?;
    let greeter = rust_grpc_example::hash_fs::server::MyGreeter::default();
    let hash_fs = rust_grpc_example::hash_fs::server::HashFsGrpcServer::new(
        pd_endpoints, options).await?;

    tracing::error!("v - Starting gRPC Server...");
    println!("Starting gRPC Server...");
    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .add_service(HashFsServer::new(hash_fs))
        .serve(addr)
        .await?;

    Ok(())
}
