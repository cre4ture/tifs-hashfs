use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_dir = "api/protos";
    let output_dir = "./src/grpc/generated";
    std::fs::create_dir_all(output_dir)?;

    let proto_list = ["greeter", "hash_fs"];
    let proto_filepaths = proto_list.map(|file| format!("{input_dir}/{file}.proto"));

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir(output_dir).compile_protos(
        &proto_filepaths, &["api/protos"])?;

    for generated_file in proto_list.map(|file| format!("{output_dir}/{file}.rs")) {
        println!("read file: {generated_file}");
        let content = fs::read_to_string(generated_file.clone())?;
        let new_content = content.replace(
            "use tonic::codegen::http::Uri;",
            "use tonic::codegen::http::Uri/**/;\nuse std::convert::TryInto;"
        );
        fs::write(generated_file, new_content)?;
    }

    Ok(())
}
