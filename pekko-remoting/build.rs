use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    
    // Configure prost for remoting protocol
    prost_build::Config::new()
        .file_descriptor_set_path(out_dir.join("remoting_descriptor.bin"))
        .compile_protos(
            &[
                "proto/remoting.proto",
                "proto/cluster.proto",
            ],
            &["proto/"],
        )?;
    
    // Configure tonic for gRPC services if needed
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("remoting_tonic_descriptor.bin"))
        .compile(
            &[
                "proto/remoting.proto",
                "proto/cluster.proto",
            ],
            &["proto/"],
        )?;
    
    Ok(())
}