// Cross-validation: reads a .vortex file with vortex's own Rust reader and
// prints a deterministic summary so the .NET writer's output can be verified
// against the reference implementation.
//
// Usage: vortex-validator <file.vortex>
//
// On success, prints `OK rows=<N> dtype=<...>` followed by a per-batch line
// `BATCH rows=<N>` for each emitted batch, then exits 0. On failure, prints
// `ERR <message>` to stderr and exits non-zero.

use std::path::PathBuf;

use futures::StreamExt;
use futures::pin_mut;
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_buffer::ByteBuffer;
use vortex_file::OpenOptionsSessionExt;
use vortex_io::session::RuntimeSession;
use vortex_io::session::RuntimeSessionExt;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("usage: vortex-validator <file.vortex>");
        std::process::exit(2);
    }
    let path = PathBuf::from(&args[1]);
    let bytes = std::fs::read(&path)?;
    let buffer = ByteBuffer::from(bytes);

    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>()
        .with_tokio();
    vortex_file::register_default_encodings(&session);

    let vxf = match session.open_options().open_buffer(buffer) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("ERR open_buffer: {e}");
            std::process::exit(1);
        }
    };

    let row_count = vxf.row_count();
    let dtype = format!("{}", vxf.dtype());
    println!("OK rows={} dtype={}", row_count, dtype);

    let stream = match vxf.scan().and_then(|s| s.into_array_stream()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("ERR scan: {e}");
            std::process::exit(1);
        }
    };

    pin_mut!(stream);
    let mut total = 0u64;
    while let Some(next) = stream.next().await {
        match next {
            Ok(arr) => {
                println!("BATCH rows={}", arr.len());
                total += arr.len() as u64;
            }
            Err(e) => {
                eprintln!("ERR next: {e}");
                std::process::exit(1);
            }
        }
    }
    println!("DONE total={}", total);
    Ok(())
}
