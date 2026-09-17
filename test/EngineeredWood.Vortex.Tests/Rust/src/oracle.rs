// Decodes a .vortex file with vortex's own reader and writes its values as an
// Arrow IPC file, so EngineeredWood.Vortex.Tests can compare what EW decodes
// from the same file against the reference implementation, value by value.
// Used by VortexUpstreamCompatTests over upstream's published compatibility
// fixtures (vortex-test/compat-gen), whose expected values are otherwise only
// available as Rust code.
//
// Usage: vortex-oracle <file.vortex> <out.arrow>
//
// The Arrow schema is vortex's own mapping of the file's dtype (strings as
// Utf8View, decimals as Decimal128/256, temporal extensions as Arrow temporal
// types); the tests compare logical values, not physical Arrow types. Each
// chunk the scan yields becomes one record batch. On failure, prints
// `ERR <message>` to stderr and exits non-zero.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::StructArray;
use arrow_array::cast::AsArray;
use arrow_ipc::writer::FileWriter;
use arrow_schema::DataType;
use arrow_schema::Field;
use futures::StreamExt;
use futures::pin_mut;
use vortex::VortexSessionDefault;
use vortex::arrow::ArrowSessionExt;
use vortex::file::OpenOptionsSessionExt;
use vortex::io::session::RuntimeSessionExt;
use vortex_array::VortexSessionExecute;
use vortex_buffer::ByteBuffer;
use vortex_session::VortexSession;

fn fail(stage: &str, e: impl std::fmt::Display) -> ! {
    eprintln!("ERR {stage}: {e}");
    std::process::exit(1);
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: vortex-oracle <file.vortex> <out.arrow>");
        std::process::exit(2);
    }
    let input = PathBuf::from(&args[1]);
    let output = PathBuf::from(&args[2]);
    let bytes = std::fs::read(&input).unwrap_or_else(|e| fail("read", e));

    // The facade's default session is what upstream's own compatibility check reads with.
    let session = VortexSession::default().with_tokio();
    let vxf = session
        .open_options()
        .open_buffer(ByteBuffer::from(bytes))
        .unwrap_or_else(|e| fail("open_buffer", e));

    let schema = Arc::new(
        session
            .arrow()
            .to_arrow_schema(vxf.dtype())
            .unwrap_or_else(|e| fail("schema", e)),
    );
    let target = Field::new("", DataType::Struct(schema.fields().clone()), false);

    let file = File::create(&output).unwrap_or_else(|e| fail("create", e));
    let mut writer = FileWriter::try_new(file, &schema).unwrap_or_else(|e| fail("writer", e));

    let stream = vxf
        .scan()
        .and_then(|s| s.into_array_stream())
        .unwrap_or_else(|e| fail("scan", e));
    pin_mut!(stream);
    let mut ctx = session.create_execution_ctx();
    while let Some(next) = stream.next().await {
        let array = next.unwrap_or_else(|e| fail("next", e));
        let arrow = session
            .arrow()
            .execute_arrow(array, Some(&target), &mut ctx)
            .unwrap_or_else(|e| fail("execute_arrow", e));
        let batch = RecordBatch::from(StructArray::from(arrow.as_struct().clone()))
            .with_schema(schema.clone())
            .unwrap_or_else(|e| fail("batch", e));
        writer.write(&batch).unwrap_or_else(|e| fail("write", e));
    }
    writer.finish().unwrap_or_else(|e| fail("finish", e));
}
