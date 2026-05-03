// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Tests.TestData;
using EngineeredWood.Vortex.Writer;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Cross-validates that vortex's own Rust reader can open and scan files
/// produced by <see cref="VortexFileWriter"/>. Spawns the
/// <c>vortex-validator</c> Rust binary (built alongside the fixture
/// generator at <c>test/EngineeredWood.Vortex.Tests/Rust/</c>); skips if the
/// binary isn't built so CI without a Rust toolchain passes.
///
/// <para>To build the validator: <c>cd test/EngineeredWood.Vortex.Tests/Rust
/// &amp;&amp; cargo build --release --bin vortex-validator</c>.</para>
/// </summary>
public class VortexCrossValidationTests
{
    private static string? FindValidator()
    {
        // Test data dir is at .../EngineeredWood.Vortex.Tests/TestData; the Rust
        // crate is at .../EngineeredWood.Vortex.Tests/Rust/target/release/.
        var testDataDir = TestDataPath.Resolve("struct_int_3rows.vortex");
        var rustTargetDir = Path.Combine(
            Path.GetDirectoryName(testDataDir)!, "..", "Rust", "target", "release");
        var exeName = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? "vortex-validator.exe" : "vortex-validator";
        var path = Path.GetFullPath(Path.Combine(rustTargetDir, exeName));
        return File.Exists(path) ? path : null;
    }

    private static (int ExitCode, string Stdout, string Stderr) RunValidator(string validator, string fileArg)
    {
        var psi = new ProcessStartInfo
        {
            FileName = validator,
            ArgumentList = { fileArg },
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEnd();
        var stderr = p.StandardError.ReadToEnd();
        p.WaitForExit();
        return (p.ExitCode, stdout, stderr);
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenPrimitiveFile()
    {
        var validator = FindValidator();
        if (validator is null)
        {
            // Soft-skip: the Rust validator wasn't built. CI/dev machines
            // without a Rust toolchain still get green; this test only
            // signals when the validator IS available and disagrees.
            return;
        }

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("i32", Int32Type.Default, nullable: false),
            new Field("f64", DoubleType.Default, nullable: true),
            new Field("name", StringType.Default, nullable: false),
        }, metadata: null);
        var i32 = new Int32Array.Builder();
        var f64 = new DoubleArray.Builder();
        var name = new StringArray.Builder();
        for (int i = 0; i < 100; i++)
        {
            i32.Append(i * 3);
            if (i % 5 == 0) f64.AppendNull(); else f64.Append(i + 0.5);
            name.Append($"row-{i:D3}");
        }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { i32.Build(), f64.Build(), name.Build() }, 100);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            Assert.Contains("OK rows=100", stdout);
            Assert.Contains("BATCH rows=100", stdout);
            Assert.Contains("DONE total=100", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenMultiBatchFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("k", Int64Type.Default, nullable: false),
        }, metadata: null);
        var sizes = new[] { 50, 75, 25 };

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema))
            {
                int rows = 0;
                foreach (var sz in sizes)
                {
                    var k = new Int64Array.Builder();
                    for (int i = 0; i < sz; i++) k.Append(rows + i);
                    var batch = new RecordBatch(schema, new IArrowArray[] { k.Build() }, sz);
                    w.WriteBatch(batch);
                    rows += sz;
                }
                w.Close();
            }

            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            Assert.Contains("OK rows=150", stdout);
            // Each batch is its own emitted ArrayStream entry — vortex may merge
            // small batches, so just check the cumulative DONE line.
            Assert.Contains("DONE total=150", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenBitPackedFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("u8", UInt8Type.Default, nullable: false),
            new Field("u32", UInt32Type.Default, nullable: false),
            new Field("u64", UInt64Type.Default, nullable: false),
        }, metadata: null);
        const int n = 2_000;
        var u8B = new UInt8Array.Builder();
        var u32B = new UInt32Array.Builder();
        var u64B = new UInt64Array.Builder();
        for (int i = 0; i < n; i++)
        {
            u8B.Append((byte)(i % 9));    // 4 bits
            u32B.Append((uint)(i % 500));  // 9 bits
            u64B.Append((ulong)(i % 3));   // 2 bits
        }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { u8B.Build(), u32B.Build(), u64B.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            Assert.Contains($"OK rows={n}", stdout);
            Assert.Contains($"DONE total={n}", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenDeltaFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Locally-constant pattern triggers fastlanes.delta dispatch.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("k", UInt32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 4_096;
        var k = new UInt32Array.Builder();
        for (int i = 0; i < n; i++) k.Append((uint)(i / 64) + 1_000_000u);
        var batch = new RecordBatch(schema, new IArrowArray[] { k.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            Assert.Contains($"OK rows={n}", stdout);
            Assert.Contains($"DONE total={n}", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenForFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("ts", Int64Type.Default, nullable: false),
            new Field("offset_from_base", Int32Type.Default, nullable: false),
            new Field("nullable_neg", Int64Type.Default, nullable: true),
        }, metadata: null);
        const int n = 1_500;
        var ts = new Int64Array.Builder();
        var off = new Int32Array.Builder();
        var nn = new Int64Array.Builder();
        for (int i = 0; i < n; i++)
        {
            ts.Append(1_700_000_000_000L + (i * 7L));   // narrow range, high min
            off.Append(-200 + (i % 50));                 // negative min
            if (i % 5 == 0) nn.AppendNull();
            else nn.Append(-100_000L + (i % 100));       // nullable + negative
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { ts.Build(), off.Build(), nn.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            Assert.Contains($"OK rows={n}", stdout);
            Assert.Contains($"DONE total={n}", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenConstantFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("k", Int64Type.Default, nullable: false),
            new Field("flag", BooleanType.Default, nullable: false),
        }, metadata: null);
        const int n = 800;
        var k = new Int64Array.Builder();
        var flag = new BooleanArray.Builder();
        for (int i = 0; i < n; i++) { k.Append(42L); flag.Append(true); }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { k.Build(), flag.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            Assert.Contains($"OK rows={n}", stdout);
            Assert.Contains($"DONE total={n}", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenDecimalFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var d128 = new Decimal128Type(18, 4);
        var d256 = new Decimal256Type(50, 6);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("amount", d128, nullable: false),
            new Field("price", d128, nullable: true),
            new Field("big", d256, nullable: false),
        }, metadata: null);

        var b128 = new Decimal128Array.Builder(d128);
        var b128n = new Decimal128Array.Builder(d128);
        var b256 = new Decimal256Array.Builder(d256);
        const int n = 200;
        for (int i = 0; i < n; i++)
        {
            b128.Append((decimal)(i - 100) * 1.0001m);
            if (i % 4 == 0) b128n.AppendNull();
            else b128n.Append((decimal)(i - 50) * 0.5m);
            b256.Append((decimal)(i - 100) * 12345.6789m);
        }
        var batch = new RecordBatch(schema,
            new IArrowArray[] { b128.Build(), b128n.Build(), b256.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            Assert.Contains($"OK rows={n}", stdout);
            Assert.Contains($"DONE total={n}", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenListFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("xs", new ListType(Int32Type.Default), nullable: false),
        }, metadata: null);
        var listB = new ListArray.Builder(Int32Type.Default);
        var inner = (Int32Array.Builder)listB.ValueBuilder;
        for (int i = 0; i < 30; i++)
        {
            int n = (i % 4) + 1;
            listB.Append();
            for (int j = 0; j < n; j++) inner.Append(i * 100 + j);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { listB.Build() }, 30);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch);

            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            Assert.Contains("OK rows=30", stdout);
            Assert.Contains("DONE total=30", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }
}
