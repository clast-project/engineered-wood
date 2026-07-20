// Copyright (c) clast-project. All rights reserved.
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
        // TestDataPath.Resolve returns bin/.../TestData/<file>; walk up until
        // we land on the EngineeredWood.Vortex.Tests project dir (sibling to
        // the Rust crate at Rust/target/release/vortex-validator).
        var exeName = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? "vortex-validator.exe" : "vortex-validator";
        var dir = Path.GetDirectoryName(TestDataPath.Resolve("struct_int_3rows.vortex"));
        for (int i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, "Rust", "target", "release", exeName);
            if (File.Exists(candidate)) return candidate;
            dir = Path.GetDirectoryName(dir);
        }
        return null;
    }

    private static (int ExitCode, string Stdout, string Stderr) RunValidator(string validator, string fileArg)
    {
        var psi = new ProcessStartInfo
        {
            FileName = validator,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
#if NETCOREAPP2_1_OR_GREATER
        psi.ArgumentList.Add(fileArg);
#else
        psi.Arguments = "\"" + fileArg.Replace("\"", "\\\"") + "\"";
#endif
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
    public void RustReader_OpensDotNetWrittenListOfStructFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // List<Struct<id: int32, name: string>> — exercises both list and
        // struct array-level encodings cascading.
        var elemType = new StructType(new[]
        {
            new Field("id", Int32Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: false),
        });
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("rows", new ListType(new Field("item", elemType, nullable: false)), nullable: false),
        }, metadata: null);

        const int n = 50;
        var idB = new Int32Array.Builder();
        var nameB = new StringArray.Builder();
        var listOffsets = new int[n + 1];
        int totalElems = 0;
        for (int i = 0; i < n; i++)
        {
            listOffsets[i] = totalElems;
            int len = (i % 3) + 1;
            for (int j = 0; j < len; j++) { idB.Append(i * 100 + j); nameB.Append($"r{i}_{j}"); }
            totalElems += len;
        }
        listOffsets[n] = totalElems;
        var elemsStruct = new StructArray(elemType, totalElems,
            new IArrowArray[] { idB.Build(), nameB.Build() }, ArrowBuffer.Empty, 0);
        var offsetsBytes = new byte[(n + 1) * 4];
        for (int i = 0; i <= n; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(
                offsetsBytes.AsSpan(i * 4, 4), listOffsets[i]);
        var listArr = new ListArray(
            new ListType(new Field("item", elemType, nullable: false)),
            n, new ArrowBuffer(offsetsBytes), elemsStruct, ArrowBuffer.Empty, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { listArr }, n);

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
    public void RustReader_OpensDotNetWrittenStructOfStructFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var innerType = new StructType(new[]
        {
            new Field("y", Int32Type.Default, nullable: false),
            new Field("z", StringType.Default, nullable: false),
        });
        var outerType = new StructType(new[]
        {
            new Field("x", Int32Type.Default, nullable: false),
            new Field("inner", innerType, nullable: false),
        });
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("nested", outerType, nullable: false),
        }, metadata: null);

        const int n = 100;
        var xB = new Int32Array.Builder();
        var yB = new Int32Array.Builder();
        var zB = new StringArray.Builder();
        for (int i = 0; i < n; i++) { xB.Append(i); yB.Append(i * 2); zB.Append($"v{i}"); }
        var inner = new StructArray(innerType, n,
            new IArrowArray[] { yB.Build(), zB.Build() }, ArrowBuffer.Empty, 0);
        var outer = new StructArray(outerType, n,
            new IArrowArray[] { xB.Build(), inner }, ArrowBuffer.Empty, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { outer }, n);

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
    public void RustReader_OpensDotNetWrittenBitPackedWithPatchesFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", UInt32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 2_048;
        var b = new UInt32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 100 == 0) b.Append((uint)(0x80000000u + (uint)i));
            else b.Append((uint)(i % 200));
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenRleFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Repetitive doubles → fastlanes.rle dispatches.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("d", DoubleType.Default, nullable: false),
        }, metadata: null);
        var palette = new[] { 1.5, 2.71828, -3.14, 100.0 };
        const int n = 2_048;
        var b = new DoubleArray.Builder();
        for (int i = 0; i < n; i++) b.Append(palette[(i / 64) % palette.Length]);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenDictFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Mixed: a non-nullable dict-friendly column AND a nullable one to
        // exercise both is_nullable_codes flag values + the codes-validity
        // child encoding path.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("status", StringType.Default, nullable: false),
            new Field("nullable_status", StringType.Default, nullable: true),
        }, metadata: null);
        var states = new[] { "open", "closed", "pending", "error" };
        const int n = 500;
        var b = new StringArray.Builder();
        var nb = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        {
            b.Append(states[i % states.Length]);
            if (i % 7 == 0) nb.AppendNull();
            else nb.Append(states[(i + 1) % states.Length]);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build(), nb.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenSlicedDictFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Build a 400-row repetitive nullable string column then slice off
        // the first 47 rows (odd offset → bit-level validity copy). Both the
        // values and the validity of the remaining 353 rows must round-trip
        // correctly through the Rust reader.
        var palette = new[] { "open", "closed", "pending", "error", "stalled" };
        const int sourceLen = 400;
        var b = new StringArray.Builder();
        for (int i = 0; i < sourceLen; i++)
        {
            if (i % 9 == 0) b.AppendNull();
            else b.Append(palette[i % palette.Length]);
        }
        var sliced = (StringArray)((StringArray)b.Build()).Slice(47, sourceLen - 47);

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: true),
        }, metadata: null);
        var batch = new RecordBatch(schema, new IArrowArray[] { sliced }, sliced.Length);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true);

            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            Assert.Contains($"OK rows={sliced.Length}", stdout);
            Assert.Contains($"DONE total={sliced.Length}", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenRunEndFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // 6 distinct Int32 values × 200-row runs → vortex.runend dispatches
        // (no nulls, no slicing — matches writer's current scope).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        const int runLen = 200;
        var palette = new[] { 1, 2, 3, 4, 5, 6 };
        const int n = runLen * 6;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++) b.Append(palette[i / runLen]);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenFsstFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // 500-row column of URL-shaped strings — dict rejects (all distinct),
        // FSST trains a symbol table on the shared scheme/host/path prefix
        // and packs each row into a few bytes plus a per-row id suffix.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("u", StringType.Default, nullable: false),
        }, metadata: null);
        const int n = 500;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++)
            b.Append($"https://www.example.com/path/to/resource/{i:D6}?query=value&session=abc123def456");
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenSparseFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // 5 000-row Int32 column where most rows are 0 with sprinkles of
        // non-zero values. Tests the full sparse round-trip through vortex's
        // Rust reader: fill scalar buffer, indices child, values child,
        // PatchesMetadata wrapper.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 5_000;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++) b.Append(i % 50 == 0 ? i + 1 : 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenNullableRunEndFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Mix of value runs and null runs. Leading + trailing null runs
        // exercise the boundary handling at i=0 (seed) and i=n-1 (final
        // close).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: true),
        }, metadata: null);
        const int n = 1_500;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            // Pattern: [null × 100][1 × 200][null × 50][2 × 400][null × 100][3 × 650]
            int phase =
                i < 100 ? 0 :
                i < 300 ? 1 :
                i < 350 ? 0 :
                i < 750 ? 2 :
                i < 850 ? 0 :
                3;
            if (phase == 0) b.AppendNull();
            else b.Append(phase);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenNullableSparseFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Nullable Int32 sparse: mode = 0, scattered non-zero patches and
        // null patches. The validity of patch_values rides as a vortex.bool
        // child of the patch_values primitive node.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: true),
        }, metadata: null);
        const int n = 5_000;
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 100 == 0) b.AppendNull();
            else if (i % 50 == 0) b.Append(i);
            else b.Append(0);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenNullableFsstFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Nullable URL-shaped FSST column. Validity bitmap rides as a
        // vortex.bool node at children[2], after uncompressed_lengths and
        // codes_offsets.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("u", StringType.Default, nullable: true),
        }, metadata: null);
        const int n = 600;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 13 == 0) b.AppendNull();
            else b.Append($"https://www.example.com/path/to/resource/{i:D6}?token=abc123");
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenZonedStatsFloatFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Float column with Phase C's full stat set: max, max_is_truncated,
        // min, min_is_truncated, sum, null_count, nan_count. Cross-val is
        // the strongest signal that we got the wire format right — the
        // Rust reader parses the zones table struct and validates each
        // field's dtype against the present_stats bitset.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: true),
        }, metadata: null);
        var sizes = new[] { 200, 200, 200, 100 };

        DoubleArray BuildBatch(int batchIdx, int n)
        {
            var b = new DoubleArray.Builder();
            for (int i = 0; i < n; i++)
            {
                if (i % 13 == 0) b.AppendNull();
                else if (i == 7 && batchIdx == 2) b.Append(double.NaN);
                else b.Append(batchIdx * 100.0 + i * 0.5);
            }
            return b.Build();
        }

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                for (int batchIdx = 0; batchIdx < sizes.Length; batchIdx++)
                    w.WriteBatch(new RecordBatch(schema,
                        new IArrowArray[] { BuildBatch(batchIdx, sizes[batchIdx]) },
                        sizes[batchIdx]));
                w.Close();
            }

            int total = sizes.Sum();
            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            Assert.Contains($"OK rows={total}", stdout);
            Assert.Contains($"DONE total={total}", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenZonedStatsFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Multi-batch file with preserveStats: each column wrapped in
        // vortex.stats(data, zones) carrying per-zone null_count. The
        // Rust reader skips zones it doesn't need but validates the
        // overall layout shape.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int32Type.Default, nullable: true),
        }, metadata: null);
        var sizes = new[] { 200, 200, 200, 100 };

        Int32Array BuildBatch(int startRow, int n)
        {
            var b = new Int32Array.Builder();
            for (int i = 0; i < n; i++)
            {
                if ((startRow + i) % 7 == 0) b.AppendNull();
                else b.Append(startRow + i);
            }
            return b.Build();
        }

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var w = new VortexFileWriter(fs, schema, preserveStats: true))
            {
                int rows = 0;
                foreach (var sz in sizes)
                {
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { BuildBatch(rows, sz) }, sz));
                    rows += sz;
                }
                w.Close();
            }

            int total = sizes.Sum();
            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            Assert.Contains($"OK rows={total}", stdout);
            Assert.Contains($"DONE total={total}", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenVarBinViewFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Mixed inline (≤ 12 bytes) and referenced (> 12 bytes) strings,
        // with nulls. Tests the views buffer + data buffer split + validity
        // child end-to-end through vortex's Rust reader.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", StringType.Default, nullable: true),
        }, metadata: null);
        const int n = 250;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 17 == 0) b.AppendNull();
            else if (i % 2 == 0) b.Append($"row-{i}");
            else b.Append($"longer-than-twelve-bytes-row-{i:D6}-tail");
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, preferVarBinView: true);

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
    public void RustReader_OpensDotNetWrittenAlpRdFloatFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // f32 ALP-RD: bounded magnitudes around three pivots. ALP rejects,
        // ALP-RD applies with right_parts as u32.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", FloatType.Default, nullable: false),
        }, metadata: null);
        const int n = 4_096;
        var b = new FloatArray.Builder();
        var rng = new Random(2026);
        for (int i = 0; i < n; i++)
        {
            float pivot = (i % 3) switch { 0 => 1.5f, 1 => 12.5f, _ => 100.5f };
            b.Append(pivot + (float)(rng.NextDouble() * 0.4));
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenAlpRdFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Doubles where ALP can't find a profitable (e, f) — bounded
        // magnitudes around three pivots. ALP rejects, ALP-RD applies.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int n = 4_096;
        var b = new DoubleArray.Builder();
        var rng = new Random(2026);
        for (int i = 0; i < n; i++)
        {
            double pivot = (i % 3) switch { 0 => 1.5, 1 => 12.5, _ => 100.5 };
            b.Append(pivot + rng.NextDouble() * 0.4);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenAlpFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Decimal-like f64 column → vortex.alp dispatches. Mix in a couple of
        // non-decimal values (PI, NaN) to exercise the patches path.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("price", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int n = 1_000;
        var b = new DoubleArray.Builder();
        for (int i = 0; i < n; i++)
        {
            switch (i)
            {
                case 100: b.Append(Math.PI); break;
                case 500: b.Append(double.NaN); break;
                default: b.Append(1.5 + (i % 100) * 0.01); break;
            }
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenPcoDoubleFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Doubles compressed via vortex.pco. Pco is opt-in via preferPco=true,
        // so only triggers when the user explicitly requests it. Bounded
        // magnitudes ensure pco's mode-search picks a profitable mode.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: false),
        }, metadata: null);
        const int n = 4_096;
        var b = new DoubleArray.Builder();
        var rng = new Random(2026);
        for (int i = 0; i < n; i++) b.Append(100.0 + rng.NextDouble() * 50.0);
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true, preferPco: true);

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
    public void RustReader_OpensDotNetWrittenPcoNullableInt64File()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Nullable i64 — vortex.pco with a validity child. Exercises the
        // dense-buffer compaction path on the writer side and the
        // sparse-splice path on the reader side.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", Int64Type.Default, nullable: true),
        }, metadata: null);
        const int n = 2_048;
        var b = new Int64Array.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 13 == 0) b.AppendNull();
            else b.Append((long)i * 1_000_000L - 500L);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true, preferPco: true);

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
    public void RustReader_OpensDotNetWrittenTimestampPrimitiveFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Default Timestamp path: vortex.timestamp Extension wrapping
        // vortex.ext { vortex.primitive } i64 storage. Validates that the
        // Extension dtype + array wrapping round-trip through vortex 0.70.
        var type = new TimestampType(TimeUnit.Microsecond, (string?)"UTC");
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("ts", type, nullable: false),
        }, metadata: null);
        const int n = 100;
        long baseUs = 1_704_067_200L * 1_000_000L;
        var ticks = new long?[n];
        for (int i = 0; i < n; i++) ticks[i] = baseUs + (long)i * 60_000_000L;
        var batch = new RecordBatch(schema, new IArrowArray[]
        {
            BuildTimestampArrayForCrossValidation(type, ticks),
        }, n);

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
    public void RustReader_OpensDotNetWrittenDateTimePartsFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // preferDateTimeParts: vortex.timestamp Extension wrapping vortex.ext
        // { vortex.datetimeparts(days, seconds, subseconds) }. Each part is
        // recursively encoded with compress=true so children land on
        // bitpacked / FoR.
        var type = new TimestampType(TimeUnit.Microsecond, (string?)null);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("ts", type, nullable: false),
        }, metadata: null);
        const int n = 4096;
        long baseUs = 1_704_067_200L * 1_000_000L;
        var rng = new Random(2026);
        var ticks = new long?[n];
        for (int i = 0; i < n; i++)
            ticks[i] = baseUs + (long)i * 600_000_000L + rng.Next(0, 1_000_000);
        var batch = new RecordBatch(schema, new IArrowArray[]
        {
            BuildTimestampArrayForCrossValidation(type, ticks),
        }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, compress: true, preferDateTimeParts: true);

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

    /// <summary>
    /// Helper: builds a TimestampArray directly from raw i64 ticks (Apache.Arrow's
    /// TimestampArray.Builder takes DateTimeOffset and would lossily convert ns
    /// inputs).
    /// </summary>
    private static TimestampArray BuildTimestampArrayForCrossValidation(
        TimestampType type, long?[] ticksOrNull)
    {
        int n = ticksOrNull.Length;
        var bytes = new byte[(long)n * 8];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
        var validity = new byte[(n + 7) / 8];
        int nullCount = 0;
        for (int i = 0; i < n; i++)
        {
            if (ticksOrNull[i] is null) { nullCount++; continue; }
            span[i] = ticksOrNull[i]!.Value;
            validity[i >> 3] |= (byte)(1 << (i & 7));
        }
        return new TimestampArray(
            type, new ArrowBuffer(bytes),
            nullCount > 0 ? new ArrowBuffer(validity) : ArrowBuffer.Empty,
            n, nullCount, 0);
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenDate32File()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("d", Date32Type.Default, nullable: false),
        }, metadata: null);
        const int n = 100;
        var bytes = new byte[(long)n * 4];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(bytes.AsSpan());
        for (int i = 0; i < n; i++) span[i] = 19_723 + i;
        var arr = new Date32Array(new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

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
    public void RustReader_OpensDotNetWrittenDate64File()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("d", Date64Type.Default, nullable: true),
        }, metadata: null);
        const int n = 80;
        var bytes = new byte[(long)n * 8];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
        var validity = new byte[(n + 7) / 8];
        int nullCount = 0;
        long baseMs = 1_704_067_200L * 1_000L;
        for (int i = 0; i < n; i++)
        {
            if (i % 9 == 0) { nullCount++; continue; }
            span[i] = baseMs + (long)i * 86_400_000L;
            validity[i >> 3] |= (byte)(1 << (i & 7));
        }
        var arr = new Date64Array(new ArrowBuffer(bytes), new ArrowBuffer(validity), n, nullCount, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

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
    public void RustReader_OpensDotNetWrittenTime32File()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var type = new Time32Type(TimeUnit.Millisecond);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("t", type, nullable: false),
        }, metadata: null);
        const int n = 60;
        var bytes = new byte[(long)n * 4];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(bytes.AsSpan());
        for (int i = 0; i < n; i++) span[i] = i * 60_000;
        var arr = new Time32Array(type, new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

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
    public void RustReader_OpensDotNetWrittenTime64File()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var type = new Time64Type(TimeUnit.Nanosecond);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("t", type, nullable: false),
        }, metadata: null);
        const int n = 80;
        var bytes = new byte[(long)n * 8];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, long>(bytes.AsSpan());
        for (int i = 0; i < n; i++) span[i] = (long)i * 1_000_000_000L;
        var arr = new Time64Array(type, new ArrowBuffer(bytes), ArrowBuffer.Empty, n, 0, 0);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

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
    public void RustReader_OpensDotNetWrittenUuidFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        var type = new FixedSizeBinaryType(16);
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("u", type, nullable: false),
        }, metadata: null);
        const int n = 50;
        var bytes = new byte[(long)n * 16];
        for (int i = 0; i < n; i++)
            for (int k = 0; k < 16; k++)
                bytes[i * 16 + k] = (byte)(i + k);
        var arrData = new ArrayData(
            type, n, 0, 0,
            new[] { ArrowBuffer.Empty, new ArrowBuffer(bytes) });
        var arr = new Apache.Arrow.Arrays.FixedSizeBinaryArray(arrData);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

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

#if NET6_0_OR_GREATER
    [Fact]
    public void RustReader_OpensDotNetWrittenHalfFloatFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // F16 column: 2 bytes/row, no Extension wrap (just plain
        // vortex.primitive). Apache.Arrow's HalfFloatArray needs System.Half
        // (net6+); we construct it directly via Half values. Reader-side
        // decode is still rejected on netstandard2.0, but Rust's validator
        // handles F16 natively, so this exercises the writer end-to-end.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("h", HalfFloatType.Default, nullable: false),
        }, metadata: null);
        const int n = 50;
        var bytes = new byte[(long)n * 2];
        var span = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, Half>(bytes.AsSpan());
        for (int i = 0; i < n; i++) span[i] = (Half)(1.5f + i * 0.25f);
        var arrData = new ArrayData(
            HalfFloatType.Default, n, 0, 0,
            new[] { ArrowBuffer.Empty, new ArrowBuffer(bytes) });
        var arr = new HalfFloatArray(arrData);
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, n);

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
#endif

    [Fact]
    public void RustReader_OpensDotNetWrittenNullableAlpRdFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Nullable f64 ALP-RD column. Validates that the writer's null
        // handling on left_parts (validity bitmap rebased to offset 0) and
        // patch-skip-at-null behaviour produce a wire shape vortex 0.70
        // accepts.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", DoubleType.Default, nullable: true),
        }, metadata: null);
        const int n = 4096;
        var rng = new Random(2026);
        var b = new DoubleArray.Builder();
        for (int i = 0; i < n; i++)
        {
            if (i % 11 == 0) b.AppendNull();
            else
            {
                double pivot = (i % 3) switch { 0 => 1.5, 1 => 12.5, _ => 100.5 };
                b.Append(pivot + rng.NextDouble() * 0.4);
            }
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenFsstBinaryFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Repetitive binary column → FSST. Validates the symbol-table +
        // codes wire shape works for BinaryType the same way it does for
        // StringType (vortex 0.70 dispatches both through the same
        // vortex.fsst encoding).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", BinaryType.Default, nullable: false),
        }, metadata: null);
        const int n = 200;
        var prefix = new byte[] { 0xCA, 0xFE, 0xBA, 0xBE, 0xDE, 0xAD, 0xBE, 0xEF };
        var b = new BinaryArray.Builder();
        for (int i = 0; i < n; i++)
        {
            var bytes = new byte[12];
            Buffer.BlockCopy(prefix, 0, bytes, 0, 8);
            bytes[8] = (byte)(i & 0xFF);
            bytes[9] = (byte)((i >> 8) & 0xFF);
            bytes[10] = 0x55;
            bytes[11] = 0xAA;
            b.Append((ReadOnlySpan<byte>)bytes);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

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
    public void RustReader_OpensDotNetWrittenVarBinViewBinaryFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // BinaryArray under preferVarBinView. Mix of inline + referenced
        // payloads exercises both view-format branches.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", BinaryType.Default, nullable: false),
        }, metadata: null);
        const int n = 100;
        var b = new BinaryArray.Builder();
        for (int i = 0; i < n; i++)
        {
            var bytes = (i % 2 == 0)
                ? new byte[] { (byte)i, 0xAA, 0xBB }
                : new byte[20];
            if (i % 2 == 1)
                for (int k = 0; k < 20; k++) bytes[k] = (byte)((i + k) & 0xFF);
            b.Append((ReadOnlySpan<byte>)bytes);
        }
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, preferVarBinView: true);

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
    public void RustReader_OpensDotNetWrittenDictLayoutFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // Multi-batch low-cardinality string column under preferDictLayout:
        // the file uses a vortex.dict layout sharing one global values
        // segment across all batches. Validates the layout-level dict wire
        // shape (children = [flat-values, chunked-of-flat-codes], metadata
        // = { codes_ptype, is_nullable_codes }).
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("color", StringType.Default, nullable: false),
        }, metadata: null);
        var palette = new[] { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" };
        const int rowsPerBatch = 100;
        const int batchCount = 4;

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            {
                using var w = new VortexFileWriter(fs, schema, preferDictLayout: true);
                var rng = new Random(123);
                for (int batch = 0; batch < batchCount; batch++)
                {
                    var b = new StringArray.Builder();
                    for (int i = 0; i < rowsPerBatch; i++)
                        b.Append(palette[rng.Next(palette.Length)]);
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, rowsPerBatch));
                }
                w.Close();
            }

            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            int total = rowsPerBatch * batchCount;
            Assert.Contains($"OK rows={total}", stdout);
            Assert.Contains($"DONE total={total}", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenDictLayoutWithStatsFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // preferDictLayout AND preserveStats: each column's data layout is
        // wrapped in vortex.stats(vortex.dict(values, codes-chunked), zones).
        // Confirms vortex 0.70 accepts the dict-inside-stats layout shape.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("color", StringType.Default, nullable: false),
        }, metadata: null);
        var palette = new[] { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" };
        const int rowsPerBatch = 100;
        const int batchCount = 3;

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            {
                using var w = new VortexFileWriter(fs, schema,
                    preserveStats: true, preferDictLayout: true);
                var rng = new Random(456);
                for (int batch = 0; batch < batchCount; batch++)
                {
                    var b = new StringArray.Builder();
                    for (int i = 0; i < rowsPerBatch; i++)
                        b.Append(palette[rng.Next(palette.Length)]);
                    w.WriteBatch(new RecordBatch(schema, new IArrowArray[] { b.Build() }, rowsPerBatch));
                }
                w.Close();
            }

            var (code, stdout, stderr) = RunValidator(validator, path);
            Assert.True(code == 0,
                $"Rust validator failed (exit {code}). stderr:\n{stderr}\nstdout:\n{stdout}");
            int total = rowsPerBatch * batchCount;
            Assert.Contains($"OK rows={total}", stdout);
            Assert.Contains($"DONE total={total}", stdout);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void RustReader_OpensDotNetWrittenStringStatsFile()
    {
        var validator = FindValidator();
        if (validator is null) return;

        // String column with preserveStats=true exercises our StringFull
        // zone-stats scheme: per-zone min/max emitted as nullable Utf8
        // children of the zones-table struct. Validates that the bitset
        // (0xD8) and child layout (max + max_is_truncated, min +
        // min_is_truncated, null_count, uncompressed_size) match what
        // vortex 0.70's reader expects.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("v", StringType.Default, nullable: false),
        }, metadata: null);
        const int n = 100;
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++) b.Append($"row-{i:D3}");
        var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, n);

        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
                VortexFileWriter.Write(fs, batch, preserveStats: true);

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
