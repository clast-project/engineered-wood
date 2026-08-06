// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Reflection;
using System.Text;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Snapshot;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// <see cref="DeltaFormatException.ErrorCode"/> — that the conditions a caller must tell apart really
/// are distinguishable without reading the message.
/// </summary>
public class DeltaErrorCodeTests : IDisposable
{
    private readonly string _tempDir;

    public DeltaErrorCodeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_errcode_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static ProtocolAction Protocol(int reader, int writer) =>
        new() { MinReaderVersion = reader, MinWriterVersion = writer };

    // ── The four conditions the migrating consumer named ──

    [Fact]
    public async Task EmptyDirectory_IsNotFound_AndCatchableAsItsOwnType()
    {
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));
        Directory.CreateDirectory(Path.Combine(_tempDir, "_delta_log"));

        // The whole point of the separate type: this is catchable WITHOUT knowing any code.
        var ex = await Assert.ThrowsAsync<DeltaTableNotFoundException>(
            async () => await SnapshotBuilder.BuildAsync(log));

        Assert.Equal(DeltaErrorCodes.PathDoesNotExist, ex.ErrorCode);

        // And it still reaches an existing handler written before the type existed.
        Assert.IsAssignableFrom<DeltaFormatException>(ex);
    }

    [Fact]
    public void UnsupportedReaderFeature_IsACapabilityAnswer_NotCorruption()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            ReaderFeatures = ["someFutureFeature"],
        };

        var ex = Assert.Throws<DeltaFormatException>(
            () => ProtocolVersions.ValidateReadSupport(protocol));

        Assert.Equal(DeltaErrorCodes.UnsupportedFeaturesForRead, ex.ErrorCode);
        Assert.IsNotType<DeltaTableNotFoundException>(ex);
    }

    [Fact]
    public void UnsupportedWriterFeature_IsDistinctFromTheReaderCase()
    {
        var protocol = new ProtocolAction
        {
            MinReaderVersion = 3,
            MinWriterVersion = 7,
            WriterFeatures = ["someFutureWriteFeature"],
        };

        var ex = Assert.Throws<DeltaFormatException>(
            () => ProtocolVersions.ValidateWriteSupport(protocol));

        Assert.Equal(DeltaErrorCodes.UnsupportedFeaturesForWrite, ex.ErrorCode);
    }

    /// <summary>
    /// A protocol VERSION above ours and a named FEATURE we lack are different codes, because a caller
    /// can act on the second (drop the feature) and not the first (upgrade the library).
    /// </summary>
    [Theory]
    [InlineData(99, 7)]
    [InlineData(3, 99)]
    public void ProtocolVersionTooHigh_IsItsOwnCode(int reader, int writer)
    {
        var ex = Assert.Throws<DeltaFormatException>(
            () => ProtocolVersions.ValidateWriteSupport(Protocol(reader, writer)));

        Assert.Equal(DeltaErrorCodes.InvalidProtocolVersion, ex.ErrorCode);
    }

    [Fact]
    public async Task IncompleteLog_IsTruncatedLog_NotNotFound()
    {
        var log = new TransactionLog(new LocalTableFileSystem(_tempDir));

        await log.WriteCommitAsync(0, new List<DeltaAction>
        {
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "truncated",
                Format = Format.Parquet,
                SchemaString = """{"type":"struct","fields":[]}""",
                PartitionColumns = [],
            },
        });
        await log.WriteCommitAsync(1, [new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }]);

        // Delete the base of the log: version 1 survives but nothing can reconstruct it.
        File.Delete(Path.Combine(_tempDir, "_delta_log", $"{0:D20}.json"));

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await SnapshotBuilder.BuildAsync(log));

        Assert.Equal(DeltaErrorCodes.TruncatedTransactionLog, ex.ErrorCode);

        // The distinction that matters: a truncated log is NOT "no table here".
        Assert.IsNotType<DeltaTableNotFoundException>(ex);
    }

    // ── Decode failures ──

    [Fact]
    public void MalformedCommitJson_IsInvalidLogJson()
    {
        var ex = Assert.Throws<DeltaFormatException>(
            () => ActionSerializer.Deserialize(Encoding.UTF8.GetBytes("[1,2,3]\n")));

        Assert.Equal(DeltaErrorCodes.InvalidLogJson, ex.ErrorCode);
    }

    [Fact]
    public void ActionMissingARequiredField_IsMissingRequiredField()
    {
        // A well-formed add action with no `path`.
        var ex = Assert.Throws<DeltaFormatException>(
            () => ActionSerializer.Deserialize(
                Encoding.UTF8.GetBytes("""{"add":{"size":1,"modificationTime":1,"dataChange":true}}""" + "\n")));

        Assert.Equal(DeltaErrorCodes.MissingRequiredField, ex.ErrorCode);

        // The field name stays in the message; the code says only which KIND of failure it is.
        Assert.Contains("add.path", ex.Message);
    }

    /// <summary>
    /// Every way a deletion vector's bytes can be unreadable shares one code — a caller cannot act
    /// differently on a bad magic number than on a truncated buffer, and the stack frame says which.
    /// </summary>
    [Theory]
    [InlineData(new byte[] { 1, 2 })]                          // too short for the magic
    [InlineData(new byte[] { 9, 9, 9, 9, 0, 0, 0, 0 })]        // wrong magic
    public void UnreadableDeletionVectorBytes_ShareOneCode(byte[] data)
    {
        var ex = Assert.Throws<DeltaFormatException>(() => RoaringBitmapReader.Deserialize(data));
        Assert.Equal(DeltaErrorCodes.InvalidDeletionVector, ex.ErrorCode);
    }

    /// <summary>
    /// A storage type we do not implement is a CAPABILITY answer, not corruption — the bytes may be
    /// perfectly good — so it does not share the code above.
    /// </summary>
    [Fact]
    public async Task UnknownDeletionVectorStorageType_IsACapabilityCode()
    {
        var reader = new DeletionVectorReader(new LocalTableFileSystem(_tempDir));
        var dv = new DeletionVector
        {
            StorageType = "zz",
            PathOrInlineDv = "whatever",
            SizeInBytes = 1,
            Cardinality = 1,
        };

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await reader.ReadAsync(dv));

        Assert.Equal(DeltaErrorCodes.UnsupportedDeletionVectorStorageType, ex.ErrorCode);
        Assert.NotEqual(DeltaErrorCodes.InvalidDeletionVector, ex.ErrorCode);
    }

    // ── Guards on the code set itself ──

    private static List<(string Name, string Value)> DeclaredCodes() =>
        typeof(DeltaErrorCodes)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (f.Name, (string)f.GetRawConstantValue()!))
            .ToList();

    [Fact]
    public void EveryCodeIsUniqueAndWellFormed()
    {
        var codes = DeclaredCodes();
        Assert.NotEmpty(codes);

        var duplicates = codes.GroupBy(c => c.Value)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();
        Assert.Empty(duplicates);

        // DELTA_SCREAMING_SNAKE, matching delta-spark's error-class style — several of ours ARE
        // delta-spark names verbatim, and a stray lowercase one would not be recognisable as either.
        foreach (var (name, value) in codes)
        {
            Assert.True(
                System.Text.RegularExpressions.Regex.IsMatch(value, "^DELTA_[A-Z0-9_]+$"),
                $"{name} = \"{value}\" is not in DELTA_SCREAMING_SNAKE form.");
        }
    }

    /// <summary>
    /// No throw site in the log layer may omit a code. Without this, a new `throw new
    /// DeltaFormatException(message)` compiles, ships, and silently returns a consumer to matching on
    /// message text — the exact situation this work removed.
    /// </summary>
    [Fact]
    public void NoLogLayerThrowSiteOmitsItsCode()
    {
        string? root = FindRepoRoot();
        if (root is null)
            return; // sources are not laid out beside the binaries in this run

        string sourceDir = Path.Combine(root, "src", "EngineeredWood.DeltaLake");
        Assert.True(Directory.Exists(sourceDir), $"expected sources at {sourceDir}");

        var offenders = new List<string>();
        foreach (string file in Directory.EnumerateFiles(sourceDir, "*.cs", SearchOption.AllDirectories))
        {
            // The exception's own definition legitimately mentions the constructors.
            if (Path.GetFileName(file) is "DeltaFormatException.cs") continue;
            if (file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}")) continue;

            string text = File.ReadAllText(file);
            const string marker = "new DeltaFormatException(";
            for (int i = text.IndexOf(marker, StringComparison.Ordinal); i >= 0;
                 i = text.IndexOf(marker, i + 1, StringComparison.Ordinal))
            {
                string window = text.Substring(i, Math.Min(140, text.Length - i));
                if (!window.Contains("DeltaErrorCodes.", StringComparison.Ordinal))
                {
                    int line = text.Take(i).Count(c => c == '\n') + 1;
                    offenders.Add($"{Path.GetFileName(file)}:{line}");
                }
            }
        }

        Assert.True(offenders.Count == 0,
            "these throw sites carry no DeltaErrorCodes value: " + string.Join(", ", offenders));
    }

    private static string? FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "engineered-wood.slnx")))
                return dir.FullName;
            dir = dir.Parent;
        }
        return null;
    }
}
