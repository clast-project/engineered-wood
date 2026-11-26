// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using EngineeredWood.Avro;
using EngineeredWood.Iceberg.Serialization;

namespace EngineeredWood.Iceberg.Tests.Corpus;

/// <summary>
/// The committed corpora in <c>TestData/corpus</c>: tables written by Spark 4.1 + Iceberg 1.12
/// (<c>Interop/spark/generate_corpus.py</c>) and by iceberg-go (<c>Interop/iceberg-go/generate_corpus.py</c>),
/// each with what its writer reads back in <c>expected/&lt;table&gt;.json</c>.
/// </summary>
/// <remarks>
/// The tables were written under a neutral warehouse, recorded as <c>warehouse_uri</c> in <c>corpus.json</c>,
/// and every location inside them is absolute under it. <see cref="LocalPath"/> maps such a location onto the
/// committed copy.
/// </remarks>
internal sealed class IcebergCorpus
{
    public static readonly IcebergCorpus Spark = new("spark");
    public static readonly IcebergCorpus IcebergGo = new("iceberg-go");

    private readonly string _root;
    private readonly string _warehouseUri;

    private IcebergCorpus(string engine)
    {
        Engine = engine;
        _root = Path.Combine(SourceCorpusDirectory(), engine);
        Info = JsonDocument.Parse(File.ReadAllText(Path.Combine(_root, "corpus.json"))).RootElement;
        _warehouseUri = Info.GetProperty("warehouse_uri").GetString()!.TrimEnd('/');
        Tables = Info.GetProperty("tables").EnumerateArray().Select(t => t.GetString()!).ToArray();

        // The neutral warehouse the tables were written under is the committed copy here: one registration
        // relocates every absolute location the metadata holds.
        Resolver = new IO.StorageResolver().Register(
            IO.StorageLocation.Parse(_warehouseUri), new IO.Local.LocalTableFileSystem(WarehouseDirectory));
    }

    /// <summary>Reaches every location the corpus's metadata names, in the committed copy.</summary>
    public IO.StorageResolver Resolver { get; }

    /// <summary>The corpus as the absolute-location file system EW's manifest code reads through.</summary>
    internal IO.ITableFileSystem FileSystem => new LocationFileSystem(Resolver);

    public string Engine { get; }

    /// <summary>The committed copy of the warehouse the tables were written under.</summary>
    public string WarehouseDirectory => Path.Combine(_root, "wh");

    /// <summary>
    /// The corpus in the source tree, found by walking up from the test output. It is not copied to the output:
    /// its deepest paths, under bin/&lt;configuration&gt;/&lt;tfm&gt;/, would pass Windows' 260-character limit.
    /// </summary>
    private static string SourceCorpusDirectory()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            string candidate = Path.Combine(dir.FullName, "TestData", "corpus");
            if (Directory.Exists(candidate) && File.Exists(Path.Combine(dir.FullName, "EngineeredWood.Iceberg.Tests.csproj")))
                return candidate;
        }

        throw new DirectoryNotFoundException("test/EngineeredWood.Iceberg.Tests/TestData/corpus not found above " + AppContext.BaseDirectory);
    }

    public JsonElement Info { get; }

    public IReadOnlyList<string> Tables { get; }

    public static TheoryData<string> SparkTables() => Data(Spark);

    public static TheoryData<string> IcebergGoTables() => Data(IcebergGo);

    private static TheoryData<string> Data(IcebergCorpus corpus)
    {
        var data = new TheoryData<string>();
        foreach (var t in corpus.Tables)
            data.Add(t);
        return data;
    }

    public JsonElement Expected(string table) =>
        JsonDocument.Parse(File.ReadAllText(Path.Combine(_root, "expected", table + ".json"))).RootElement;

    /// <summary>The committed copy of a file the table's metadata names.</summary>
    public string LocalPath(string location)
    {
        // Resolved, not string-matched: iceberg-go writes file:///C:/..., Spark file:/tmp/.... Nothing is decoded:
        // Spark's partition directories hold literal escapes (fx=fwAAAQ%3D%3D) on disk and in the manifests alike.
        string relative = Resolver.Resolve(location).Path;
        return Path.Combine(WarehouseDirectory, relative.Replace('/', Path.DirectorySeparatorChar));
    }

    /// <summary>The table's current metadata file, as its writer last committed it.</summary>
    public string MetadataLocation(string table)
    {
        var expected = Expected(table);
        return Engine == "spark"
            ? expected.GetProperty("metadata_location").GetString()!
            : expected.GetProperty("scenario").GetProperty("metadata_location").GetString()!;
    }

    public string MetadataText(string table) => File.ReadAllText(LocalPath(MetadataLocation(table)));

    public TableMetadata Metadata(string table) => IcebergJsonSerializer.Deserialize<TableMetadata>(MetadataText(table));

    /// <summary>
    /// The data and delete files of a snapshot with their raw bound bytes, read straight from the manifests'
    /// Avro: the files the manifest codec (#586) decodes, without depending on it.
    /// </summary>
    public IReadOnlyList<ManifestFileBounds> ReadFileBounds(Snapshot snapshot)
    {
        var files = new List<ManifestFileBounds>();
        foreach (string manifest in ReadColumn(snapshot.ManifestList, "manifest_path"))
        {
            foreach (var batch in Read(manifest))
            {
                var status = (Int32Array)batch.Column("status");
                var dataFile = (StructArray)batch.Column("data_file");
                var type = (Apache.Arrow.Types.StructType)dataFile.Data.DataType;
                var path = (StringArray)dataFile.Fields[type.GetFieldIndex("file_path")];
                var content = (Int32Array)dataFile.Fields[type.GetFieldIndex("content")];
                var lower = dataFile.Fields[type.GetFieldIndex("lower_bounds")];
                var upper = dataFile.Fields[type.GetFieldIndex("upper_bounds")];
                for (int i = 0; i < batch.Length; i++)
                {
                    if (status.GetValue(i) == 2) // DELETED
                        continue;
                    files.Add(new ManifestFileBounds(
                        path.GetString(i), content.GetValue(i) ?? 0, BoundMap(lower, i), BoundMap(upper, i)));
                }
            }
        }

        return files;
    }

    private IEnumerable<string> ReadColumn(string avroLocation, string column)
    {
        foreach (var batch in Read(avroLocation))
        {
            var values = (StringArray)batch.Column(column);
            for (int i = 0; i < batch.Length; i++)
                yield return values.GetString(i);
        }
    }

    private List<RecordBatch> Read(string avroLocation)
    {
        using var stream = File.OpenRead(LocalPath(avroLocation));
        using var reader = new AvroReaderBuilder().Build(stream);
        return reader.ToList();
    }

    private static Dictionary<int, byte[]> BoundMap(IArrowArray array, int row)
    {
        var map = new Dictionary<int, byte[]>();
        var list = (ListArray)array;
        if (list.IsNull(row))
            return map;
        var entries = (StructArray)list.Values;
        var keys = (Int32Array)entries.Fields[0];
        var values = (BinaryArray)entries.Fields[1];
        for (int k = list.ValueOffsets[row]; k < list.ValueOffsets[row + 1]; k++)
            map[keys.GetValue(k)!.Value] = values.GetBytes(k).ToArray();
        return map;
    }
}

/// <summary>A live file of a snapshot, with the bounds its manifest entry carries.</summary>
internal sealed record ManifestFileBounds(
    string FilePath, int Content, IReadOnlyDictionary<int, byte[]> Lower, IReadOnlyDictionary<int, byte[]> Upper);
