// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.IO;
using EngineeredWood.Lance.Table.Manifest;
using EngineeredWood.Lance.Table.Proto;
using Google.Protobuf;

namespace EngineeredWood.Lance.Table;

/// <summary>
/// Wraps a single <see cref="LanceFileWriter"/> in a minimal Lance
/// dataset directory layout — <c>data/&lt;uuid&gt;.lance</c> for the
/// columnar bytes, <c>_versions/&lt;encoded&gt;.manifest</c> for the
/// version-1 manifest, and <c>_transactions/0-&lt;uuid&gt;.txn</c> for
/// the originating transaction. The result opens cleanly via
/// <see cref="LanceTable.OpenAsync(string, ulong?, CancellationToken)"/>.
///
/// <para><b>Scope</b>: a single fresh version (no append/overwrite of an
/// existing dataset), one fragment, all columns in one data file. The
/// underlying <see cref="LanceFileWriter"/> dictates which Arrow types
/// can be written.</para>
///
/// <para>Typical usage:</para>
/// <code>
/// await using var ds = await LanceDatasetWriter.CreateAsync(path);
/// await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
/// await ds.FinishAsync();
/// </code>
/// </summary>
public sealed class LanceDatasetWriter : IAsyncDisposable
{
    // Manifest footer trailer: u16 major + u16 minor identifying the
    // manifest *file* format (not the data files). pylance/lance-rs
    // currently writes (0, 2). The on-disk reader doesn't validate
    // these, but matching upstream avoids surprises with other tools.
    private const ushort ManifestMajorVersion = 0;
    private const ushort ManifestMinorVersion = 2;

    private readonly string _datasetPath;
    private readonly string _dataFileName;
    private bool _finished;
    private bool _disposed;

    /// <summary>
    /// The underlying file writer. Use it to add columns; call
    /// <see cref="FinishAsync"/> on the dataset writer (NOT on this
    /// inner writer) when done — finishing the dataset closes the file
    /// and emits the manifest.
    /// </summary>
    public LanceFileWriter FileWriter { get; }

    private LanceDatasetWriter(string datasetPath, string dataFileName, LanceFileWriter fileWriter)
    {
        _datasetPath = datasetPath;
        _dataFileName = dataFileName;
        FileWriter = fileWriter;
    }

    /// <summary>
    /// Creates an empty Lance dataset at <paramref name="datasetPath"/>
    /// and returns a writer for adding columns. The directory is created
    /// if it doesn't exist; if it does, it MUST be empty (we don't
    /// support appending to or overwriting an existing dataset yet).
    /// </summary>
    public static async ValueTask<LanceDatasetWriter> CreateAsync(
        string datasetPath, CancellationToken cancellationToken = default)
    {
        if (datasetPath is null) throw new ArgumentNullException(nameof(datasetPath));

        // Refuse to clobber an existing dataset.
        if (Directory.Exists(datasetPath))
        {
            if (Directory.Exists(Path.Combine(datasetPath, "_versions"))
                || Directory.Exists(Path.Combine(datasetPath, "data")))
                throw new InvalidOperationException(
                    $"Path '{datasetPath}' already contains a Lance dataset. " +
                    "Overwriting an existing dataset is not yet supported.");
        }
        else
        {
            Directory.CreateDirectory(datasetPath);
        }

        Directory.CreateDirectory(Path.Combine(datasetPath, "data"));
        Directory.CreateDirectory(Path.Combine(datasetPath, "_versions"));
        Directory.CreateDirectory(Path.Combine(datasetPath, "_transactions"));

        string dataFileName = Guid.NewGuid().ToString("N") + ".lance";
        string dataFilePath = Path.Combine(datasetPath, "data", dataFileName);
        var fileWriter = await LanceFileWriter.CreateAsync(dataFilePath, cancellationToken)
            .ConfigureAwait(false);

        return new LanceDatasetWriter(datasetPath, dataFileName, fileWriter);
    }

    /// <summary>
    /// Finalises the dataset: closes the underlying data file, writes
    /// <c>_transactions/0-&lt;uuid&gt;.txn</c>, and writes
    /// <c>_versions/&lt;encoded&gt;.manifest</c> (version 1, encoded as
    /// <c>u64::MAX - 1</c>). Idempotent on success.
    /// </summary>
    public async Task FinishAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_finished) return;

        await FileWriter.FinishAsync(cancellationToken).ConfigureAwait(false);
        // Dispose the underlying file handle now so its bytes are
        // flushed before we read its size below.
        await FileWriter.DisposeAsync().ConfigureAwait(false);

        long dataFileSize = FileWriter.FinalSizeBytes;
        long totalRows = FileWriter.TotalRows;
        var fields = FileWriter.SchemaFields;
        if (fields.Count == 0)
            throw new InvalidOperationException(
                "Cannot finalise a Lance dataset with no columns.");

        string txnUuid = Guid.NewGuid().ToString();
        string txnFileName = $"0-{txnUuid}.txn";

        // --- Build & write the Transaction proto ---
        var transaction = new Transaction
        {
            ReadVersion = 0,
            Uuid = txnUuid,
        };
        byte[] transactionBytes = transaction.ToByteArray();
        await File.WriteAllBytesAsync(
            Path.Combine(_datasetPath, "_transactions", txnFileName),
            transactionBytes, cancellationToken).ConfigureAwait(false);

        // --- Build the Manifest proto ---
        // Each top-level Field gets a corresponding column index in the
        // data file (we always write one column per field, in order).
        var dataFile = new Proto.DataFile
        {
            Path = _dataFileName,
            FileMajorVersion = (uint)FileWriter.Version.Major,
            FileMinorVersion = (uint)FileWriter.Version.Minor,
            FileSizeBytes = (ulong)dataFileSize,
        };
        for (int i = 0; i < fields.Count; i++)
        {
            dataFile.Fields.Add(fields[i].Id);
            dataFile.ColumnIndices.Add(i);
        }

        var fragment = new DataFragment
        {
            Id = 0,
            PhysicalRows = (ulong)totalRows,
        };
        fragment.Files.Add(dataFile);

        var manifest = new Proto.Manifest
        {
            Version = 1,
            TransactionFile = txnFileName,
            DataFormat = new Proto.Manifest.Types.DataStorageFormat
            {
                FileFormat = "lance",
                Version = $"{FileWriter.Version.Major}.{FileWriter.Version.Minor}",
            },
        };
        foreach (var f in fields)
            manifest.Fields.Add(f);
        manifest.Fragments.Add(fragment);

        // --- Pack the manifest file body + footer ---
        // Layout (matches ManifestReader.Parse):
        //   [u32 LE: transaction_length]
        //   [Transaction bytes]
        //   [u32 LE: manifest_length]   <-- footer.MetadataPosition points here
        //   [Manifest bytes]
        //   [u64 LE: manifestLenPos]
        //   [u16 LE: major][u16 LE: minor]["LANC"]
        byte[] manifestBytes = manifest.ToByteArray();
        long bodySize = sizeof(uint) + transactionBytes.Length
                      + sizeof(uint) + manifestBytes.Length;
        long manifestLenPos = sizeof(uint) + transactionBytes.Length;
        long totalSize = bodySize + 16; // 16-byte trailer

        byte[] buf = new byte[checked((int)totalSize)];
        int cursor = 0;
        BinaryPrimitives.WriteUInt32LittleEndian(
            buf.AsSpan(cursor, sizeof(uint)), checked((uint)transactionBytes.Length));
        cursor += sizeof(uint);
        transactionBytes.CopyTo(buf, cursor);
        cursor += transactionBytes.Length;

        BinaryPrimitives.WriteUInt32LittleEndian(
            buf.AsSpan(cursor, sizeof(uint)), checked((uint)manifestBytes.Length));
        cursor += sizeof(uint);
        manifestBytes.CopyTo(buf, cursor);
        cursor += manifestBytes.Length;

        // 16-byte footer
        BinaryPrimitives.WriteUInt64LittleEndian(
            buf.AsSpan(cursor, sizeof(ulong)), (ulong)manifestLenPos);
        cursor += sizeof(ulong);
        BinaryPrimitives.WriteUInt16LittleEndian(
            buf.AsSpan(cursor, sizeof(ushort)), ManifestMajorVersion);
        cursor += sizeof(ushort);
        BinaryPrimitives.WriteUInt16LittleEndian(
            buf.AsSpan(cursor, sizeof(ushort)), ManifestMinorVersion);
        cursor += sizeof(ushort);
        // "LANC" magic (little-endian: 'L', 'A', 'N', 'C')
        buf[cursor++] = (byte)'L';
        buf[cursor++] = (byte)'A';
        buf[cursor++] = (byte)'N';
        buf[cursor++] = (byte)'C';

        // --- Write the manifest file with the v2 (u64::MAX - version) name ---
        const ulong version = 1;
        ulong encodedName = ulong.MaxValue - version;
        string manifestFileName = encodedName.ToString(System.Globalization.CultureInfo.InvariantCulture)
                                + ".manifest";
        await File.WriteAllBytesAsync(
            Path.Combine(_datasetPath, "_versions", manifestFileName),
            buf, cancellationToken).ConfigureAwait(false);

        _finished = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        // If the user never called FinishAsync, dispose the file writer
        // anyway so its handle releases. The dataset will be incomplete
        // (no manifest), but we don't try to clean up the partial data
        // file — that's a recovery decision the caller can make.
        await FileWriter.DisposeAsync().ConfigureAwait(false);
    }
}
