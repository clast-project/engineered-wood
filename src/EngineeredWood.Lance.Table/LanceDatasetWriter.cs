// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using EngineeredWood.IO;
using EngineeredWood.Lance.Format;
using EngineeredWood.Lance.Table.Manifest;
using EngineeredWood.Lance.Table.Proto;
using Google.Protobuf;
using LanceField = EngineeredWood.Lance.Proto.Field;
using LanceManifest = EngineeredWood.Lance.Table.Proto.Manifest;

namespace EngineeredWood.Lance.Table;

/// <summary>
/// Wraps one or more <see cref="LanceFileWriter"/> instances in a Lance
/// dataset directory layout — <c>data/&lt;uuid&gt;.lance</c> per fragment,
/// <c>_versions/&lt;encoded&gt;.manifest</c> for the version manifest, and
/// <c>_transactions/{readVersion}-&lt;uuid&gt;.txn</c> for the originating
/// transaction. Output opens cleanly via
/// <see cref="LanceTable.OpenAsync(string, ulong?, CancellationToken)"/>.
///
/// <para><b>Modes</b>:</para>
/// <list type="bullet">
///   <item><see cref="CreateAsync"/> — fresh dataset; refuses to clobber
///   an existing one.</item>
///   <item><see cref="AppendAsync"/> — opens an existing dataset, adds
///   new fragments alongside the existing ones, bumps the version. The
///   appended fragments must share the existing dataset's schema (same
///   field names, types, and ids).</item>
///   <item><see cref="OverwriteAsync"/> — replaces an existing dataset's
///   contents. The new manifest references only the new fragments; old
///   data files remain on disk for vacuum to clean up. Schema can change.</item>
/// </list>
///
/// <para><b>Multi-fragment</b>: call <see cref="NewFragmentAsync"/>
/// between batches of column writes to close the current data file and
/// start a new fragment in the same transaction. All fragments produced
/// in one session land in the same manifest version with monotonically
/// increasing fragment ids. Each fragment must have the same schema as
/// the first.</para>
///
/// <para>Typical usage:</para>
/// <code>
/// await using var ds = await LanceDatasetWriter.CreateAsync(path);
/// await ds.FileWriter.WriteInt32ColumnAsync("x", batch1);
/// await ds.NewFragmentAsync();
/// await ds.FileWriter.WriteInt32ColumnAsync("x", batch2);
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
    private readonly LanceWriteMode _mode;
    // For Append: the existing manifest (its fields are preserved, its
    // fragments are carried forward). For Create/Overwrite: null.
    private readonly LanceManifest? _baseManifest;
    // For Overwrite: the latest version we read (so the new manifest
    // bumps to readVersion+1 instead of always 1). 0 if no prior dataset.
    private readonly ulong _readVersion;

    private LanceFileWriter _currentFileWriter;
    private string _currentDataFileName;
    private readonly List<CompletedFragment> _completedFragments = new();
    // Canonical schema (taken from the first finished fragment in this
    // session). New fragments must match exactly.
    private IReadOnlyList<LanceField>? _sessionFields;
    private bool _finished;
    private bool _disposed;

    /// <summary>
    /// The underlying writer for the CURRENT fragment. Use it to add
    /// columns. Switching to a new fragment via <see cref="NewFragmentAsync"/>
    /// returns a fresh writer here. Call <see cref="FinishAsync"/> on the
    /// dataset writer (NOT on the inner writer) when done — finishing the
    /// dataset closes the active file and emits the manifest.
    /// </summary>
    public LanceFileWriter FileWriter => _currentFileWriter;

    private LanceDatasetWriter(
        string datasetPath, LanceWriteMode mode, LanceManifest? baseManifest,
        ulong readVersion, LanceFileWriter fileWriter, string dataFileName)
    {
        _datasetPath = datasetPath;
        _mode = mode;
        _baseManifest = baseManifest;
        _readVersion = readVersion;
        _currentFileWriter = fileWriter;
        _currentDataFileName = dataFileName;
    }

    /// <summary>
    /// Creates an empty Lance dataset at <paramref name="datasetPath"/>.
    /// Throws <see cref="InvalidOperationException"/> if the path already
    /// contains a Lance dataset. Use <see cref="AppendAsync"/> or
    /// <see cref="OverwriteAsync"/> for those cases.
    /// </summary>
    public static ValueTask<LanceDatasetWriter> CreateAsync(
        string datasetPath, CancellationToken cancellationToken = default)
        => OpenInternalAsync(datasetPath, LanceWriteMode.Create, cancellationToken);

    /// <summary>
    /// Opens an existing Lance dataset for appending new fragments. The
    /// new fragments' schema must match the existing dataset's schema
    /// (same field names, types, and ids). Throws if no manifest exists
    /// at <paramref name="datasetPath"/>.
    /// </summary>
    public static ValueTask<LanceDatasetWriter> AppendAsync(
        string datasetPath, CancellationToken cancellationToken = default)
        => OpenInternalAsync(datasetPath, LanceWriteMode.Append, cancellationToken);

    /// <summary>
    /// Opens an existing Lance dataset (or creates a new one) and prepares
    /// to overwrite its contents. The new manifest's version is one more
    /// than the latest existing version (or 1 if no prior version) and
    /// references only the new fragments. Old data files remain on disk
    /// for a future vacuum step to clean up.
    /// </summary>
    public static ValueTask<LanceDatasetWriter> OverwriteAsync(
        string datasetPath, CancellationToken cancellationToken = default)
        => OpenInternalAsync(datasetPath, LanceWriteMode.Overwrite, cancellationToken);

    private static async ValueTask<LanceDatasetWriter> OpenInternalAsync(
        string datasetPath, LanceWriteMode mode, CancellationToken cancellationToken)
    {
        if (datasetPath is null) throw new ArgumentNullException(nameof(datasetPath));

        LanceManifest? baseManifest = null;
        ulong readVersion = 0;

        if (mode == LanceWriteMode.Create)
        {
            if (Directory.Exists(datasetPath))
            {
                if (Directory.Exists(Path.Combine(datasetPath, "_versions"))
                    || Directory.Exists(Path.Combine(datasetPath, "data")))
                    throw new InvalidOperationException(
                        $"Path '{datasetPath}' already contains a Lance dataset. " +
                        "Use AppendAsync or OverwriteAsync to modify it.");
            }
            else
            {
                Directory.CreateDirectory(datasetPath);
            }
        }
        else if (mode == LanceWriteMode.Append)
        {
            if (!Directory.Exists(datasetPath)
                || !Directory.Exists(Path.Combine(datasetPath, "_versions")))
                throw new InvalidOperationException(
                    $"Path '{datasetPath}' does not contain a Lance dataset; " +
                    "cannot append. Use CreateAsync to make a new one.");

            var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(datasetPath);
            var entry = await ManifestPathResolver.ResolveLatestAsync(fs, cancellationToken)
                .ConfigureAwait(false);
            baseManifest = await ManifestReader.ReadAsync(fs, entry.Path, cancellationToken)
                .ConfigureAwait(false);
            readVersion = baseManifest.Version;
        }
        else // Overwrite
        {
            if (Directory.Exists(datasetPath)
                && Directory.Exists(Path.Combine(datasetPath, "_versions")))
            {
                var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(datasetPath);
                var entries = await ManifestPathResolver.ListAllAsync(fs, cancellationToken)
                    .ConfigureAwait(false);
                if (entries.Count > 0)
                {
                    var latest = await ManifestReader.ReadAsync(fs, entries[0].Path, cancellationToken)
                        .ConfigureAwait(false);
                    readVersion = latest.Version;
                }
            }
            else if (!Directory.Exists(datasetPath))
            {
                Directory.CreateDirectory(datasetPath);
            }
        }

        Directory.CreateDirectory(Path.Combine(datasetPath, "data"));
        Directory.CreateDirectory(Path.Combine(datasetPath, "_versions"));
        Directory.CreateDirectory(Path.Combine(datasetPath, "_transactions"));

        string dataFileName = Guid.NewGuid().ToString("N") + ".lance";
        string dataFilePath = Path.Combine(datasetPath, "data", dataFileName);
        var fileWriter = await LanceFileWriter.CreateAsync(dataFilePath, cancellationToken)
            .ConfigureAwait(false);

        return new LanceDatasetWriter(
            datasetPath, mode, baseManifest, readVersion, fileWriter, dataFileName);
    }

    /// <summary>
    /// Closes the current fragment's data file and starts a new one. Both
    /// fragments end up in the same manifest version when
    /// <see cref="FinishAsync"/> is called. Each subsequent fragment must
    /// have the same schema (field names, types, and ids) as the first.
    /// Throws if the current fragment has no rows yet — empty fragments
    /// aren't representable in the manifest.
    /// </summary>
    public async Task NewFragmentAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_finished)
            throw new InvalidOperationException(
                "Dataset writer has already been finalised; cannot start a new fragment.");

        if (_currentFileWriter.TotalRows == 0)
            throw new InvalidOperationException(
                "Current fragment has no rows yet; write at least one column before starting a new fragment.");

        await CloseCurrentFragmentAsync(cancellationToken).ConfigureAwait(false);

        // Open the next fragment's writer.
        string newName = Guid.NewGuid().ToString("N") + ".lance";
        string newPath = Path.Combine(_datasetPath, "data", newName);
        _currentFileWriter = await LanceFileWriter.CreateAsync(newPath, cancellationToken)
            .ConfigureAwait(false);
        _currentDataFileName = newName;
    }

    /// <summary>
    /// Closes the current data file (if it has rows) and writes the
    /// transaction + manifest files. For Append: the new manifest
    /// preserves the base dataset's fields and fragments and appends the
    /// new ones with bumped fragment ids. For Overwrite: only the new
    /// fragments are referenced. Idempotent on success.
    /// </summary>
    public async Task FinishAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_finished) return;

        if (_currentFileWriter.TotalRows > 0)
        {
            await CloseCurrentFragmentAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // No rows in current writer; just dispose it (its file is empty
            // / partial — we leave it on disk; vacuum can clean up).
            await _currentFileWriter.DisposeAsync().ConfigureAwait(false);
        }

        if (_completedFragments.Count == 0)
            throw new InvalidOperationException(
                "Cannot finalise a Lance dataset with no fragments.");

        // For Append, validate our session schema against the base manifest.
        IReadOnlyList<LanceField> sessionFields = _sessionFields!;
        if (_mode == LanceWriteMode.Append)
        {
            ValidateSchemaMatches(
                _baseManifest!.Fields, sessionFields,
                "the appended fragments");
        }

        // --- Build the transaction proto ---
        string txnUuid = Guid.NewGuid().ToString();
        // Transaction filename convention: {read_version}-{uuid}.txn —
        // the read_version prefix lets concurrent appenders detect
        // version conflicts at the filename level.
        string txnFileName = $"{_readVersion}-{txnUuid}.txn";
        var transaction = new Transaction
        {
            ReadVersion = _readVersion,
            Uuid = txnUuid,
        };
        byte[] transactionBytes = transaction.ToByteArray();
        await File.WriteAllBytesAsync(
            Path.Combine(_datasetPath, "_transactions", txnFileName),
            transactionBytes, cancellationToken).ConfigureAwait(false);

        // --- Build the manifest proto ---
        LanceManifest manifest = BuildManifest(sessionFields, txnFileName);

        // --- Pack manifest body + footer (matches ManifestReader.Parse) ---
        byte[] manifestBytes = manifest.ToByteArray();
        long bodySize = sizeof(uint) + transactionBytes.Length
                      + sizeof(uint) + manifestBytes.Length;
        long manifestLenPos = sizeof(uint) + transactionBytes.Length;
        long totalSize = bodySize + 16;

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

        BinaryPrimitives.WriteUInt64LittleEndian(
            buf.AsSpan(cursor, sizeof(ulong)), (ulong)manifestLenPos);
        cursor += sizeof(ulong);
        BinaryPrimitives.WriteUInt16LittleEndian(
            buf.AsSpan(cursor, sizeof(ushort)), ManifestMajorVersion);
        cursor += sizeof(ushort);
        BinaryPrimitives.WriteUInt16LittleEndian(
            buf.AsSpan(cursor, sizeof(ushort)), ManifestMinorVersion);
        cursor += sizeof(ushort);
        buf[cursor++] = (byte)'L';
        buf[cursor++] = (byte)'A';
        buf[cursor++] = (byte)'N';
        buf[cursor++] = (byte)'C';

        ulong newVersion = manifest.Version;
        ulong encodedName = ulong.MaxValue - newVersion;
        string manifestFileName = encodedName.ToString(System.Globalization.CultureInfo.InvariantCulture)
                                + ".manifest";
        await File.WriteAllBytesAsync(
            Path.Combine(_datasetPath, "_versions", manifestFileName),
            buf, cancellationToken).ConfigureAwait(false);

        _finished = true;
    }

    /// <summary>
    /// Build the manifest combining base (Append) or fresh schema
    /// (Create/Overwrite) with the new fragments produced this session.
    /// </summary>
    private LanceManifest BuildManifest(
        IReadOnlyList<LanceField> sessionFields, string transactionFileName)
    {
        var newFragments = new List<DataFragment>(_completedFragments.Count);
        ulong nextFragmentId = 0;
        ulong nextVersion = _readVersion + 1;
        IEnumerable<LanceField> manifestFields;

        if (_mode == LanceWriteMode.Append)
        {
            // Use existing fields and field ids (already validated they
            // match our session fields).
            manifestFields = _baseManifest!.Fields;
            // Carry forward existing fragments, then bump fragment ids
            // beyond the highest existing one.
            foreach (var frag in _baseManifest.Fragments)
                newFragments.Add(frag.Clone());
            ulong maxExisting = 0;
            foreach (var frag in _baseManifest.Fragments)
                if (frag.Id >= maxExisting) maxExisting = frag.Id + 1;
            nextFragmentId = maxExisting;
        }
        else
        {
            manifestFields = sessionFields;
        }

        foreach (var fragment in _completedFragments)
        {
            var dataFile = new DataFile
            {
                Path = fragment.FileName,
                FileMajorVersion = (uint)fragment.Version.Major,
                FileMinorVersion = (uint)fragment.Version.Minor,
                FileSizeBytes = (ulong)fragment.FileSizeBytes,
            };
            for (int i = 0; i < fragment.Fields.Count; i++)
            {
                dataFile.Fields.Add(fragment.Fields[i].Id);
                dataFile.ColumnIndices.Add(i);
            }
            var fragProto = new DataFragment
            {
                Id = nextFragmentId++,
                PhysicalRows = (ulong)fragment.TotalRows,
            };
            fragProto.Files.Add(dataFile);
            newFragments.Add(fragProto);
        }

        var manifest = new LanceManifest
        {
            Version = nextVersion,
            TransactionFile = transactionFileName,
            DataFormat = new LanceManifest.Types.DataStorageFormat
            {
                FileFormat = "lance",
                Version = $"{_completedFragments[0].Version.Major}.{_completedFragments[0].Version.Minor}",
            },
        };
        foreach (var f in manifestFields)
            manifest.Fields.Add(f);
        manifest.Fragments.AddRange(newFragments);
        return manifest;
    }

    private async Task CloseCurrentFragmentAsync(CancellationToken cancellationToken)
    {
        await _currentFileWriter.FinishAsync(cancellationToken).ConfigureAwait(false);
        // Capture metadata BEFORE disposing the writer (FinalSizeBytes is
        // valid only after FinishAsync).
        long size = _currentFileWriter.FinalSizeBytes;
        long rows = _currentFileWriter.TotalRows;
        // Snapshot the schema fields by deep-cloning each LanceField so
        // later writers' state can't mutate our view.
        var fields = _currentFileWriter.SchemaFields
            .Select(f => f.Clone())
            .ToList();
        var version = _currentFileWriter.Version;

        await _currentFileWriter.DisposeAsync().ConfigureAwait(false);

        if (_sessionFields is null)
        {
            _sessionFields = fields;
        }
        else
        {
            ValidateSchemaMatches(_sessionFields, fields,
                $"fragment {_completedFragments.Count + 1}");
        }

        _completedFragments.Add(new CompletedFragment(
            _currentDataFileName, size, rows, fields, version));
    }

    /// <summary>
    /// Validate that <paramref name="actual"/>'s field list matches
    /// <paramref name="expected"/> exactly (same length, same id, name,
    /// parent_id, logical_type, encoding per field).
    /// </summary>
    private static void ValidateSchemaMatches(
        IReadOnlyList<LanceField> expected,
        IReadOnlyList<LanceField> actual,
        string subject)
    {
        if (expected.Count != actual.Count)
            throw new InvalidOperationException(
                $"Schema mismatch in {subject}: expected {expected.Count} fields, got {actual.Count}.");
        for (int i = 0; i < expected.Count; i++)
        {
            var e = expected[i]; var a = actual[i];
            if (e.Id != a.Id || e.ParentId != a.ParentId
                || !string.Equals(e.Name, a.Name, StringComparison.Ordinal)
                || !string.Equals(e.LogicalType, a.LogicalType, StringComparison.Ordinal)
                || e.Encoding != a.Encoding)
            {
                throw new InvalidOperationException(
                    $"Schema mismatch in {subject} at field {i}: " +
                    $"expected (id={e.Id} parent={e.ParentId} name='{e.Name}' " +
                    $"logical='{e.LogicalType}' encoding={e.Encoding}), got " +
                    $"(id={a.Id} parent={a.ParentId} name='{a.Name}' " +
                    $"logical='{a.LogicalType}' encoding={a.Encoding}).");
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        // Dispose the current file writer (if not already finished).
        await _currentFileWriter.DisposeAsync().ConfigureAwait(false);
    }

    private sealed record CompletedFragment(
        string FileName,
        long FileSizeBytes,
        long TotalRows,
        IReadOnlyList<LanceField> Fields,
        LanceVersion Version);
}

/// <summary>
/// How <see cref="LanceDatasetWriter"/> should treat an existing dataset
/// at the target path.
/// </summary>
public enum LanceWriteMode
{
    /// <summary>Refuse if the path already contains a dataset.</summary>
    Create,
    /// <summary>Add new fragments alongside the existing ones; schema
    /// must match.</summary>
    Append,
    /// <summary>Replace the dataset's contents with a fresh manifest;
    /// schema can change.</summary>
    Overwrite,
}
