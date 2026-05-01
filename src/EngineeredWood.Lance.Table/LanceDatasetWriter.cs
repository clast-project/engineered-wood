// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.IO;
using EngineeredWood.Lance.Format;
using EngineeredWood.Lance.Table.Deletions;
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
    private readonly LanceCompressionScheme _compression;
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
        string datasetPath, LanceWriteMode mode, LanceCompressionScheme compression,
        LanceManifest? baseManifest, ulong readVersion,
        LanceFileWriter fileWriter, string dataFileName)
    {
        _datasetPath = datasetPath;
        _mode = mode;
        _compression = compression;
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
        string datasetPath, CancellationToken cancellationToken = default,
        LanceCompressionScheme compression = LanceCompressionScheme.None)
        => OpenInternalAsync(datasetPath, LanceWriteMode.Create, compression, cancellationToken);

    /// <summary>
    /// Opens an existing Lance dataset for appending new fragments. The
    /// new fragments' schema must match the existing dataset's schema
    /// (same field names, types, and ids). Throws if no manifest exists
    /// at <paramref name="datasetPath"/>.
    /// </summary>
    public static ValueTask<LanceDatasetWriter> AppendAsync(
        string datasetPath, CancellationToken cancellationToken = default,
        LanceCompressionScheme compression = LanceCompressionScheme.None)
        => OpenInternalAsync(datasetPath, LanceWriteMode.Append, compression, cancellationToken);

    /// <summary>
    /// Opens an existing Lance dataset (or creates a new one) and prepares
    /// to overwrite its contents. The new manifest's version is one more
    /// than the latest existing version (or 1 if no prior version) and
    /// references only the new fragments. Old data files remain on disk
    /// for a future vacuum step to clean up.
    /// </summary>
    public static ValueTask<LanceDatasetWriter> OverwriteAsync(
        string datasetPath, CancellationToken cancellationToken = default,
        LanceCompressionScheme compression = LanceCompressionScheme.None)
        => OpenInternalAsync(datasetPath, LanceWriteMode.Overwrite, compression, cancellationToken);

    private static async ValueTask<LanceDatasetWriter> OpenInternalAsync(
        string datasetPath, LanceWriteMode mode,
        LanceCompressionScheme compression,
        CancellationToken cancellationToken)
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
        var fileWriter = await LanceFileWriter.CreateAsync(
            dataFilePath, cancellationToken, compression).ConfigureAwait(false);

        return new LanceDatasetWriter(
            datasetPath, mode, compression, baseManifest, readVersion,
            fileWriter, dataFileName);
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
        _currentFileWriter = await LanceFileWriter.CreateAsync(
            newPath, cancellationToken, _compression).ConfigureAwait(false);
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
        await WriteManifestFileAsync(_datasetPath, manifest, transactionBytes, cancellationToken)
            .ConfigureAwait(false);
        _finished = true;
    }

    /// <summary>
    /// Pack a manifest proto + its transaction proto into the on-disk
    /// manifest file format (transaction-len + transaction + manifest-len
    /// + manifest + 16-byte trailer with the manifest-len position, file
    /// version, and "LANC" magic) and write it to
    /// <c>_versions/{u64::MAX - version}.manifest</c>. Used by both the
    /// regular write path and out-of-band ops like
    /// <see cref="DeleteRowsAsync"/>.
    /// </summary>
    private static async Task WriteManifestFileAsync(
        string datasetPath, LanceManifest manifest, byte[] transactionBytes,
        CancellationToken cancellationToken)
    {
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

        ulong encodedName = ulong.MaxValue - manifest.Version;
        string manifestFileName = encodedName.ToString(System.Globalization.CultureInfo.InvariantCulture)
                                + ".manifest";
        await File.WriteAllBytesAsync(
            Path.Combine(datasetPath, "_versions", manifestFileName),
            buf, cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    /// Marks a set of rows as deleted in the dataset at
    /// <paramref name="datasetPath"/>. Writes a per-fragment deletion
    /// file under <c>_deletions/</c> for each affected fragment, then
    /// publishes a new manifest version where those fragments carry a
    /// <c>deletion_file</c> reference. Returns the new version number.
    ///
    /// <para><paramref name="deletedOffsetsByFragment"/> maps a fragment
    /// id to the per-fragment row offsets that should be deleted (0 ≤
    /// offset &lt; fragment.PhysicalRows). Fragments not present in the
    /// map are unchanged. If a fragment already has a deletion file from
    /// a prior version, the new offsets are merged with the existing
    /// ones.</para>
    ///
    /// <para>Returns the current version (no-op) when the map is empty.
    /// All deletion files use the Arrow IPC format
    /// (<c>DeletionFileType.ARROW_ARRAY</c>) — a single Int32 column of
    /// row offsets — which is the simplest and most broadly compatible
    /// shape; switching to Roaring bitmaps for dense deletes is a
    /// follow-up.</para>
    /// </summary>
    public static async ValueTask<long> DeleteRowsAsync(
        string datasetPath,
        IReadOnlyDictionary<ulong, IReadOnlyList<int>> deletedOffsetsByFragment,
        CancellationToken cancellationToken = default)
    {
        if (datasetPath is null) throw new ArgumentNullException(nameof(datasetPath));
        if (deletedOffsetsByFragment is null)
            throw new ArgumentNullException(nameof(deletedOffsetsByFragment));
        if (!Directory.Exists(datasetPath)
            || !Directory.Exists(Path.Combine(datasetPath, "_versions")))
            throw new InvalidOperationException(
                $"Path '{datasetPath}' does not contain a Lance dataset.");

        // Read the current manifest to figure out what version we're
        // building on top of and what fragments exist.
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(datasetPath);
        var entry = await ManifestPathResolver.ResolveLatestAsync(fs, cancellationToken)
            .ConfigureAwait(false);
        var baseManifest = await ManifestReader.ReadAsync(fs, entry.Path, cancellationToken)
            .ConfigureAwait(false);
        ulong readVersion = baseManifest.Version;

        // Filter out empty work and validate fragment ids upfront.
        var work = deletedOffsetsByFragment
            .Where(kv => kv.Value is not null && kv.Value.Count > 0)
            .ToDictionary(kv => kv.Key, kv => kv.Value);
        if (work.Count == 0)
            return (long)readVersion;

        var fragById = baseManifest.Fragments.ToDictionary(f => f.Id);
        foreach (var fragId in work.Keys)
        {
            if (!fragById.ContainsKey(fragId))
                throw new ArgumentException(
                    $"Fragment id {fragId} is not in the dataset (have ids: " +
                    $"[{string.Join(", ", fragById.Keys)}]).",
                    nameof(deletedOffsetsByFragment));
        }

        Directory.CreateDirectory(Path.Combine(datasetPath, "_deletions"));

        ulong newVersion = readVersion + 1;
        // The DeletionFile.id field is opaque per the proto comment ("used
        // to differentiate this file from others written by concurrent
        // writers"). Using newVersion is a stable, monotonic, unique-
        // per-version choice — concurrent writers would conflict at the
        // manifest filename level first anyway.
        ulong newDeletionId = newVersion;

        // Build the new fragments list — unaffected fragments cloned
        // verbatim, affected ones get a fresh deletion_file reference.
        var newFragments = new List<DataFragment>(baseManifest.Fragments.Count);
        foreach (var oldFrag in baseManifest.Fragments)
        {
            if (!work.TryGetValue(oldFrag.Id, out var newOffsetsList))
            {
                newFragments.Add(oldFrag.Clone());
                continue;
            }

            // Validate per-fragment offsets and merge with any existing
            // deletion mask.
            var merged = new HashSet<int>();
            if (oldFrag.DeletionFile is not null && oldFrag.DeletionFile.NumDeletedRows > 0)
            {
                var existing = await DeletionFileReader.ReadAsync(
                    fs, oldFrag.Id, oldFrag.DeletionFile, cancellationToken)
                    .ConfigureAwait(false);
                foreach (int off in existing.DeletedOffsets)
                    merged.Add(off);
            }
            ulong physicalRows = oldFrag.PhysicalRows;
            foreach (int off in newOffsetsList)
            {
                if (off < 0 || (ulong)off >= physicalRows)
                    throw new ArgumentOutOfRangeException(
                        nameof(deletedOffsetsByFragment),
                        $"Row offset {off} out of range for fragment {oldFrag.Id} " +
                        $"(physical_rows={physicalRows}).");
                merged.Add(off);
            }

            // Write the deletion file. Path format matches the reader's
            // expectation: _deletions/{fragmentId}-{readVersion}-{id}.arrow.
            string deletionFileName =
                $"{oldFrag.Id}-{readVersion}-{newDeletionId}.arrow";
            string deletionFilePath = Path.Combine(
                datasetPath, "_deletions", deletionFileName);
            await WriteDeletionFileArrowAsync(
                deletionFilePath, merged, cancellationToken).ConfigureAwait(false);

            // Clone the fragment but replace the deletion_file ref.
            var newFrag = oldFrag.Clone();
            newFrag.DeletionFile = new DeletionFile
            {
                FileType = DeletionFile.Types.DeletionFileType.ArrowArray,
                ReadVersion = readVersion,
                Id = newDeletionId,
                NumDeletedRows = (ulong)merged.Count,
            };
            newFragments.Add(newFrag);
        }

        // --- Build the new manifest ---
        string txnUuid = Guid.NewGuid().ToString();
        string txnFileName = $"{readVersion}-{txnUuid}.txn";
        var transaction = new Transaction
        {
            ReadVersion = readVersion,
            Uuid = txnUuid,
        };
        byte[] transactionBytes = transaction.ToByteArray();
        await File.WriteAllBytesAsync(
            Path.Combine(datasetPath, "_transactions", txnFileName),
            transactionBytes, cancellationToken).ConfigureAwait(false);

        var newManifest = new LanceManifest
        {
            Version = newVersion,
            TransactionFile = txnFileName,
            DataFormat = baseManifest.DataFormat?.Clone() ?? new LanceManifest.Types.DataStorageFormat
            {
                FileFormat = "lance",
                Version = "2.1",
            },
        };
        newManifest.Fields.AddRange(baseManifest.Fields);
        newManifest.Fragments.AddRange(newFragments);

        await WriteManifestFileAsync(datasetPath, newManifest, transactionBytes, cancellationToken)
            .ConfigureAwait(false);

        return (long)newVersion;
    }

    /// <summary>
    /// Deletes rows matching <paramref name="predicate"/> from the dataset
    /// at <paramref name="datasetPath"/>. For each fragment, reads the
    /// columns the predicate references, evaluates it, and collects the
    /// row offsets where it returns true; those offsets are then written
    /// out via <see cref="DeleteRowsAsync"/>. Returns the new version
    /// number, or the current version when no rows match (no-op).
    ///
    /// <para>Rows already covered by an existing deletion file are
    /// skipped (no point re-marking them). Predicate-evaluation
    /// semantics match <see cref="LanceTable.ReadAsync(IReadOnlyList{string},
    /// Predicate, CancellationToken)"/>: a null result counts as
    /// not-true and the row stays.</para>
    /// </summary>
    public static async ValueTask<long> DeleteAsync(
        string datasetPath, Predicate predicate,
        CancellationToken cancellationToken = default)
    {
        if (datasetPath is null) throw new ArgumentNullException(nameof(datasetPath));
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));
        if (!Directory.Exists(datasetPath)
            || !Directory.Exists(Path.Combine(datasetPath, "_versions")))
            throw new InvalidOperationException(
                $"Path '{datasetPath}' does not contain a Lance dataset.");

        // Read the latest manifest + Arrow schema.
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(datasetPath);
        var entry = await ManifestPathResolver.ResolveLatestAsync(fs, cancellationToken)
            .ConfigureAwait(false);
        var manifest = await ManifestReader.ReadAsync(fs, entry.Path, cancellationToken)
            .ConfigureAwait(false);

        var lanceSchema = new EngineeredWood.Lance.Proto.Schema();
        lanceSchema.Fields.AddRange(manifest.Fields);
        var arrowSchema = EngineeredWood.Lance.Schema.LanceSchemaConverter.ToArrowSchema(lanceSchema);

        // Resolve which top-level columns the predicate touches.
        var refs = new HashSet<string>(StringComparer.Ordinal);
        CollectColumnReferences(predicate, refs);
        var readIndices = new List<int>(refs.Count);
        var readFields = new List<Apache.Arrow.Field>(refs.Count);
        foreach (string name in refs)
        {
            int idx = arrowSchema.GetFieldIndex(name);
            if (idx < 0)
                throw new ArgumentException(
                    $"Filter references unknown column '{name}'. " +
                    $"Available: [{string.Join(", ", arrowSchema.FieldsList.Select(f => f.Name))}].",
                    nameof(predicate));
            readIndices.Add(idx);
            readFields.Add(arrowSchema.FieldsList[idx]);
        }
        var readSchema = new Apache.Arrow.Schema(readFields, metadata: null);
        var evaluator = new ArrowRowEvaluator();

        var perFragmentDeletes = new Dictionary<ulong, IReadOnlyList<int>>();

        foreach (var fragment in manifest.Fragments)
        {
            DataFile file = fragment.Files[0];
            string relPath = "data/" + file.Path;
            await using IRandomAccessFile raf = await fs.OpenReadAsync(relPath, cancellationToken)
                .ConfigureAwait(false);
            await using var reader = await EngineeredWood.Lance.LanceFileReader
                .OpenAsync(raf, ownsReader: false, cancellationToken)
                .ConfigureAwait(false);

            int rowCount = checked((int)reader.NumberOfRows);

            // Skip rows already deleted — no point re-marking them, and it
            // keeps the new deletion file minimal.
            DeletionMask? existing = null;
            if (fragment.DeletionFile is not null && fragment.DeletionFile.NumDeletedRows > 0)
            {
                existing = await DeletionFileReader.ReadAsync(
                    fs, fragment.Id, fragment.DeletionFile, cancellationToken)
                    .ConfigureAwait(false);
            }

            // Read just the columns the predicate references and evaluate.
            var readArrays = new IArrowArray[readIndices.Count];
            for (int i = 0; i < readIndices.Count; i++)
                readArrays[i] = await reader
                    .ReadColumnAsync(readIndices[i], cancellationToken)
                    .ConfigureAwait(false);
            var readBatch = new RecordBatch(readSchema, readArrays, rowCount);

            BooleanArray mask = evaluator.EvaluatePredicate(predicate, readBatch);
            var toDelete = new List<int>();
            for (int i = 0; i < rowCount; i++)
            {
                if (existing is not null && existing.IsDeleted(i)) continue;
                bool? v = mask.IsNull(i) ? (bool?)null : mask.GetValue(i);
                if (v == true) toDelete.Add(i);
            }
            if (toDelete.Count > 0)
                perFragmentDeletes[fragment.Id] = toDelete;
        }

        if (perFragmentDeletes.Count == 0)
            return (long)manifest.Version;

        return await DeleteRowsAsync(datasetPath, perFragmentDeletes, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Walk a predicate tree collecting the names of every column it
    /// references. Mirrors <see cref="LanceTable"/>'s private
    /// <c>CollectColumnReferences</c>.
    /// </summary>
    private static void CollectColumnReferences(Predicate predicate, HashSet<string> sink)
    {
        switch (predicate)
        {
            case TruePredicate or FalsePredicate:
                break;
            case AndPredicate and:
                foreach (var c in and.Children) CollectColumnReferences(c, sink);
                break;
            case OrPredicate or:
                foreach (var c in or.Children) CollectColumnReferences(c, sink);
                break;
            case NotPredicate not:
                CollectColumnReferences(not.Child, sink);
                break;
            case ComparisonPredicate cmp:
                CollectColumnReferences(cmp.Left, sink);
                CollectColumnReferences(cmp.Right, sink);
                break;
            case UnaryPredicate u:
                CollectColumnReferences(u.Operand, sink);
                break;
            case SetPredicate s:
                CollectColumnReferences(s.Operand, sink);
                break;
        }
    }

    private static void CollectColumnReferences(Expression expression, HashSet<string> sink)
    {
        switch (expression)
        {
            case UnboundReference u: sink.Add(u.Name); break;
            case BoundReference b: sink.Add(b.Name); break;
            case LiteralExpression: break;
            case FunctionCall fc:
                foreach (var arg in fc.Arguments) CollectColumnReferences(arg, sink);
                break;
            case Predicate p: CollectColumnReferences(p, sink); break;
        }
    }

    /// <summary>
    /// Write a Lance Arrow-IPC deletion file: a single record batch with
    /// one column of deleted row offsets in ascending order. The proto
    /// comment says "Int32Array" but pylance / lance-rs require a strict
    /// schema of <c>{ row_id: uint32 }</c> and reject anything else as a
    /// "corrupt file". Match pylance.
    /// </summary>
    private static async Task WriteDeletionFileArrowAsync(
        string path, IReadOnlyCollection<int> deletedOffsets,
        CancellationToken cancellationToken)
    {
        // Sort for determinism; the reader doesn't require it but it
        // makes the file byte-identical for a given input set.
        var sorted = deletedOffsets.ToArray();
        System.Array.Sort(sorted);

        var builder = new UInt32Array.Builder().Reserve(sorted.Length);
        foreach (int off in sorted) builder.Append(checked((uint)off));
        var arr = builder.Build();
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("row_id", UInt32Type.Default, nullable: false),
        }, metadata: null);
        var batch = new RecordBatch(schema, new[] { (IArrowArray)arr }, sorted.Length);

        await using var fs = File.Create(path);
        using (var writer = new ArrowFileWriter(fs, schema))
        {
            await writer.WriteRecordBatchAsync(batch, cancellationToken)
                .ConfigureAwait(false);
            await writer.WriteEndAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Removes data files, manifest files, and transaction files no longer
    /// referenced by any retained version of the dataset at
    /// <paramref name="datasetPath"/>. Use to free disk space after
    /// overwrite operations or once historical versions are no longer
    /// needed.
    ///
    /// <para><b>Retention</b>: <see cref="LanceVacuumOptions.RetainVersions"/>
    /// (default 1) keeps the N most recent manifest versions and every
    /// data / transaction file they reference. Older versions and any
    /// files referenced only by them become candidates for deletion.
    /// Setting <see cref="LanceVacuumOptions.DryRun"/> = true reports
    /// what would be deleted without actually deleting anything.</para>
    ///
    /// <para>This is a one-shot maintenance operation; it does not
    /// coordinate with concurrent writers and assumes exclusive access
    /// to the dataset directory while it runs.</para>
    /// </summary>
    public static async ValueTask<LanceVacuumResult> VacuumAsync(
        string datasetPath, LanceVacuumOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (datasetPath is null) throw new ArgumentNullException(nameof(datasetPath));
        options ??= new LanceVacuumOptions();
        if (options.RetainVersions < 1)
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "RetainVersions must be at least 1; vacuum always keeps the latest version.");

        string versionsDir = Path.Combine(datasetPath, "_versions");
        string dataDir = Path.Combine(datasetPath, "data");
        string txnDir = Path.Combine(datasetPath, "_transactions");

        if (!Directory.Exists(datasetPath) || !Directory.Exists(versionsDir))
            throw new InvalidOperationException(
                $"Path '{datasetPath}' does not contain a Lance dataset; nothing to vacuum.");

        // List manifests newest-first via the resolver. Anything beyond the
        // first RetainVersions entries is a candidate for deletion.
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(datasetPath);
        var allEntries = await ManifestPathResolver.ListAllAsync(fs, cancellationToken)
            .ConfigureAwait(false);
        if (allEntries.Count == 0)
            return new LanceVacuumResult();

        int retainCount = Math.Min(options.RetainVersions, allEntries.Count);
        var retainedEntries = allEntries.Take(retainCount).ToArray();
        var droppedEntries = allEntries.Skip(retainCount).ToArray();

        // Read each retained manifest and collect the live data file
        // names + the transaction filenames it references.
        var liveDataFiles = new HashSet<string>(StringComparer.Ordinal);
        var liveTxnFiles = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in retainedEntries)
        {
            var manifest = await ManifestReader.ReadAsync(fs, entry.Path, cancellationToken)
                .ConfigureAwait(false);
            foreach (var fragment in manifest.Fragments)
            {
                foreach (var file in fragment.Files)
                {
                    // DataFile.Path is relative to the dataset root, e.g.
                    // "data/abc.lance" — but our writer just uses the bare
                    // filename. Normalise to the basename.
                    string baseName = Path.GetFileName(file.Path);
                    liveDataFiles.Add(baseName);
                }
            }
            if (!string.IsNullOrEmpty(manifest.TransactionFile))
                liveTxnFiles.Add(Path.GetFileName(manifest.TransactionFile));
        }

        var deletedDataFiles = new List<string>();
        var deletedManifests = new List<string>();
        var deletedTxnFiles = new List<string>();
        long bytesDeleted = 0;

        // Walk data/ — anything not referenced by a retained manifest is
        // orphaned. Skip non-.lance files (the dir might also hold deletion
        // files in a future version of this writer).
        if (Directory.Exists(dataDir))
        {
            foreach (string file in Directory.EnumerateFiles(dataDir, "*.lance"))
            {
                string baseName = Path.GetFileName(file);
                if (liveDataFiles.Contains(baseName)) continue;
                long size = new FileInfo(file).Length;
                if (!options.DryRun) File.Delete(file);
                deletedDataFiles.Add(baseName);
                bytesDeleted += size;
            }
        }

        // Walk _versions/ — drop manifests not in retainedEntries.
        var retainedManifestPaths = new HashSet<string>(
            retainedEntries.Select(e => Path.GetFileName(e.Path)),
            StringComparer.Ordinal);
        foreach (string file in Directory.EnumerateFiles(versionsDir, "*.manifest"))
        {
            string baseName = Path.GetFileName(file);
            if (retainedManifestPaths.Contains(baseName)) continue;
            long size = new FileInfo(file).Length;
            if (!options.DryRun) File.Delete(file);
            deletedManifests.Add(baseName);
            bytesDeleted += size;
        }

        // Walk _transactions/ — drop transactions not referenced by any
        // retained manifest.
        if (Directory.Exists(txnDir))
        {
            foreach (string file in Directory.EnumerateFiles(txnDir, "*.txn"))
            {
                string baseName = Path.GetFileName(file);
                if (liveTxnFiles.Contains(baseName)) continue;
                long size = new FileInfo(file).Length;
                if (!options.DryRun) File.Delete(file);
                deletedTxnFiles.Add(baseName);
                bytesDeleted += size;
            }
        }

        return new LanceVacuumResult
        {
            DataFilesDeleted = deletedDataFiles,
            ManifestsDeleted = deletedManifests,
            TransactionsDeleted = deletedTxnFiles,
            BytesDeleted = bytesDeleted,
            DryRun = options.DryRun,
        };
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

/// <summary>
/// Options for <see cref="LanceDatasetWriter.VacuumAsync"/>.
/// </summary>
public sealed record LanceVacuumOptions
{
    /// <summary>
    /// Number of most-recent manifest versions to retain. Older versions
    /// (and any data / transaction files only referenced by them) become
    /// candidates for deletion. Default 1 = keep only the latest version.
    /// Must be at least 1.
    /// </summary>
    public int RetainVersions { get; init; } = 1;

    /// <summary>
    /// When true, identify what would be deleted but don't actually delete
    /// anything. Useful for previewing the effect.
    /// </summary>
    public bool DryRun { get; init; } = false;
}

/// <summary>
/// Outcome of <see cref="LanceDatasetWriter.VacuumAsync"/> — lists the
/// files removed (or that would be removed in dry-run mode) and the
/// total bytes reclaimed.
/// </summary>
public sealed record LanceVacuumResult
{
    /// <summary>
    /// Filenames in <c>data/</c> that were deleted, relative to the
    /// dataset root. Each entry corresponds to one fragment data file
    /// from a vacuumed version that's no longer referenced by any
    /// retained manifest.
    /// </summary>
    public IReadOnlyList<string> DataFilesDeleted { get; init; }
        = System.Array.Empty<string>();
    /// <summary>
    /// Manifest filenames in <c>_versions/</c> that were deleted.
    /// </summary>
    public IReadOnlyList<string> ManifestsDeleted { get; init; }
        = System.Array.Empty<string>();
    /// <summary>
    /// Transaction filenames in <c>_transactions/</c> that were deleted.
    /// </summary>
    public IReadOnlyList<string> TransactionsDeleted { get; init; }
        = System.Array.Empty<string>();
    /// <summary>Total bytes reclaimed across all three buckets.</summary>
    public long BytesDeleted { get; init; }
    /// <summary>
    /// True if vacuum ran in dry-run mode — the file lists describe what
    /// would have been deleted; nothing was actually removed.
    /// </summary>
    public bool DryRun { get; init; }
}
