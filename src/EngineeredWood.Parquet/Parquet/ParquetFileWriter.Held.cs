// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Parquet;

// Small batches given to WriteBatchAsync are copied and held, then encoded together (#941).
public sealed partial class ParquetFileWriter
{
    /// <summary>
    /// A batch of fewer rows than this is held (<see cref="HeldRows"/>), and the held rows are encoded together once
    /// there would be more.
    /// </summary>
    /// <remarks>
    /// MEASURED (24 cores, the #652 lineitem shape, 16 columns, ZSTD, 200,000 rows): one-row batches took 3.8 s
    /// appended one at a time, and about 0.2 s held. Encoding 1,024 rows together costs about what a batch of 1,024
    /// does, and their copies a few hundredths of a microsecond a cell. 1,024 is also <see cref="ProbeRows"/>: held
    /// rows at the start of a file are encoded as the probe they would be cut to anyway.
    /// </remarks>
    internal const int HoldRows = 1_024;

    /// <summary>
    /// Batches of fewer rows than this are held: <see cref="HoldRows"/>, or 0 to hold none, which tests compare against.
    /// </summary>
    internal int HoldBelowRows { get; set; } = HoldRows;

    /// <summary>The bytes of the buffers rows are held in: for tests.</summary>
    internal long HeldBufferBytes => _held?.BufferBytes ?? 0;

    /// <summary>The most bytes the held rows take, in Arrow's layout, before they are encoded.</summary>
    internal const long HeldBytesLimit = 1L << 20;

    /// <summary>The held rows, or buffers for them; null until a batch is held.</summary>
    private HeldRows? _held;

    /// <summary>The schema of the last small batch found not to be holdable, so that the next of it is not looked at again.</summary>
    private Apache.Arrow.Schema? _notHeld;

    /// <summary>The schema of a batch checked whole as it was held, so that one with the same one is checked only for
    /// nulls in required columns.</summary>
    private Apache.Arrow.Schema? _heldChecked;

    /// <summary>Which columns of the file are required, by position, for the checks of held batches.</summary>
    private bool[]? _requiredColumns;

    /// <summary>The open row group's estimated size while rows are held: it does not change until they are encoded.</summary>
    private long _heldOpenBytes;

    /// <summary>
    /// Holds <paramref name="batch"/>'s rows if it is small, of the held rows' layout, and would not fill the open row
    /// group: checked as any batch is, then copied. Returns false, holding nothing, otherwise.
    /// </summary>
    /// <exception cref="ArgumentException">As for <see cref="WriteBatchAsync"/>: the batch is refused, and what is held
    /// stays as it was.</exception>
    private bool TryHold(RecordBatch batch)
    {
        if (batch.Length == 0 || batch.Length >= HoldBelowRows)
            return false;

        int heldRows = _held?.Rows ?? 0;
        if (heldRows + batch.Length > HoldBelowRows)
            return false;

        // A batch that fills the open row group by rows is encoded as it comes, so the row group is cut, and written,
        // by the call that fills it.
        long openRows = _open?.Rows ?? 0;
        if (openRows + heldRows + batch.Length >= _options.RowGroupMaxRows)
            return false;

        if (_held is null || !_held.Takes(batch))
        {
            if (heldRows > 0)
                return false; // the rows held are encoded first
            if (ReferenceEquals(batch.Schema, _notHeld))
                return false;
            if (HeldRows.For(batch) is not { } held)
            {
                _notHeld = batch.Schema;
                return false;
            }
            _held = held;
        }

        // Sized before anything is copied: a batch too large to hold grows no buffer.
        if (heldRows == 0)
            _heldOpenBytes = _open?.EstimatedBytes() ?? 0;
        long bytes = _held.BytesFor(batch);
        if (HeldRowsFill(heldRows + batch.Length, _held.Bytes + bytes))
            return false;

        CheckHeld(batch);
        _held.Append(batch, bytes);
        return true;
    }

    /// <summary>
    /// Whether <paramref name="rows"/> held rows of <paramref name="bytes"/> (in Arrow's layout) are too many bytes to
    /// hold, or would take the open row group to <see cref="ParquetWriteOptions.RowGroupMaxBytes"/>: counted at the open
    /// row group's encoded bytes per row (or the last row group's), as <see cref="RowsToAppend"/> cuts a batch; at their
    /// Arrow size before there is either.
    /// </summary>
    private bool HeldRowsFill(int rows, long bytes)
    {
        if (bytes > HeldBytesLimit)
            return true;

        long openRows = _open?.Rows ?? 0;
        double? bytesPerRow = openRows > 0 ? (double)_heldOpenBytes / openRows : _lastBytesPerRow;
        double heldBytes = bytesPerRow is { } perRow ? perRow * rows : bytes;
        return _heldOpenBytes + heldBytes >= _options.RowGroupMaxBytes;
    }

    /// <summary>
    /// Refuses a batch about to be held as <see cref="PrepareBatch"/> and <see cref="ShredAndCheck"/> would. A batch of
    /// a schema already checked needs only its required columns checked for nulls: every other check depends only on
    /// the schema. What they return is not kept: the held rows are prepared when they are encoded.
    /// </summary>
    private void CheckHeld(RecordBatch batch)
    {
        if (ReferenceEquals(batch.Schema, _heldChecked))
        {
            var required = _requiredColumns!;
            for (int i = 0; i < required.Length; i++)
            {
                if (required[i] && batch.Column(i).NullCount != 0)
                {
                    // Refused here, with the message the whole check gives.
                    ShredAndCheck(PrepareBatch(batch));
                }
            }
            return;
        }

        ShredAndCheck(PrepareBatch(batch));
        _heldChecked = batch.Schema;
        if (_requiredColumns is null)
        {
            var fields = _arrowSchema!.FieldsList;
            _requiredColumns = new bool[fields.Count];
            for (int i = 0; i < fields.Count; i++)
                _requiredColumns[i] = !fields[i].IsNullable;
        }
    }

    /// <summary>
    /// Refuses a batch that is not held, before the rows held are encoded for it: encoding them can write the row group
    /// they fill, and a refused batch leaves the open row group as it was (#945 review). Returns the batch prepared,
    /// for the caller to encode. A batch of rows already held or written was checked as they were: a file's first
    /// batch has nothing held before it.
    /// </summary>
    private RecordBatch PrepareAndCheckBeforeHeld(RecordBatch batch)
    {
        var prepared = PrepareBatch(batch);
        EnsureWholeBatchMatches(prepared);
        return prepared;
    }

    /// <summary>
    /// Encodes the held rows into the open row group, and writes the row group if that fills it. Returns whether it
    /// did. The held rows are encoded whole: they cannot fill the row group by rows, and a row group they take past
    /// <see cref="ParquetWriteOptions.RowGroupMaxBytes"/> does so by less than they encode to. A failure discards the
    /// open row group, the held rows with it.
    /// </summary>
    private async ValueTask<bool> EncodeHeldAsync(CancellationToken cancellationToken)
    {
        if (_held is not { Rows: > 0 } held)
            return false;

        try
        {
            await WriteHeaderAsync(cancellationToken).ConfigureAwait(false);
            // Prepared as any batch is (compacting nothing, rescaling nothing: CheckHeld checked these layouts), and
            // encoded with its arrays over the held buffers, which the writer does not keep.
            AppendToOpenRowGroup(PrepareBatch(held.Build()));
        }
        catch
        {
            DiscardOpenRowGroup();
            throw;
        }
        finally
        {
            held.Clear();
        }

        if (_open!.Rows >= _options.RowGroupMaxRows || _open.EstimatedBytes() >= _options.RowGroupMaxBytes)
        {
            await FlushOpenRowGroupAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        return false;
    }
}
