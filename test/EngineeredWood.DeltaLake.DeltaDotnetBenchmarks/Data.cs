// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace DeltaDotnetComparison;

/// <summary>
/// The benchmark dataset. Every value is a pure function of the row's <c>id</c>, so any id range can be
/// regenerated identically, and the two engines are always handed the same rows.
/// </summary>
internal static class Data
{
    public const int CategoryCount = 16;

    public static readonly Schema Schema = new Schema.Builder()
        .Field(new Field("id", Int64Type.Default, nullable: true))
        .Field(new Field("category", StringType.Default, nullable: true))
        .Field(new Field("quantity", Int32Type.Default, nullable: true))
        .Field(new Field("amount", DoubleType.Default, nullable: true))
        .Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: true))
        .Field(new Field("note", StringType.Default, nullable: true))
        .Build();

    private static readonly string[] Categories =
        Enumerable.Range(0, CategoryCount).Select(i => $"cat_{i:D2}").ToArray();

    private static readonly long BaseMicros =
        new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1000;

    private const string Alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /// <summary>Rows <c>[firstId, firstId + rows)</c>, in batches of at most <paramref name="batchRows"/>.</summary>
    public static List<RecordBatch> Generate(long firstId, int rows, int batchRows = 65_536)
    {
        var batches = new List<RecordBatch>();
        for (int start = 0; start < rows; start += batchRows)
            batches.Add(Batch(firstId + start, Math.Min(batchRows, rows - start)));
        return batches;
    }

    /// <summary><paramref name="batch"/> with every <c>amount</c> doubled: a MERGE source whose matched rows visibly
    /// change the table.</summary>
    public static RecordBatch DoubleAmount(RecordBatch batch)
    {
        int column = batch.Schema.GetFieldIndex("amount");
        var amounts = (DoubleArray)batch.Column(column);
        var doubled = new DoubleArray.Builder().Reserve(batch.Length);
        for (int i = 0; i < batch.Length; i++)
            doubled.Append(amounts.GetValue(i)!.Value * 2);
        var columns = Enumerable.Range(0, batch.ColumnCount).Select(c => c == column ? doubled.Build() : batch.Column(c));
        return new RecordBatch(batch.Schema, columns.ToList(), batch.Length);
    }

    private static RecordBatch Batch(long firstId, int length)
    {
        var id = new Int64Array.Builder().Reserve(length);
        var category = new StringArray.Builder();
        var quantity = new Int32Array.Builder().Reserve(length);
        var amount = new DoubleArray.Builder().Reserve(length);
        var ts = new TimestampArray.Builder((TimestampType)Schema.FieldsList[4].DataType).Reserve(length);
        var note = new StringArray.Builder();
        Span<char> chars = stackalloc char[16];

        for (int i = 0; i < length; i++)
        {
            long rowId = firstId + i;
            ulong h = Mix((ulong)rowId);
            id.Append(rowId);
            category.Append(Categories[(int)(h % CategoryCount)]);
            quantity.Append((int)((h >> 8) % 1000));
            amount.Append(((h >> 20) % 1_000_000) / 100.0);
            ts.Append(DateTimeOffset.FromUnixTimeMilliseconds((BaseMicros + rowId * 1000) / 1000));
            ulong n = Mix(h);
            for (int c = 0; c < chars.Length; c++)
            {
                chars[c] = Alphabet[(int)(n % (ulong)Alphabet.Length)];
                n = c % 8 == 7 ? Mix(n) : n / (ulong)Alphabet.Length;
            }
            note.Append(chars.ToString());
        }

        return new RecordBatch(Schema,
            [id.Build(), category.Build(), quantity.Build(), amount.Build(), ts.Build(), note.Build()],
            length);
    }

    /// <summary>SplitMix64 finalizer.</summary>
    private static ulong Mix(ulong z)
    {
        z += 0x9E3779B97F4A7C15UL;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }
}
