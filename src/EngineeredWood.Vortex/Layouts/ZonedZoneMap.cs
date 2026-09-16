// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;

namespace EngineeredWood.Vortex.Layouts;

/// <summary>
/// One aggregate function recorded in a <c>vortex.zoned</c> layout's metadata: its wire id and
/// its serialized options, exactly as upstream's <c>AggregateSpecProto</c> carries them.
/// </summary>
internal sealed class ZoneAggregate
{
    public string Id { get; }
    public byte[] Options { get; }

    public ZoneAggregate(string id, byte[] options)
    {
        Id = id;
        Options = options;
    }
}

/// <summary>
/// The <c>vortex.zoned</c> layout (vortex 0.84+, edition <c>core2026.08.0</c>), which replaced
/// <c>vortex.stats</c> as the zone map the default writer produces.
/// </summary>
/// <remarks>
/// <para>The children are the same as <c>vortex.stats</c> (data, then the zones table), but the
/// metadata names aggregate functions instead of a <see cref="Stat"/> bitset:
/// <c>version:u8 = 1</c>, then a protobuf <c>{ 1: uint32 zone_len, 2: repeated { 1: string id,
/// 2: bytes options } }</c> (<c>vortex-layout/src/layouts/zoned/mod.rs</c>).</para>
/// <para>The zones table is a struct with one field per aggregate that supports the column's
/// dtype, in metadata order, named <c>id(options)</c> and typed by the aggregate's partial state
/// (<c>aggregate_stats_table_dtype</c> in <c>schema.rs</c>). Every field is nullable.</para>
/// <para>An aggregate this reader doesn't know means the table's dtype can't be derived, so the
/// zone map is ignored and the column is read without pruning, as upstream does under
/// <c>allow_unknown</c>.</para>
/// </remarks>
internal static class ZonedZoneMap
{
    public const string Max = "vortex.max";
    public const string Min = "vortex.min";
    public const string BoundedMax = "vortex.bounded_max";
    public const string BoundedMin = "vortex.bounded_min";
    public const string NullCount = "vortex.null_count";
    public const string NanCount = "vortex.nan_count";

    private const byte MetadataVersion = 1;

    /// <summary>
    /// Parses the layout metadata. Returns false when the version is not one this reader knows.
    /// </summary>
    public static bool TryParseMetadata(byte[] metadata, out int zoneLen, out IReadOnlyList<ZoneAggregate> aggregates)
    {
        zoneLen = 0;
        aggregates = System.Array.Empty<ZoneAggregate>();
        if (metadata.Length < 1 || metadata[0] != MetadataVersion)
            return false;

        var span = new ReadOnlySpan<byte>(metadata, 1, metadata.Length - 1);
        var list = new List<ZoneAggregate>();
        long len = 0;
        int pos = 0;
        while (pos < span.Length)
        {
            var tag = (ulong)Varint.ReadUnsigned(span, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (fieldNum == 1 && wireType == 0)
            {
                len = Varint.ReadUnsigned(span, ref pos);
            }
            else if (fieldNum == 2 && wireType == 2)
            {
                var msgLen = checked((int)Varint.ReadUnsigned(span, ref pos));
                list.Add(ParseSpec(span.Slice(pos, msgLen)));
                pos += msgLen;
            }
            else
            {
                SkipField(span, ref pos, wireType);
            }
        }

        if (len is < 0 or > uint.MaxValue)
            throw new VortexFormatException($"vortex.zoned zone_len {len} is out of range.");
        zoneLen = checked((int)len);
        aggregates = list;
        return true;
    }

    /// <summary>
    /// Mirrors upstream's <c>aggregate_stats_table_dtype</c>. Returns null when an aggregate is
    /// unknown, since then the table's shape can't be known.
    /// </summary>
    public static StructType? BuildStructType(IArrowType columnDtype, IReadOnlyList<ZoneAggregate> aggregates)
    {
        var fields = new List<Field>(aggregates.Count);
        foreach (var agg in aggregates)
        {
            if (!TryStateDType(agg, columnDtype, out var stateDtype))
                return null;
            if (stateDtype is not null)
                fields.Add(new Field(DisplayName(agg), stateDtype, nullable: true));
        }
        return new StructType(fields);
    }

    /// <summary>
    /// Projects a decoded zones table onto <see cref="ZoneStats"/>. Must be given the struct that
    /// <see cref="BuildStructType"/> described.
    /// </summary>
    public static ZoneStats FromStruct(StructArray zonesStruct, IArrowType columnDtype, ZoneInfo zoneInfo)
    {
        IArrowArray? min = null, max = null;
        BooleanArray? minInexact = null, maxInexact = null;
        UInt64Array? nullCount = null, nanCount = null;
        var present = new SortedSet<Stat>();
        int zoneCount = zoneInfo.ZoneCount;

        int idx = 0;
        foreach (var agg in zoneInfo.Aggregates!)
        {
            if (!TryStateDType(agg, columnDtype, out var stateDtype) || stateDtype is null)
                continue;
            var field = zonesStruct.Fields[idx++];
            switch (agg.Id)
            {
                case Max:
                    // Min and max that count NaN are poisoned by it, which the evaluator's bounds
                    // model has no place for. Only the NaN-skipping form is usable.
                    if (max is null && SkipsNans(agg, columnDtype))
                    {
                        max = field;
                        maxInexact = null;
                        present.Add(Stat.Max);
                    }
                    break;
                case Min:
                    if (min is null && SkipsNans(agg, columnDtype))
                    {
                        min = field;
                        minInexact = null;
                        present.Add(Stat.Min);
                    }
                    break;
                case BoundedMax:
                    if (max is null)
                    {
                        max = BoundedMaxBound((StructArray)field, zoneCount);
                        // An upper bound that was truncated is indistinguishable from an exact one.
                        maxInexact = Constant(true, zoneCount);
                        present.Add(Stat.Max);
                    }
                    break;
                case BoundedMin:
                    if (min is null)
                    {
                        min = field;
                        minInexact = BoundedMinInexact(field, BoundedMaxBytes(agg), zoneCount);
                        present.Add(Stat.Min);
                    }
                    break;
                case NullCount:
                    nullCount ??= (UInt64Array)field;
                    present.Add(Stat.NullCount);
                    break;
                case NanCount:
                    nanCount ??= (UInt64Array)field;
                    present.Add(Stat.NaNCount);
                    break;
            }
        }

        return new ZoneStats(
            zoneInfo.ZoneLen, zoneCount, present.ToList(),
            min, max, minInexact, maxInexact, sum: null,
            nullCount, nanCount, uncompressedSize: null,
            isConstant: null, isSorted: null, isStrictSorted: null);
    }

    /// <summary>
    /// The partial-state dtype of <paramref name="agg"/> over <paramref name="columnDtype"/>, or
    /// null when the aggregate doesn't support that dtype (upstream then omits the field). Returns
    /// false for an aggregate this reader doesn't know.
    /// </summary>
    private static bool TryStateDType(ZoneAggregate agg, IArrowType columnDtype, out IArrowType? stateDtype)
    {
        switch (agg.Id)
        {
            case Max:
            case Min:
            case BoundedMin:
                stateDtype = MinMaxSupported(columnDtype) ? columnDtype : null;
                return true;
            case BoundedMax:
                stateDtype = MinMaxSupported(columnDtype)
                    ? new StructType(new[]
                    {
                        new Field("bound", columnDtype, nullable: true),
                        new Field("unknown", BooleanType.Default, nullable: false),
                    })
                    : null;
                return true;
            case NullCount:
                stateDtype = UInt64Type.Default;
                return true;
            case NanCount:
                stateDtype = columnDtype is HalfFloatType or FloatType or DoubleType
                    ? UInt64Type.Default
                    : null;
                return true;
            default:
                stateDtype = null;
                return false;
        }
    }

    /// <summary>Mirrors upstream's <c>minmax_supported_dtype</c> for the Arrow types a column can have.</summary>
    private static bool MinMaxSupported(IArrowType dtype) => dtype switch
    {
        NullType or StructType or MapType => false,
        ListType l => MinMaxSupported(l.ValueDataType),
        LargeListType l => MinMaxSupported(l.ValueDataType),
        FixedSizeListType l => MinMaxSupported(l.ValueDataType),
        _ => true,
    };

    /// <summary>Upstream names each zones-table field with the aggregate's display form, <c>id(options)</c>.</summary>
    private static string DisplayName(ZoneAggregate agg)
    {
        string options = agg.Id switch
        {
            Max or Min => ParseSkipNans(agg.Options) ? "" : "skip_nans=false",
            BoundedMax or BoundedMin => BoundedMaxBytes(agg).ToString(System.Globalization.CultureInfo.InvariantCulture),
            _ => "",
        };
        return $"{agg.Id}({options})";
    }

    private static bool SkipsNans(ZoneAggregate agg, IArrowType columnDtype) =>
        columnDtype is not (HalfFloatType or FloatType or DoubleType) || ParseSkipNans(agg.Options);

    /// <summary>Reads <c>NumericalAggregateOpts { 1: bool skip_nans }</c>; proto3 omits a false field.</summary>
    private static bool ParseSkipNans(byte[] options)
    {
        var span = options.AsSpan();
        bool skipNans = false;
        int pos = 0;
        while (pos < span.Length)
        {
            var tag = (ulong)Varint.ReadUnsigned(span, ref pos);
            if (tag >> 3 == 1 && (tag & 0x7) == 0)
                skipNans = Varint.ReadUnsigned(span, ref pos) != 0;
            else
                SkipField(span, ref pos, tag & 0x7);
        }
        return skipNans;
    }

    /// <summary>Bounded aggregates serialize <c>max_bytes</c> as a little-endian u64.</summary>
    private static ulong BoundedMaxBytes(ZoneAggregate agg)
    {
        if (agg.Options.Length != 8)
            throw new VortexFormatException(
                $"{agg.Id} options must be 8 bytes, got {agg.Options.Length}.");
        ulong value = 0;
        for (int i = 7; i >= 0; i--)
            value = (value << 8) | agg.Options[i];
        return value;
    }

    /// <summary>
    /// Upstream truncates a lower bound only when the value is longer than <c>max_bytes</c>, and
    /// cuts UTF-8 at a character boundary at most 3 bytes short of it. A zone's bound shorter than
    /// that was therefore never truncated, and as the least of its chunks' bounds it is the zone's
    /// true minimum. Anything longer may be a truncated prefix.
    /// </summary>
    private static BooleanArray BoundedMinInexact(IArrowArray bound, ulong maxBytes, int zoneCount)
    {
        var builder = new BooleanArray.Builder();
        builder.Reserve(zoneCount);
        for (int z = 0; z < zoneCount; z++)
        {
            int? length = bound switch
            {
                StringArray s when s.IsValid(z) => s.GetValueLength(z),
                BinaryArray b when b.IsValid(z) => b.GetValueLength(z),
                _ => null,
            };
            builder.Append(length is null || (ulong)length.Value + 3 >= maxBytes);
        }
        return builder.Build();
    }

    private static BooleanArray Constant(bool value, int length)
    {
        var builder = new BooleanArray.Builder();
        builder.Reserve(length);
        for (int i = 0; i < length; i++) builder.Append(value);
        return builder.Build();
    }

    /// <summary>
    /// The upper bound of each zone from a <c>vortex.bounded_max</c> partial
    /// <c>{ bound, unknown }</c>: null where the partial is null (an empty zone) or
    /// <c>unknown</c> is set (no upper bound fits the byte limit). Upstream writes a
    /// null bound under <c>unknown</c>, but its reader ignores the bound there
    /// either way, and so does this one.
    /// </summary>
    private static IArrowArray BoundedMaxBound(StructArray partial, int length)
    {
        var bound = partial.Fields[0];
        var unknown = (BooleanArray)partial.Fields[1];

        bool Valid(int i) => partial.IsValid(i) && bound.IsValid(i) && unknown.GetValue(i) != true;

        int nulls = 0;
        for (int i = 0; i < length; i++)
            if (!Valid(i)) nulls++;
        if (nulls == bound.NullCount)
            return bound;

        var data = bound.Data;
        var bitmap = new ArrowBuffer.BitmapBuilder(data.Offset + length);
        for (int i = 0; i < data.Offset; i++)
            bitmap.Append(false);
        for (int i = 0; i < length; i++)
            bitmap.Append(Valid(i));

        var buffers = data.Buffers.ToArray();
        buffers[0] = bitmap.Build();
        return ArrowArrayFactory.BuildArray(new ArrayData(
            data.DataType, data.Length, nulls, data.Offset, buffers, data.Children, data.Dictionary));
    }

    private static ZoneAggregate ParseSpec(ReadOnlySpan<byte> span)
    {
        string id = "";
        byte[] options = System.Array.Empty<byte>();
        int pos = 0;
        while (pos < span.Length)
        {
            var tag = (ulong)Varint.ReadUnsigned(span, ref pos);
            var fieldNum = tag >> 3;
            var wireType = tag & 0x7;
            if (wireType == 2 && fieldNum is 1 or 2)
            {
                var len = checked((int)Varint.ReadUnsigned(span, ref pos));
                var bytes = span.Slice(pos, len).ToArray();
                if (fieldNum == 1) id = Encoding.UTF8.GetString(bytes);
                else options = bytes;
                pos += len;
            }
            else
            {
                SkipField(span, ref pos, wireType);
            }
        }
        return new ZoneAggregate(id, options);
    }

    private static void SkipField(ReadOnlySpan<byte> span, ref int pos, ulong wireType) =>
        Encodings.ProtobufWire.SkipField(span, ref pos, wireType, "vortex.zoned metadata");
}
