// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table;

public sealed partial class DeltaTable
{
    /// <summary>
    /// The bytes per row that bringing a data file's batch to the table's shape (<see cref="ReconcileToTableShape"/>)
    /// adds to what the Parquet read decodes, for a first reservation to cover (#851,
    /// <see cref="EngineeredWood.Scans.ScanReservation.AddedBytesPerRow"/>). The scan settles the reshaped batch,
    /// so a reservation of the raw batch alone was short by these on every unit's first item: a scan's unit is a row
    /// group, so unbatched that was every item.
    /// <list type="bullet">
    /// <item>A partition column: its value tiled per row (<see cref="Partitioning.PartitionUtils"/>), or NULLs for a
    /// null value.</item>
    /// <item>A column the file predates: all NULL, at its type's full width (<see cref="EngineeredWood.Arrow.ArrowCompute.MakeNullArray"/>).</item>
    /// <item>A widened column: the wider type's width over the narrower one the file holds.</item>
    /// </list>
    /// Each in the representation the read returns (<paramref name="byteArrays"/>). Top-level columns only; a column
    /// the file holds, or one dropped from the table, adds nothing.
    /// </summary>
    internal static long ReconcileAddedBytesPerRow(
        Snapshot.Snapshot snapshot, IReadOnlyList<string>? columns, IReadOnlyDictionary<string, string>? partitionValues,
        IReadOnlyList<string> partitionColumns, bool hasPartitions, ColumnMappingMode mappingMode,
        IReadOnlyDictionary<string, string> logicalToPhysical, Apache.Arrow.Schema fileSchema,
        ByteArrayOutputKind? byteArrays)
    {
        var expected = columns is not null ? BuildProjectedSchema(snapshot.ArrowSchema, columns) : snapshot.ArrowSchema;
        var fileByName = new Dictionary<string, Field>(StringComparer.Ordinal);
        Dictionary<int, Field>? fileById = mappingMode == ColumnMappingMode.Id ? new() : null;
        foreach (var field in fileSchema.FieldsList)
        {
            fileByName[field.Name] = field;
            if (fileById is not null && ColumnMapping.GetParquetFieldId(field) is int id)
                fileById[id] = field;
        }
        bool widens = ReadsWiden(snapshot);
        // A byte array column that reshaping builds and the representation then converts is rebuilt by a builder,
        // whose validity bitmap, where the column has nulls, is allocated with room to grow: up to twice its bits.
        bool converted = byteArrays is { } k && k != ByteArrayOutputKind.Default;

        long bits = 0;
        foreach (var field in expected.FieldsList)
        {
            var type = byteArrays is { } kind ? ByteArrayRepresentation.Of(field.DataType, kind) : field.DataType;
            if (hasPartitions && partitionColumns.Contains(field.Name))
            {
                // Resolved as the reshaping resolves it: the map's spelling decided from all its keys, and the values
                // that read as NULL, the Hive sentinel for a column that is not a string included (#852 review).
                string? value = Partitioning.PartitionUtils.ConstantValueOf(
                    field, partitionValues, partitionColumns, logicalToPhysical);
                bits += value is null ? NullBitsPerRow(type, converted) : ConstantBitsPerRow(type, value);
                continue;
            }

            Field? held = null;
            if (fileById is not null)
            {
                if (snapshot.Schema.Fields.FirstOrDefault(f => f.Name == field.Name) is { } delta
                    && ColumnMapping.GetFieldId(delta) is int id)
                {
                    fileById.TryGetValue(id, out held);
                }
            }
            else
            {
                fileByName.TryGetValue(logicalToPhysical.TryGetValue(field.Name, out var physical) ? physical : field.Name, out held);
            }

            if (held is null)
                bits += NullBitsPerRow(type, converted);
            else if (widens)
                bits += Math.Max(0, FixedBits(type) - FixedBits(held.DataType));
        }
        return (bits + 7) / 8;
    }

    /// <summary><see cref="ReconcileAddedBytesPerRow"/> for one data file's read through the built-in reader; 0 through
    /// the codec seam, whose host builds the batch.</summary>
    private static async ValueTask<long> ReconcileAddedBytesPerRowAsync(
        DataFileRead read, ByteArrayOutputKind? byteArrays, CancellationToken cancellationToken)
    {
        if (read.Reader is not { } reader)
            return 0;
        var fileSchema = await reader.GetArrowSchemaAsync(cancellationToken).ConfigureAwait(false);
        return ReconcileAddedBytesPerRow(
            read.Snapshot, read.Columns, read.AddFile.PartitionValues, read.PartitionColumns, read.HasPartitions,
            read.MappingMode, read.LogicalToPhysical, fileSchema, byteArrays);
    }

    /// <summary>
    /// The bytes per row a scan's metadata columns add (<see cref="WithMetadataColumns"/>): a row address, a locator's
    /// path (tiled per row) and position, and a row id and commit version, either of which may be all NULL.
    /// </summary>
    private static long MetadataAddedBytesPerRow(ScanFileShape scan, byte[]? pathBytes)
    {
        long bits = 0;
        if (scan.WantAddress)
            bits += 64;
        if (scan.WantLocator)
            bits += 32 + 8L * (pathBytes?.Length ?? 0) + 64;
        if (scan.WantTracking)
            bits += 2 * (64 + 1);
        return (bits + 7) / 8;
    }

    /// <summary>Bits per row of a constant column of <paramref name="value"/> (<see cref="EngineeredWood.Arrow.ArrowCompute.Repeat"/>,
    /// which writes no validity): a byte array's offset or view and its bytes, else the type's width.</summary>
    private static long ConstantBitsPerRow(IArrowType type, string value)
    {
        long length = System.Text.Encoding.UTF8.GetByteCount(value);
        return type switch
        {
            ExtensionType extension => ConstantBitsPerRow(extension.StorageType, value),
            StringType or BinaryType => 32 + 8 * length,
            LargeStringType or LargeBinaryType => 64 + 8 * length,
            // A view holds up to 12 bytes inline, longer ones in a data buffer.
            StringViewType or BinaryViewType => 128 + (length > 12 ? 8 * length : 0),
            _ => FixedBits(type),
        };
    }

    /// <summary>Bits per row of an all-NULL column of <paramref name="type"/>, as
    /// <see cref="EngineeredWood.Arrow.ArrowCompute.MakeNullArray"/> builds it: a validity bit and zeroed values, offsets or views, with
    /// a struct's fields each built the same way and a list's children empty. A byte array <paramref name="converted"/>
    /// to another representation is rebuilt by a builder, whose validity has room to grow: two bits.</summary>
    private static long NullBitsPerRow(IArrowType type, bool converted)
    {
        long byteArrayValidity = converted ? 2 : 1;
        return type switch
        {
            ExtensionType extension => NullBitsPerRow(extension.StorageType, converted),
            NullType => 0,
            BooleanType => 2,
            StringType or BinaryType => byteArrayValidity + 32,
            LargeStringType or LargeBinaryType => byteArrayValidity + 64,
            StringViewType or BinaryViewType => byteArrayValidity + 128,
            ListType or Apache.Arrow.Types.MapType => 1 + 32,
            LargeListType => 1 + 64,
            Apache.Arrow.Types.StructType structType =>
                1 + structType.Fields.Sum(f => NullBitsPerRow(f.DataType, converted)),
            FixedSizeListType fixedList => 1 + fixedList.ListSize * NullBitsPerRow(fixedList.ValueDataType, converted),
            FixedWidthType fixedWidth => 1 + fixedWidth.BitWidth,
            _ => 1 + 64,
        };
    }

    private static long FixedBits(IArrowType type) => type switch
    {
        ExtensionType extension => FixedBits(extension.StorageType),
        FixedWidthType fixedWidth => fixedWidth.BitWidth,
        _ => 0,
    };
}
