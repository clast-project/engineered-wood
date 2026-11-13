// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table;

public sealed partial class DeltaTable
{
    /// <summary>What reshaping adds (<see cref="ReconcileAddedBytesPerRow"/>): per row of the batch, and per row the
    /// read decodes, which is what it adds per element of a list or map
    /// (<see cref="EngineeredWood.Scans.ScanReservation.AddedBytesPerDecodedRow"/>).</summary>
    internal readonly record struct ReshapeBytes(long PerRow, long PerDecodedRow);

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
    /// Each in the representation the read returns (<paramref name="byteArrays"/>), and at every depth, as the reshaping
    /// recurses (#852 review round 2): a field added to a struct the file holds, or widened inside one, adds its bytes
    /// per struct row; inside a list or map element, per element, taken as <paramref name="leafValuesPerRow"/> of the
    /// file's top-level column (the most leaf values a row holds, which bounds its elements at any depth; one when
    /// unknown). A column the file holds unchanged, or one dropped from the table, adds nothing.
    /// <para>The reshaping rebuilds a nested column over the batch's own rows (<see cref="NestedWindow"/>, #853), even
    /// where the batch is sliced from a larger decode, so what it adds is the batch's own. The validity bits and offsets
    /// that rebuild copies stand in for the raw batch's, so they add nothing. What it adds per element of a list or map
    /// is per row DECODED, though: the elements per row are a row group's average, and a batch may hold far more than
    /// its share of them (#854 review). A batch holds no more than the whole row group's, which that bounds.</para>
    /// <para>The file's columns are matched in the names the read gives them: its schema is renamed by the read's own
    /// rule (<see cref="ToLogicalBatch"/>), physical names or field ids to logical, nested ones included.</para>
    /// </summary>
    internal static ReshapeBytes ReconcileAddedBytesPerRow(
        Snapshot.Snapshot snapshot, IReadOnlyList<string>? columns, IReadOnlyDictionary<string, string>? partitionValues,
        IReadOnlyList<string> partitionColumns, bool hasPartitions, ColumnMappingMode mappingMode,
        IReadOnlyDictionary<string, string> logicalToPhysical, Apache.Arrow.Schema fileSchema,
        ByteArrayOutputKind? byteArrays, IReadOnlyDictionary<string, double>? leafValuesPerRow = null)
    {
        var expected = columns is not null ? BuildProjectedSchema(snapshot.ArrowSchema, columns) : snapshot.ArrowSchema;
        var held = LogicalFields(snapshot, mappingMode, logicalToPhysical, fileSchema);
        bool widens = ReadsWiden(snapshot);
        // A byte array column that reshaping builds and the representation then converts is rebuilt by a builder,
        // whose validity bitmap, where the column has nulls, is allocated with room to grow: up to twice its bits.
        bool converted = byteArrays is { } k && k != ByteArrayOutputKind.Default;

        double bits = 0, decodedBits = 0;
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

            if (!held.TryGetValue(field.Name, out var heldField))
            {
                bits += NullBitsPerRow(type, converted);
                continue;
            }
            double elements = 1;
            if (leafValuesPerRow is not null
                && leafValuesPerRow.TryGetValue(
                    logicalToPhysical.TryGetValue(field.Name, out var physical) ? physical : field.Name, out var perRow))
            {
                elements = Math.Max(1, perRow);
            }
            // The part under a list or map: what the column adds with its elements counted, less what it adds with
            // none (EvolvedBits counts what is inside an element per element).
            double evolved = EvolvedBits(type, heldField.DataType, converted, widens, rowsPerRow: 1, elements);
            double flat = EvolvedBits(type, heldField.DataType, converted, widens, rowsPerRow: 1, elements: 0);
            bits += flat;
            decodedBits += evolved - flat;
        }
        return new ReshapeBytes(Bytes(bits), Bytes(decodedBits));

        static long Bytes(double bits)
        {
            double bytes = Math.Ceiling(bits / 8);
            return bytes >= EngineeredWood.Memory.MemoryBudget.MaxCharge
                ? EngineeredWood.Memory.MemoryBudget.MaxCharge
                : (long)bytes;
        }
    }

    /// <summary>A <see cref="ReshapeBytes"/> a reference can publish whole to the units that share a file.</summary>
    private sealed record ReshapeBytesBox(ReshapeBytes Value);

    /// <summary>
    /// The file's top-level fields by the names the read gives them: its schema renamed as a batch of it is
    /// (<see cref="ToLogicalBatch"/>, over a batch of no rows). A schema the rename refuses is matched as stored, by
    /// physical name or, in id mode, field id, with its nested fields left as stored.
    /// </summary>
    private static Dictionary<string, Field> LogicalFields(
        Snapshot.Snapshot snapshot, ColumnMappingMode mappingMode, IReadOnlyDictionary<string, string> logicalToPhysical,
        Apache.Arrow.Schema fileSchema)
    {
        var physicalToLogical = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in logicalToPhysical)
            physicalToLogical[pair.Value] = pair.Key;
        var result = new Dictionary<string, Field>(StringComparer.Ordinal);
        try
        {
            using var empty = new RecordBatch(
                fileSchema, fileSchema.FieldsList.Select(f => EngineeredWood.Arrow.ArrowCompute.MakeNullArray(f.DataType, 0)), 0);
            var logical = ToLogicalBatch(empty, snapshot.Schema, mappingMode, physicalToLogical);
            foreach (var field in logical.Schema.FieldsList)
                result[field.Name] = field;
            return result;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // An estimate never fails a read: matched as stored instead.
        }

        Dictionary<int, Field>? byId = null;
        if (mappingMode == ColumnMappingMode.Id)
        {
            byId = new();
            foreach (var field in fileSchema.FieldsList)
            {
                if (ColumnMapping.GetParquetFieldId(field) is int id)
                    byId[id] = field;
            }
        }
        foreach (var delta in snapshot.Schema.Fields)
        {
            Field? field = null;
            if (byId is not null)
            {
                if (ColumnMapping.GetFieldId(delta) is int id)
                    byId.TryGetValue(id, out field);
            }
            else
            {
                field = fileSchema.FieldsList.FirstOrDefault(f => f.Name == (
                    logicalToPhysical.TryGetValue(delta.Name, out var physical) ? physical : delta.Name));
            }
            if (field is not null)
                result[delta.Name] = field;
        }
        return result;
    }

    /// <summary>
    /// Bits per row that reshaping adds to a column the file holds as <paramref name="held"/> to give it the type
    /// <paramref name="expected"/>, as <see cref="SchemaEvolution.BackfillMissingColumns"/> and the widener rebuild it:
    /// a struct's missing fields as NULLs and widened leaves at their wider width, at <paramref name="rowsPerRow"/>
    /// entries a row (one at the top, <paramref name="elements"/> inside a list or map element).
    /// </summary>
    private static double EvolvedBits(
        IArrowType expected, IArrowType held, bool converted, bool widens, double rowsPerRow, double elements)
    {
        while (expected is ExtensionType e)
            expected = e.StorageType;
        while (held is ExtensionType h)
            held = h.StorageType;
        switch (expected, held)
        {
            // MapType is not a ListType in Apache.Arrow (pinned by MapType_IsNotAListType_SoTheWalkArmOrderIsIrrelevant), so the arm order is free.
            case (Apache.Arrow.Types.MapType em, Apache.Arrow.Types.MapType hm):
                return EvolvedBits(em.KeyField.DataType, hm.KeyField.DataType, converted, widens, elements, elements)
                    + EvolvedBits(em.ValueField.DataType, hm.ValueField.DataType, converted, widens, elements, elements);
            case (ListType el, ListType hl):
                return EvolvedBits(el.ValueDataType, hl.ValueDataType, converted, widens, elements, elements);
            case (LargeListType el, LargeListType hl):
                return EvolvedBits(el.ValueDataType, hl.ValueDataType, converted, widens, elements, elements);
            case (FixedSizeListType el, FixedSizeListType hl):
                return EvolvedBits(el.ValueDataType, hl.ValueDataType, converted, widens, rowsPerRow * el.ListSize, elements);
            case (Apache.Arrow.Types.StructType es, Apache.Arrow.Types.StructType hs):
            {
                double bits = 0;
                foreach (var child in es.Fields)
                {
                    var match = hs.Fields.FirstOrDefault(f => string.Equals(f.Name, child.Name, StringComparison.Ordinal));
                    bits += match is null
                        ? rowsPerRow * NullBitsPerRow(child.DataType, converted)
                        : EvolvedBits(child.DataType, match.DataType, converted, widens, rowsPerRow, elements);
                }
                return bits;
            }
            default:
                return widens ? rowsPerRow * Math.Max(0, FixedBits(expected) - FixedBits(held)) : 0;
        }
    }

    /// <summary><see cref="ReconcileAddedBytesPerRow"/> for one data file's read through the built-in reader; 0 through
    /// the codec seam, whose host builds the batch.</summary>
    private static async ValueTask<ReshapeBytes> ReconcileAddedBytesPerRowAsync(
        DataFileRead read, ByteArrayOutputKind? byteArrays, CancellationToken cancellationToken)
    {
        if (read.Reader is not { } reader)
            return default;
        var fileSchema = await reader.GetArrowSchemaAsync(cancellationToken).ConfigureAwait(false);
        return ReconcileAddedBytesPerRow(
            read.Snapshot, read.Columns, read.AddFile.PartitionValues, read.PartitionColumns, read.HasPartitions,
            read.MappingMode, read.LogicalToPhysical, fileSchema, byteArrays,
            await reader.LeafValuesPerRowAsync(cancellationToken).ConfigureAwait(false));
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
