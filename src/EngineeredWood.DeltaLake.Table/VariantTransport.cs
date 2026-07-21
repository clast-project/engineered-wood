// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Converts between the variant TRANSPORT form — one self-delimiting binary per row (the parquet-variant
/// metadata bytes immediately followed by the value bytes), marked with
/// <see cref="SchemaConverter.VariantTransportExtensionName"/> field metadata — and the canonical
/// <see cref="VariantArray"/> (<c>arrow.parquet.variant</c> over <c>struct&lt;metadata, value&gt;</c>).
/// The variant metadata header carries its own size, so the transport splits without a length prefix.
/// Embedding hosts whose Arrow boundary cannot carry an extension type over struct storage exchange
/// variant values in this LEAF-binary form: the WRITE side converts marked blob columns before the
/// built-in parquet codec (marker-keyed — a no-op for canonical input), and the READ side converts the
/// pipeline's output back to blobs when <c>DeltaTableOptions.VariantTransportBlob</c> is set.
///
/// <para><b>Shredding</b> (the VariantShredding spec) is handled through <c>Apache.Arrow.Operations</c>:
/// the WRITE side infers a per-file shredding schema from the column's values
/// (<see cref="ShredSchemaInferer"/>) and, when one applies, shreds each row into typed columns plus a
/// residual (<see cref="VariantShredder"/> — the shared metadata dictionary keeps residual field-name
/// references valid); the READ side reassembles ANY spec layout — unshredded, partially or fully shredded,
/// ours or a foreign writer's — via <see cref="VariantArrayShreddingExtensions.GetLogicalVariantValue"/>.
/// SQL NULL rows ride the storage struct's validity (distinct from a variant JSON null).</para>
/// </summary>
internal static class VariantTransport
{
    /// <summary>True when the Delta field is a <c>variant</c> primitive.</summary>
    internal static bool IsVariantField(StructField field) =>
        field.Type is PrimitiveType p && string.Equals(p.TypeName, "variant", StringComparison.Ordinal);

    /// <summary>
    /// The metadata prefix length of a concatenated variant blob (header byte ++ dictionary_size ++
    /// offsets ++ dictionary bytes — every piece sized by the header's offset_size, so the prefix is
    /// self-delimiting per the Variant binary spec v1).
    /// </summary>
    internal static int MetadataLength(ReadOnlySpan<byte> blob)
    {
        if (blob.Length < 3)
            throw new DeltaFormatException($"variant transport blob too short ({blob.Length} bytes).");
        byte header = blob[0];
        int version = header & 0x0F;
        if (version != 1)
            throw new DeltaFormatException($"unsupported variant metadata version {version}.");
        int offsetSize = ((header >> 6) & 0x3) + 1;
        long dictSize = ReadLittleEndian(blob, 1, offsetSize);
        int offsetsStart = 1 + offsetSize;
        long lastOffset = ReadLittleEndian(blob, offsetsStart + (int)dictSize * offsetSize, offsetSize);
        long total = offsetsStart + (dictSize + 1) * offsetSize + lastOffset;
        if (total <= 0 || total >= blob.Length)
            throw new DeltaFormatException("variant transport blob has a malformed metadata prefix.");
        return (int)total;
    }

    private static long ReadLittleEndian(ReadOnlySpan<byte> blob, int offset, int size)
    {
        if (offset + size > blob.Length)
            throw new DeltaFormatException("variant transport blob truncated inside the metadata prefix.");
        long v = 0;
        for (int i = 0; i < size; i++)
            v |= (long)blob[offset + i] << (8 * i);
        return v;
    }

    /// <summary>
    /// WRITE direction: replaces every transport-marked binary column in <paramref name="batch"/> with a
    /// <see cref="VariantArray"/> (splitting each blob into its metadata/value halves), so the built-in
    /// parquet writer emits the VARIANT-annotated group. Marker-keyed (works on logical- and
    /// physical-named batches alike); a no-op for canonical input or when the batch carries no variant
    /// column.
    /// </summary>
    internal static RecordBatch ToVariantArrays(RecordBatch batch)
    {
        List<Field>? fields = null;
        List<IArrowArray>? arrays = null;
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var f = batch.Schema.FieldsList[c];
            if (!SchemaConverter.IsVariantTransportField(f) || batch.Column(c) is not BinaryArray blob)
                continue;
            if (fields is null)
            {
                fields = new List<Field>(batch.Schema.FieldsList);
                arrays = new List<IArrowArray>(batch.ColumnCount);
                for (int i = 0; i < batch.ColumnCount; i++)
                    arrays.Add(batch.Column(i));
            }
            var variant = BuildVariantColumn(blob);
            // Keep the field metadata (column-mapping physicalName/PARQUET:field_id survive) — only the
            // type changes to the extension; the transport marker is harmless alongside it.
            fields[c] = new Field(f.Name, variant.Data.DataType, f.IsNullable, f.Metadata);
            arrays![c] = variant;
        }
        if (fields is null)
            return batch;
        var sb = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            sb.Field(f);
        return new RecordBatch(sb.Build(), arrays!, batch.Length);
    }

    /// <summary>
    /// READ direction (selected by <c>DeltaTableOptions.VariantTransportBlob</c>): converts each column
    /// whose DELTA type is <c>variant</c> back to the transport blob, tagging the field with the marker.
    /// Handles every shape the pipeline can deliver: a <see cref="VariantArray"/> (the built-in reader's
    /// registry wrapped an annotated group — possibly SHREDDED, reassembled per row), a bare
    /// <see cref="StructArray"/> (an unannotated spec-minimal file), or an already-binary column
    /// (a pluggable host reader delivered the transport directly — passes through).
    /// </summary>
    internal static RecordBatch ToTransportBlobs(RecordBatch batch, StructType deltaSchema)
    {
        List<Field>? fields = null;
        List<IArrowArray>? arrays = null;
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var f = batch.Schema.FieldsList[c];
            StructField? deltaField = null;
            foreach (var df in deltaSchema.Fields)
            {
                if (string.Equals(df.Name, f.Name, StringComparison.Ordinal))
                {
                    deltaField = df;
                    break;
                }
            }
            if (deltaField is null || !IsVariantField(deltaField))
                continue;

            var column = batch.Column(c);
            if (column is BinaryArray)
            {
                // A host reader delivered the transport form directly — pass the DATA through, but make
                // sure the FIELD still carries the marker (an intermediate reconcile may have relabeled it).
                if (!SchemaConverter.IsVariantTransportField(f))
                {
                    if (fields is null)
                    {
                        fields = new List<Field>(batch.Schema.FieldsList);
                        arrays = new List<IArrowArray>(batch.ColumnCount);
                        for (int i = 0; i < batch.ColumnCount; i++)
                            arrays.Add(batch.Column(i));
                    }
                    var meta = new Dictionary<string, string>
                    {
                        ["ARROW:extension:name"] = SchemaConverter.VariantTransportExtensionName,
                    };
                    if (f.Metadata is { } passSrc)
                    {
                        foreach (var kv in passSrc)
                            meta[kv.Key] = kv.Value;
                    }
                    fields[c] = new Field(f.Name, Apache.Arrow.Types.BinaryType.Default, f.IsNullable, meta);
                }
                continue;
            }

            var st = column switch
            {
                VariantArray va => va.Storage as StructArray,
                StructArray s => s,
                _ => null,
            };
            if (st is null)
            {
                throw new DeltaFormatException(
                    $"column '{f.Name}' is declared variant but materialised as {column.GetType().Name}, "
                    + "which is neither VariantArray, struct-of-binary, nor the binary transport form.");
            }

            var structType = (Apache.Arrow.Types.StructType)st.Data.DataType;
            bool shredded = structType.GetFieldIndex("typed_value") >= 0
                            || structType.GetFieldIndex("value") < 0;
            int off = st.Data.Offset; // struct children do NOT incorporate the parent's offset
            var builder = new BinaryArray.Builder();
            if (shredded)
            {
                // Shredded layout (typed_value present, or a fully-shredded file without a value column):
                // reassemble each row into logical variant bytes via Apache.Arrow.Operations (merges typed
                // columns + residual bytes per the VariantShredding spec, ours or a foreign writer's).
                if (off != 0)
                {
                    throw new DeltaFormatException(
                        $"column '{f.Name}': shredded variant reassembly over an offset struct slice is "
                        + "not supported (fresh reader batches are never sliced).");
                }
                var shreddedVariant = column as VariantArray
                    ?? new VariantArray(ArrowArrayFactory.BuildArray(st.Data));
                var encoder = new VariantBuilder();
                for (int r = 0; r < st.Length; r++)
                {
                    if (st.IsNull(r))
                    {
                        builder.AppendNull();
                        continue;
                    }
                    var logical = shreddedVariant.GetLogicalVariantValue(r);
                    var (em, ev) = encoder.Encode(logical);
                    var combined = new byte[em.Length + ev.Length];
                    em.CopyTo(combined, 0);
                    ev.CopyTo(combined, em.Length);
                    builder.Append(combined.AsSpan());
                }
            }
            else
            {
                BinaryArray? meta = null, val = null;
                for (int i = 0; i < structType.Fields.Count; i++)
                {
                    string name = structType.Fields[i].Name;
                    var child = ArrowArrayFactory.BuildArray(st.Data.Children[i]) as BinaryArray;
                    if (string.Equals(name, "metadata", StringComparison.Ordinal))
                        meta = child;
                    else if (string.Equals(name, "value", StringComparison.Ordinal))
                        val = child;
                }
                if (meta is null || val is null)
                {
                    throw new DeltaFormatException(
                        $"column '{f.Name}' is annotated VARIANT but lacks binary metadata/value children.");
                }

                for (int r = 0; r < st.Length; r++)
                {
                    if (st.IsNull(r) || meta.IsNull(off + r) || val.IsNull(off + r))
                    {
                        builder.AppendNull();
                        continue;
                    }
                    var m = meta.GetBytes(off + r);
                    var v = val.GetBytes(off + r);
                    var combined = new byte[m.Length + v.Length];
                    m.CopyTo(combined);
                    v.CopyTo(combined.AsSpan(m.Length));
                    builder.Append(combined.AsSpan());
                }
            }

            if (fields is null)
            {
                fields = new List<Field>(batch.Schema.FieldsList);
                arrays = new List<IArrowArray>(batch.ColumnCount);
                for (int i = 0; i < batch.ColumnCount; i++)
                    arrays.Add(batch.Column(i));
            }
            var tagged = new Dictionary<string, string>
            {
                ["ARROW:extension:name"] = SchemaConverter.VariantTransportExtensionName,
            };
            if (f.Metadata is { } src)
            {
                foreach (var kv in src)
                    tagged[kv.Key] = kv.Value;
            }
            fields[c] = new Field(f.Name, Apache.Arrow.Types.BinaryType.Default, f.IsNullable, tagged);
            arrays![c] = builder.Build();
        }
        if (fields is null)
            return batch;
        var sb = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            sb.Field(f);
        return new RecordBatch(sb.Build(), arrays!, batch.Length);
    }

    /// <summary>
    /// Builds the codec-facing variant column from a transport-blob column. The column's values are parsed
    /// once; when the inferred shredding schema applies (uniform objects/primitives/arrays per
    /// <see cref="ShredSchemaInferer"/>), the rows are shredded into typed columns + residuals — data
    /// skipping on shredded leaves is the variant "superpower" spec readers (Spark, DuckDB) exploit.
    /// Mixed-shape columns stay unshredded. SQL NULL rows become null STORAGE rows (validity), which is
    /// distinct from a variant JSON null riding in the value bytes.
    /// </summary>
    private static VariantArray BuildVariantColumn(BinaryArray blob)
    {
        int n = blob.Length;
        var values = new VariantValue[n];
        var isNull = new bool[n];
        var nonNull = new List<VariantValue>(n);
        for (int r = 0; r < n; r++)
        {
            if (blob.IsNull(r))
            {
                isNull[r] = true;
                values[r] = VariantValue.Null; // placeholder; masked by validity below
                continue;
            }
            var bytes = blob.GetBytes(r);
            int metaLen = MetadataLength(bytes);
            var reader = new VariantReader(bytes.Slice(0, metaLen), bytes.Slice(metaLen));
            values[r] = reader.ToVariantValue();
            nonNull.Add(values[r]);
        }

        var schema = nonNull.Count > 0
            ? new ShredSchemaInferer().Infer(nonNull)
            : ShredSchema.Unshredded();

        if (schema.TypedValueType == ShredType.None)
        {
            // Unshredded: pass the original bytes through untouched (no re-encode).
            var builder = new VariantArray.Builder();
            for (int r = 0; r < n; r++)
            {
                if (isNull[r])
                {
                    builder.AppendNull();
                    continue;
                }
                var bytes = blob.GetBytes(r);
                int metaLen = MetadataLength(bytes);
                builder.Append(bytes.Slice(0, metaLen), bytes.Slice(metaLen));
            }
            return builder.Build();
        }

        var (metadata, rows) = VariantShredder.Shred(values, schema);
        var variant = ShreddedVariantArrayBuilder.Build(schema, metadata, rows);
        // Re-apply the SQL-null rows as STORAGE validity (the shredder saw a variant-null placeholder).
        int nulls = 0;
        foreach (var b in isNull)
        {
            if (b) { nulls++; }
        }
        if (nulls > 0)
        {
            variant = (VariantArray)WithValidity(variant, isNull, nulls);
        }
        return variant;
    }

    // Rebuilds the extension array's storage struct with a validity bitmap marking the SQL-null rows
    // (buffers and children shared — only the top-level validity changes).
    private static IArrowArray WithValidity(VariantArray variant, bool[] isNull, int nullCount)
    {
        var storage = variant.StorageArray.Data;
        var validity = new ArrowBuffer.BitmapBuilder(isNull.Length);
        foreach (var b in isNull)
        {
            validity.Append(!b);
        }
        var newStorage = new ArrayData(
            storage.DataType, storage.Length, nullCount, storage.Offset,
            new[] { validity.Build() }, storage.Children, storage.Dictionary);
        return new VariantArray(variant.VariantType, ArrowArrayFactory.BuildArray(newStorage));
    }
}
