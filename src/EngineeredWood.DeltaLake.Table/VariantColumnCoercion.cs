// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Materialises variant columns from the Delta SCHEMA rather than the parquet annotation.
/// </summary>
/// <remarks>
/// <para>The Delta spec (Reader Requirements for Variant Data Type) says a reader "must recognize and
/// tolerate a <c>variant</c> data type in a Delta schema" and "must use the correct physical schema
/// (struct-of-binary, with fields <c>value</c> and <c>metadata</c>)" — i.e. the SCHEMA is the source of
/// truth, not the parquet file's logical-type annotation. The annotation is optional: Spark 4.0.x and
/// any spec-minimal writer omit it (Spark only started emitting it in 4.1), and EngineeredWood itself
/// omits it when <see cref="DeltaTableOptions.EmitVariantLogicalType"/> is false.</para>
///
/// <para>The parquet reader wraps a column as <see cref="VariantArray"/> only when the file carries the
/// annotation (and reassembles any shredding then). For an UNANNOTATED file the same column arrives as
/// a bare <see cref="StructArray"/>. This coercion closes that gap: for every top-level field the Delta
/// schema declares as <c>variant</c>, an unwrapped struct-of-binary is re-presented as a
/// <see cref="VariantArray"/> so downstream consumers see a uniform type regardless of how the file was
/// written.</para>
///
/// <para>Two things it is careful about:</para>
/// <list type="bullet">
/// <item>Child order. <see cref="VariantType"/>'s array factory is POSITIONAL (child 0 = metadata,
/// child 1 = value); it ignores field names. Writers disagree on order — EngineeredWood writes
/// <c>(metadata, value)</c>, Spark writes <c>(value, metadata)</c> — so the storage struct is reordered
/// BY NAME into the canonical <c>(metadata, value)</c> before wrapping, or the two binaries would be
/// silently swapped.</item>
/// <item>Scope. Top-level columns only, matching the parquet reader (which does not wrap variants nested
/// inside list/map). An already-wrapped <see cref="VariantArray"/> — the annotated-file case, possibly
/// shredding-reassembled — is left untouched.</item>
/// </list>
/// </remarks>
internal static class VariantColumnCoercion
{
    /// <summary>
    /// Returns <paramref name="batch"/> with each top-level column whose <paramref name="schema"/> field
    /// is <see cref="VariantType"/> presented as a <see cref="VariantArray"/>. Columns already wrapped, or
    /// whose schema field is not variant, pass through unchanged; the batch is rebuilt only if something
    /// actually needed wrapping.
    /// </summary>
    internal static RecordBatch Coerce(RecordBatch batch, Apache.Arrow.Schema schema)
    {
        IArrowArray[]? rewritten = null;

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            var field = schema.GetFieldByName(batch.Schema.FieldsList[i].Name);
            if (field?.DataType is not VariantType variantType)
                continue;

            var column = batch.Column(i);
            if (column is VariantArray)
                continue; // annotated file — the parquet reader already wrapped (and de-shredded) it

            if (column is not StructArray storage)
            {
                // The schema says variant but the file yielded neither a VariantArray nor a struct. This
                // should not happen for a spec-conforming file; fail loudly rather than emit a column that
                // silently contradicts the declared type.
                throw new InvalidOperationException(
                    $"Column '{batch.Schema.FieldsList[i].Name}' is declared variant but materialised as "
                    + $"{column.GetType().Name}, which is neither VariantArray nor the struct-of-binary "
                    + "physical layout the Delta spec requires.");
            }

            rewritten ??= CopyColumns(batch);
            rewritten[i] = variantType.CreateArray(ToCanonicalStorage(storage));
        }

        if (rewritten is null)
            return batch;

        return new RecordBatch(batch.Schema, rewritten, batch.Length);
    }

    /// <summary>
    /// Write-side inverse of <see cref="Coerce"/>: replaces each top-level <see cref="VariantArray"/>
    /// column with its bare storage <see cref="StructArray"/>, so the parquet writer emits a plain
    /// <c>struct&lt;metadata, value&gt;</c> group with NO <c>VARIANT</c> logical-type annotation. Used
    /// when <see cref="DeltaTableOptions.EmitVariantLogicalType"/> is false (Spark 4.0.x compatibility).
    /// The bytes are identical either way — only the parquet schema annotation differs — and the read
    /// path recovers the variant type from the Delta schema via <see cref="Coerce"/>.
    /// </summary>
    internal static RecordBatch StripAnnotation(RecordBatch batch)
    {
        IArrowArray[]? rewritten = null;
        Field[]? fields = null;

        for (int i = 0; i < batch.ColumnCount; i++)
        {
            if (batch.Column(i) is not VariantArray variant)
                continue;

            rewritten ??= CopyColumns(batch);
            fields ??= batch.Schema.FieldsList.ToArray();

            var storage = variant.Storage;
            rewritten[i] = storage;
            var original = fields[i];
            fields[i] = new Field(original.Name, storage.Data.DataType, original.IsNullable, original.Metadata);
        }

        if (rewritten is null)
            return batch;

        var schema = new Apache.Arrow.Schema(fields!, batch.Schema.Metadata);
        return new RecordBatch(schema, rewritten, batch.Length);
    }

    /// <summary>Reorders a variant storage struct to the canonical <c>(metadata, value)</c> field order
    /// the positional <see cref="VariantType"/> factory expects, resolving the two children BY NAME.</summary>
    private static StructArray ToCanonicalStorage(StructArray storage)
    {
        var type = (StructType)storage.Data.DataType;
        int metaIdx = IndexOf(type, "metadata");
        int valueIdx = IndexOf(type, "value");

        // Already canonical — the common EngineeredWood-written case; avoid a rebuild.
        if (metaIdx == 0 && valueIdx == 1 && type.Fields.Count == 2)
            return storage;

        var canonicalType = new StructType(new[] { type.Fields[metaIdx], type.Fields[valueIdx] });
        return new StructArray(
            canonicalType,
            storage.Length,
            new[] { storage.Fields[metaIdx], storage.Fields[valueIdx] },
            GetValidityBuffer(storage),
            storage.NullCount,
            storage.Offset);
    }

    private static int IndexOf(StructType type, string fieldName)
    {
        for (int i = 0; i < type.Fields.Count; i++)
        {
            if (type.Fields[i].Name == fieldName)
                return i;
        }
        throw new InvalidOperationException(
            $"Variant storage struct is missing the required '{fieldName}' field; it has "
            + $"[{string.Join(", ", System.Linq.Enumerable.Select(type.Fields, f => f.Name))}].");
    }

    private static ArrowBuffer GetValidityBuffer(StructArray storage) =>
        storage.Data.Buffers.Length > 0 ? storage.Data.Buffers[0] : ArrowBuffer.Empty;

    private static IArrowArray[] CopyColumns(RecordBatch batch)
    {
        var columns = new IArrowArray[batch.ColumnCount];
        for (int i = 0; i < batch.ColumnCount; i++)
            columns[i] = batch.Column(i);
        return columns;
    }
}
