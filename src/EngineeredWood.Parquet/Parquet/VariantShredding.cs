// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics.CodeAnalysis;

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;

namespace EngineeredWood.Parquet;

/// <summary>
/// Converts a parquet VARIANT column between its canonical unshredded form and the SHREDDED physical
/// layout — both directions of the VariantShredding spec, over <see cref="VariantArray"/>.
/// </summary>
/// <remarks>
/// <para>The parquet VARIANT spec allows a writer to "shred" a column: instead of carrying the whole
/// value in the <c>value</c> binary child, it hoists part or all of it into a typed
/// <c>typed_value</c> child so readers can push predicates down and skip decoding. Spark 4.x and
/// DuckDB both shred by default, so most variant columns encountered in the wild are shredded.</para>
///
/// <para><see cref="VariantArray"/> is a STORAGE-level view: on a shredded column its <c>value</c>
/// child is empty for shredded rows and <see cref="VariantArray.GetValueBytes"/> returns zero bytes
/// while <c>IsNull</c> reports false — a valid row holding an empty variant. Without
/// <see cref="Reassemble"/>, a caller would silently read empty values from every Spark-written
/// column. The spec mechanics themselves live in the companion <c>Apache.Arrow.Operations</c>
/// package — inference policy (<see cref="ShredOptions"/>), the shredder, the unshredder — and this
/// type is the ARRAY-LEVEL adapter over them: upstream works one <see cref="VariantValue"/> at a
/// time and has no notion of a SQL-null row, both of which a whole column needs.</para>
///
/// <para><b>Inference is per-call; the layout is per-file.</b> A shred schema describes a whole
/// parquet column, but every operation here sees only the values it is handed, and
/// <see cref="ParquetFileWriter"/> fixes a file's schema from its first row group. So a caller
/// writing several batches must infer ONCE — <see cref="InferSchema(VariantArray, ShredOptions)"/> —
/// and pass that schema to <see cref="Shred(VariantArray, ShredSchema)"/> for every batch;
/// re-inferring per batch can produce a later batch whose layout disagrees with the file's schema.
/// The <see cref="TryShred(VariantArray, out VariantArray)"/> pair does both steps at once and is for
/// the single-batch case. Rows that do not fit the schema are not an error: they fall to the residual
/// <c>value</c>, which is what it is for.</para>
/// </remarks>
public static class VariantShredding
{
    /// <summary>
    /// Returns <paramref name="array"/> unchanged when it is already unshredded; otherwise returns an
    /// equivalent unshredded <see cref="VariantArray"/> whose <c>value</c> child carries each row's
    /// fully reconstructed variant bytes. Null rows stay null.
    /// </summary>
    /// <remarks>
    /// The result is an ordinary unshredded <see cref="VariantArray"/> — values are correct and
    /// uniform, but the shredded layout is NOT preserved, so a caller cannot inspect
    /// <c>typed_value</c> afterwards. That trade is deliberate: the reader's contract is to
    /// materialise values the caller can read without taking a second dependency. If preserving the
    /// shredded layout is ever needed (e.g. to push predicates into <c>typed_value</c>), it should
    /// become an explicit opt-in on <see cref="ParquetReadOptions"/> rather than the default.
    /// </remarks>
    public static VariantArray Reassemble(VariantArray array)
    {
        if (!array.IsShredded)
        {
            return array;
        }

        var builder = new VariantArray.Builder();
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i))
            {
                builder.AppendNull();
                continue;
            }

            // GetLogicalVariantValue merges typed_value with any residual `value` bytes and returns
            // the canonical (metadata, value) pair for the row.
            builder.Append(array.GetLogicalVariantValue(i));
        }

        return builder.Build(allocator: null);
    }

    /// <summary>
    /// Infers the shredding schema that fits <paramref name="values"/>, or <see langword="null"/>
    /// when their shapes are too mixed to shred — or when every row is SQL null, leaving no shape to
    /// infer from.
    /// </summary>
    /// <param name="values">
    /// One decoded value per row. A row marked in <paramref name="isSqlNull"/> contributes nothing to
    /// the inferred schema, so a placeholder may sit there.
    /// </param>
    /// <param name="isSqlNull">Per-row SQL-null mask, or an empty span when no row is null.</param>
    /// <param name="options">
    /// Inference policy — the depth cap and the frequency/consistency thresholds a field must clear to
    /// be hoisted into <c>typed_value</c>. Defaults to <see cref="ShredOptions.Default"/>
    /// (<c>MaxDepth 3</c>, <c>MinFieldFrequency 0.5</c>, <c>MinTypeConsistency 0.8</c>). The values are
    /// read here and not retained, so a caller may reuse a mutable instance.
    /// </param>
    /// <remarks>
    /// Separating inference from
    /// <see cref="Shred(IReadOnlyList{VariantValue}, ReadOnlySpan{bool}, ShredSchema)"/> is what lets
    /// one schema span several batches — see the note on this type.
    /// </remarks>
    public static ShredSchema? InferSchema(
        IReadOnlyList<VariantValue> values,
        ReadOnlySpan<bool> isSqlNull,
        ShredOptions? options = null)
    {
        if (values is null)
        {
            throw new ArgumentNullException(nameof(values));
        }
        ValidateMask(isSqlNull, values.Count);

        var present = new List<VariantValue>(values.Count);
        for (int r = 0; r < values.Count; r++)
        {
            if (!isSqlNull.IsEmpty && isSqlNull[r])
            {
                continue;
            }
            present.Add(values[r]);
        }

        if (present.Count == 0)
        {
            return null; // an all-null column has no shape to infer from.
        }

        var schema = new ShredSchemaInferer().Infer(present, options ?? ShredOptions.Default);
        return schema.TypedValueType == ShredType.None ? null : schema;
    }

    /// <summary>
    /// Infers the shredding schema that fits <paramref name="array"/>'s logical values, or
    /// <see langword="null"/> when it has no shape to shred. Decodes every row; a caller that already
    /// holds decoded values should use the overload that takes them.
    /// </summary>
    public static ShredSchema? InferSchema(VariantArray array, ShredOptions? options = null)
    {
        if (array is null)
        {
            throw new ArgumentNullException(nameof(array));
        }

        var (values, isNull, anyNull) = Decode(array);
        return InferSchema(values, anyNull ? isNull : default, options);
    }

    /// <summary>
    /// Shreds <paramref name="values"/> into the layout described by <paramref name="schema"/>. A row
    /// that does not fit falls to the residual <c>value</c> rather than failing.
    /// </summary>
    /// <param name="values">
    /// One decoded value per row. A row marked in <paramref name="isSqlNull"/> is masked out of the
    /// result, but it is still handed to the shredder first, so the placeholder has to be a value the
    /// shredder accepts; <see cref="VariantValue.Null"/> is the obvious choice.
    /// </param>
    /// <param name="isSqlNull">
    /// Per-row SQL-null mask, or an empty span when no row is null. SQL null-ness rides the storage
    /// struct's VALIDITY and is deliberately distinct from a variant JSON null carried in the value
    /// bytes — the spec gives each encoding a meaning, and a caller that conflates them changes what
    /// <c>IS NULL</c> means for every consumer of the column.
    /// </param>
    /// <param name="schema">
    /// The layout to shred into, from
    /// <see cref="InferSchema(IReadOnlyList{VariantValue}, ReadOnlySpan{bool}, ShredOptions)"/> or
    /// built directly (<see cref="ShredSchema.ForObject"/> and friends) when the shape is known ahead
    /// of the data.
    /// </param>
    public static VariantArray Shred(
        IReadOnlyList<VariantValue> values,
        ReadOnlySpan<bool> isSqlNull,
        ShredSchema schema)
    {
        if (values is null)
        {
            throw new ArgumentNullException(nameof(values));
        }
        if (schema is null)
        {
            throw new ArgumentNullException(nameof(schema));
        }
        ValidateMask(isSqlNull, values.Count);

        var (metadata, rows) = VariantShredder.Shred(values, schema);
        var array = ShreddedVariantArrayBuilder.Build(schema, metadata, rows);

        int nulls = 0;
        for (int r = 0; r < isSqlNull.Length; r++)
        {
            if (isSqlNull[r]) nulls++;
        }

        // The shredder saw a placeholder at each SQL-null row; re-apply those rows as storage validity
        // so they read back as SQL NULL rather than as a shredded placeholder value. Upstream's shred
        // pipeline has no notion of a null row, which is why this step lives here.
        return nulls > 0 ? WithValidity(array, isSqlNull, nulls) : array;
    }

    /// <summary>
    /// Shreds <paramref name="array"/>'s logical values into the layout described by
    /// <paramref name="schema"/>, preserving which rows are SQL null. Reads the LOGICAL value per row,
    /// so an already-shredded input re-shreds into the new layout rather than losing its typed columns.
    /// </summary>
    public static VariantArray Shred(VariantArray array, ShredSchema schema)
    {
        if (array is null)
        {
            throw new ArgumentNullException(nameof(array));
        }

        var (values, isNull, anyNull) = Decode(array);
        return Shred(values, anyNull ? isNull : default, schema);
    }

    /// <summary>
    /// Infers a schema from <paramref name="values"/> and shreds them in one step, returning
    /// <see langword="false"/> when there is no shape to shred — in which case the caller should keep
    /// whatever unshredded representation it already holds. For a multi-batch write, infer once and
    /// call <see cref="Shred(IReadOnlyList{VariantValue}, ReadOnlySpan{bool}, ShredSchema)"/> instead.
    /// </summary>
    /// <remarks>
    /// <para>This overload takes ALREADY-DECODED values on purpose. A host that arrived with an
    /// encoded form (a blob column, an IPC payload) has to parse each row to decide anything about it,
    /// so handing us the parsed values keeps the decode at ONE per row; a <see cref="VariantArray"/>-
    /// only entry point would force it to encode a canonical array first and have us decode it
    /// again.</para>
    /// <para>Returning false rather than an unshredded array is likewise deliberate: building one
    /// would re-encode every row, discarding bytes the caller may already have.</para>
    /// </remarks>
    public static bool TryShred(
        IReadOnlyList<VariantValue> values,
        ReadOnlySpan<bool> isSqlNull,
        ShredOptions? options,
        [NotNullWhen(true)] out VariantArray? shredded)
    {
        var schema = InferSchema(values, isSqlNull, options);
        if (schema is null)
        {
            shredded = null;
            return false;
        }

        shredded = Shred(values, isSqlNull, schema);
        return true;
    }

    /// <inheritdoc cref="TryShred(IReadOnlyList{VariantValue}, ReadOnlySpan{bool}, ShredOptions, out VariantArray)"/>
    public static bool TryShred(
        IReadOnlyList<VariantValue> values,
        ReadOnlySpan<bool> isSqlNull,
        [NotNullWhen(true)] out VariantArray? shredded)
        => TryShred(values, isSqlNull, options: null, out shredded);

    /// <summary>
    /// Convenience overload for a caller holding a canonical (or already shredded) array with no
    /// encoded form of its own: decodes each row's logical value, infers a schema and shreds.
    /// </summary>
    public static bool TryShred(
        VariantArray array,
        ShredOptions? options,
        [NotNullWhen(true)] out VariantArray? shredded)
    {
        if (array is null)
        {
            throw new ArgumentNullException(nameof(array));
        }

        var (values, isNull, anyNull) = Decode(array);
        return TryShred(values, anyNull ? isNull : default, options, out shredded);
    }

    /// <inheritdoc cref="TryShred(VariantArray, ShredOptions, out VariantArray)"/>
    public static bool TryShred(
        VariantArray array,
        [NotNullWhen(true)] out VariantArray? shredded)
        => TryShred(array, options: null, out shredded);

    /// <summary>
    /// Decodes a column into one logical value per row plus a SQL-null mask. Masked rows carry
    /// <see cref="VariantValue.Null"/> as a placeholder — the shredder is handed every row, and the
    /// mask is what removes them again afterwards.
    /// </summary>
    private static (VariantValue[] Values, bool[] IsNull, bool AnyNull) Decode(VariantArray array)
    {
        int n = array.Length;
        var values = new VariantValue[n];
        var isNull = new bool[n];
        bool anyNull = false;
        for (int r = 0; r < n; r++)
        {
            if (array.IsNull(r))
            {
                isNull[r] = true;
                anyNull = true;
                values[r] = VariantValue.Null; // placeholder; masked by validity
                continue;
            }
            // Logical, so a shredded input re-shreds rather than losing its typed columns.
            values[r] = array.GetLogicalVariantValue(r);
        }

        return (values, isNull, anyNull);
    }

    private static void ValidateMask(ReadOnlySpan<bool> isSqlNull, int rowCount)
    {
        if (!isSqlNull.IsEmpty && isSqlNull.Length != rowCount)
        {
            throw new ArgumentException(
                $"null mask length {isSqlNull.Length} does not match {rowCount} values.",
                nameof(isSqlNull));
        }
    }

    // Rebuilds the extension array's storage struct with a validity bitmap marking the SQL-null rows
    // (buffers and children shared — only the top-level validity changes).
    private static VariantArray WithValidity(VariantArray variant, ReadOnlySpan<bool> isNull, int nullCount)
    {
        var storage = variant.StorageArray.Data;
        if (storage.Offset != 0)
        {
            // The bitmap below is built from bit 0 while the ArrayData keeps its offset, so a SLICED
            // storage array would shift which rows read as NULL. ShreddedVariantArrayBuilder always
            // returns an unsliced array; if that ever changes, fail loudly rather than silently
            // mis-attributing null-ness.
            throw new NotSupportedException(
                $"Cannot apply a SQL-null mask to a sliced variant storage array (offset {storage.Offset}).");
        }

        var validity = new ArrowBuffer.BitmapBuilder(isNull.Length);
        for (int i = 0; i < isNull.Length; i++)
        {
            validity.Append(!isNull[i]);
        }
        var newStorage = new ArrayData(
            storage.DataType, storage.Length, nullCount, storage.Offset,
            new[] { validity.Build() }, storage.Children, storage.Dictionary);
        // Apache.Arrow's factory, QUALIFIED on purpose: EngineeredWood.Parquet.Data — where this type
        // used to live — declares its own internal ArrowArrayFactory (ArrowArrayBuilder.cs) that
        // throws on 'struct'. Keeping the qualified name means the right factory stays bound if this
        // file or that using ever moves again.
        return new VariantArray(
            variant.VariantType, Apache.Arrow.ArrowArrayFactory.BuildArray(newStorage));
    }
}
