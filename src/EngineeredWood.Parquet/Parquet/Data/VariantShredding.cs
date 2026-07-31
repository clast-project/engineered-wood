// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics.CodeAnalysis;

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;

namespace EngineeredWood.Parquet.Data;

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
/// package; this type is the array-level adapter over them, so a caller does not take that
/// dependency to read or write shredded variants.</para>
///
/// <para><b>Why both directions live here.</b> Shredding is a PHYSICAL-LAYOUT concern, decided per
/// file from the column's values, and it is useful to anyone writing parquet variants — not only to
/// a particular table format. Keeping the pair together means a host that hands us variants in some
/// other in-memory form only has to reach the canonical <see cref="VariantArray"/>; the layout
/// decision, the shredder, and the reassembly are ours.</para>
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
    /// Infers a shredding schema from <paramref name="values"/> and, when one applies, shreds them
    /// into typed columns plus a residual — returning <see langword="true"/> and the shredded
    /// <see cref="VariantArray"/>. Returns <see langword="false"/> when the column's shapes are too
    /// mixed to shred (or every row is SQL null), in which case the caller should keep whatever
    /// unshredded representation it already holds.
    /// </summary>
    /// <param name="values">
    /// One decoded value per row. A row marked in <paramref name="isSqlNull"/> contributes nothing to
    /// the inferred schema and is masked out of the result, so a placeholder may sit there — it is
    /// still handed to the shredder, so it must be a value the shredder accepts;
    /// <see cref="VariantValue.Null"/> is the obvious choice.
    /// </param>
    /// <param name="isSqlNull">
    /// Per-row SQL-null mask, or an empty span when no row is null. SQL null-ness rides the storage
    /// struct's VALIDITY and is deliberately distinct from a variant JSON null carried in the value
    /// bytes — a caller that conflates the two changes what <c>IS NULL</c> means.
    /// </param>
    /// <param name="shredded">The shredded column, or <see langword="null"/> when the result is false.</param>
    /// <remarks>
    /// <para>This overload takes ALREADY-DECODED values on purpose. A host that arrived with an
    /// encoded form (a blob column, an IPC payload) has to parse each row to decide anything about
    /// it, so handing us the parsed values keeps the decode at ONE per row; a
    /// <see cref="VariantArray"/>-only entry point would force it to encode a canonical array first
    /// and have us decode it again. <see cref="TryShred(VariantArray, out VariantArray)"/> is the
    /// convenience path for a caller that genuinely starts from a canonical array.</para>
    /// <para>Returning false rather than an unshredded array is likewise deliberate: building one
    /// would re-encode every row, discarding bytes the caller may already have.</para>
    /// </remarks>
    public static bool TryShred(
        IReadOnlyList<VariantValue> values,
        ReadOnlySpan<bool> isSqlNull,
        [NotNullWhen(true)] out VariantArray? shredded)
    {
        if (values is null)
        {
            throw new ArgumentNullException(nameof(values));
        }
        if (!isSqlNull.IsEmpty && isSqlNull.Length != values.Count)
        {
            throw new ArgumentException(
                $"null mask length {isSqlNull.Length} does not match {values.Count} values.",
                nameof(isSqlNull));
        }

        shredded = null;
        int n = values.Count;
        var nonNull = new List<VariantValue>(n);
        int nulls = 0;
        for (int r = 0; r < n; r++)
        {
            if (!isSqlNull.IsEmpty && isSqlNull[r])
            {
                nulls++;
                continue;
            }
            nonNull.Add(values[r]);
        }

        if (nonNull.Count == 0)
        {
            return false; // nothing to infer from — an all-null column has no shape.
        }

        var schema = new ShredSchemaInferer().Infer(nonNull);
        if (schema.TypedValueType == ShredType.None)
        {
            return false;
        }

        var (metadata, rows) = VariantShredder.Shred(values, schema);
        var array = ShreddedVariantArrayBuilder.Build(schema, metadata, rows);
        if (nulls > 0)
        {
            // The shredder saw a placeholder at each SQL-null row; re-apply those rows as storage
            // validity so they read back as SQL NULL rather than as a shredded placeholder value.
            array = WithValidity(array, isSqlNull, nulls);
        }
        shredded = array;
        return true;
    }

    /// <summary>
    /// Convenience overload for a caller holding a canonical (or already shredded) array with no
    /// encoded form of its own: decodes each row's logical value and delegates to
    /// <see cref="TryShred(IReadOnlyList{VariantValue}, ReadOnlySpan{bool}, out VariantArray)"/>.
    /// </summary>
    public static bool TryShred(
        VariantArray array,
        [NotNullWhen(true)] out VariantArray? shredded)
    {
        if (array is null)
        {
            throw new ArgumentNullException(nameof(array));
        }

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

        return TryShred(values, anyNull ? isNull : default, out shredded);
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
        // Apache.Arrow's factory, QUALIFIED on purpose: this namespace declares its own internal
        // ArrowArrayFactory (ArrowArrayBuilder.cs) which throws on 'struct', and a type in the
        // enclosing namespace beats an imported one — so the unqualified name would compile and then
        // fail at runtime on exactly this path.
        return new VariantArray(
            variant.VariantType, Apache.Arrow.ArrowArrayFactory.BuildArray(newStorage));
    }
}
