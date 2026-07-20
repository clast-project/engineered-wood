// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Reassembles shredded parquet VARIANT columns into the canonical unshredded form.
/// </summary>
/// <remarks>
/// <para>The parquet VARIANT spec allows a writer to "shred" a column: instead of carrying the whole
/// value in the <c>value</c> binary child, it hoists part or all of it into a typed
/// <c>typed_value</c> child so readers can push predicates down and skip decoding. Spark 4.x and
/// DuckDB both shred by default, so most variant columns encountered in the wild are shredded.</para>
///
/// <para><see cref="VariantArray"/> is a STORAGE-level view: on a shredded column its <c>value</c>
/// child is empty for shredded rows and <see cref="VariantArray.GetValueBytes"/> returns zero bytes
/// while <c>IsNull</c> reports false — a valid row holding an empty variant. Without the
/// reassembly below, a caller would silently read empty values from every Spark-written column.
/// Reassembly itself lives in the companion <c>Apache.Arrow.Operations</c> package; this type is the
/// thin adapter that applies it to a whole array.</para>
///
/// <para>The result is an ordinary unshredded <see cref="VariantArray"/> — values are correct and
/// uniform, but the shredded layout is not preserved, so a caller cannot inspect
/// <c>typed_value</c> afterwards. That trade is deliberate: the reader's contract is to materialise
/// values the caller can read without taking a second dependency. If preserving the shredded layout
/// is ever needed (e.g. to push predicates into <c>typed_value</c>), it should become an explicit
/// opt-in on <see cref="ParquetReadOptions"/> rather than the default.</para>
/// </remarks>
internal static class VariantShredding
{
    /// <summary>
    /// Returns <paramref name="array"/> unchanged when it is already unshredded; otherwise returns an
    /// equivalent unshredded <see cref="VariantArray"/> whose <c>value</c> child carries each row's
    /// fully reconstructed variant bytes. Null rows stay null.
    /// </summary>
    internal static VariantArray Reassemble(VariantArray array)
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
}
