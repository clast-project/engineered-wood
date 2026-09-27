// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Translates the per-column <see cref="ParquetWriteOptions"/> a caller names by LOGICAL column into the
/// PHYSICAL leaf paths a column-mapped table's data files carry (#416).
/// </summary>
/// <remarks>
/// <para>The Parquet writer matches every per-column option against the file's own dotted leaf path (and
/// <see cref="ParquetWriteOptions.VariantShredSchemas"/> against the top-level column name). Under column
/// mapping those are <c>col-&lt;uuid&gt;</c> names, so an option naming the logical column matched
/// nothing and was silently ignored: no Bloom filter written, statistics written that the caller turned
/// off, a timestamp written at default precision.</para>
/// <para>Translation is per write, against the snapshot's schema, because a RENAME COLUMN keeps the
/// physical name and changes the logical one. A key that does not resolve is left as it is, which is
/// what it was before: ignored unless it already names a physical path, so a caller who worked around
/// the bug by naming <c>col-&lt;uuid&gt;</c> directly keeps working.</para>
/// </remarks>
internal static class ColumnMappingWriteOptions
{
    /// <summary>
    /// <paramref name="options"/> with every per-column key translated to its physical path under
    /// <paramref name="mode"/>; the same instance when nothing needs translating.
    /// </summary>
    public static ParquetWriteOptions ToPhysical(
        ParquetWriteOptions options, StructType schema, ColumnMappingMode mode)
    {
#pragma warning disable EWPARQUET0004 // translated like every other per-column option
        var extendedTimestampColumns = options.ExtendedTimestampColumns;
#pragma warning restore EWPARQUET0004
        if (mode == ColumnMappingMode.None
            || (options.BloomFilterColumns is null && options.ColumnCodecs is null
                && options.ColumnCompressionLevels is null && options.ColumnEncodings is null
                && options.ColumnDictionaryEnabled is null && options.ColumnWriteStatistics is null
                && extendedTimestampColumns is null && options.VariantShredSchemas is null))
        {
            return options;
        }

        string Physical(string key) => Translate(key, schema, mode);

        return options with
        {
            BloomFilterColumns = Keys(options.BloomFilterColumns, Physical),
            ColumnCodecs = Map(options.ColumnCodecs, Physical),
            ColumnCompressionLevels = Map(options.ColumnCompressionLevels, Physical),
            ColumnEncodings = Map(options.ColumnEncodings, Physical),
            ColumnDictionaryEnabled = Map(options.ColumnDictionaryEnabled, Physical),
            ColumnWriteStatistics = Map(options.ColumnWriteStatistics, Physical),
#pragma warning disable EWPARQUET0004
            ExtendedTimestampColumns = Keys(extendedTimestampColumns, Physical),
#pragma warning restore EWPARQUET0004
            VariantShredSchemas = Map(options.VariantShredSchemas, Physical),
        };
    }

    /// <summary>
    /// The physical dotted path for a logical one, or <paramref name="key"/> unchanged when it does not
    /// resolve against <paramref name="schema"/>.
    /// </summary>
    /// <remarks>
    /// Walks the path a segment at a time. A struct field becomes its physical name. A list's
    /// <c>list.&lt;element&gt;</c> and a map's <c>key_value.key</c>/<c>key_value.value</c> are Parquet's own
    /// nodes and pass through. Below a primitive (a variant's <c>metadata</c>/<c>value</c>) the rest passes
    /// through too. A field whose own name contains a dot is matched by trying the longest run of
    /// segments first; a path two fields could both spell is left unresolved.
    /// </remarks>
    internal static string Translate(string key, StructType schema, ColumnMappingMode mode)
    {
        string[] segments = key.Split('.');
        var output = new List<string>(segments.Length);
        DeltaDataType type = schema;
        int i = 0;
        while (i < segments.Length)
        {
            switch (type)
            {
                case StructType st:
                {
                    StructField? match = null;
                    int consumed = 0;
                    for (int take = segments.Length - i; take >= 1; take--)
                    {
                        string name = string.Join(".", segments, i, take);
                        var field = st.Fields.FirstOrDefault(f => string.Equals(f.Name, name, StringComparison.Ordinal));
                        if (field is null)
                            continue;
                        if (match is not null)
                            return key; // two readings of the same path: ambiguous
                        match = field;
                        consumed = take;
                    }
                    if (match is null)
                        return key;
                    output.Add(ColumnMapping.GetPhysicalName(match, mode));
                    type = match.Type;
                    i += consumed;
                    break;
                }
                case ArrayType array:
                    if (segments.Length - i < 2 || segments[i] != "list")
                        return key;
                    output.Add(segments[i]);
                    output.Add(segments[i + 1]);
                    type = array.ElementType;
                    i += 2;
                    break;
                case MapType map:
                    if (segments.Length - i < 2 || segments[i] != "key_value"
                        || segments[i + 1] is not ("key" or "value"))
                        return key;
                    output.Add(segments[i]);
                    output.Add(segments[i + 1]);
                    type = segments[i + 1] == "key" ? map.KeyType : map.ValueType;
                    i += 2;
                    break;
                default:
                    for (; i < segments.Length; i++)
                        output.Add(segments[i]);
                    break;
            }
        }
        return string.Join(".", output);
    }

    private static IReadOnlyCollection<string>? Keys(IReadOnlyCollection<string>? keys, Func<string, string> translate) =>
        keys is null ? null : new HashSet<string>(keys.Select(translate), StringComparer.Ordinal);

    private static IReadOnlyDictionary<string, T>? Map<T>(
        IReadOnlyDictionary<string, T>? map, Func<string, string> translate)
    {
        if (map is null)
            return null;
        var translated = new Dictionary<string, T>(StringComparer.Ordinal);
        foreach (var entry in map)
            translated[translate(entry.Key)] = entry.Value;
        return translated;
    }
}
