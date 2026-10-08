// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Keeps the <c>delta.dataSkippingStatsColumns</c> table property consistent with the schema, as delta-spark does.
/// </summary>
/// <remarks>
/// <para>The property names columns by LOGICAL name: a comma-separated list of dotted paths, a part backquoted when
/// it is not a plain identifier, with a doubled backquote standing for one. Spark validates it against the schema
/// on every metadata update (<c>OptimisticTransactionImpl.updateMetadataInternal</c> calls
/// <c>StatisticsCollection.validateDeltaStatsColumns</c> first), so an entry naming a column that is gone makes
/// every later ALTER on the table fail there. Spark therefore removes the entries a DROP takes away and rewrites
/// the ones a RENAME moves, and this does the same.</para>
/// <para>Both follow delta-spark 4.4.0 (read from its bytecode): a change at path <c>p</c> touches every entry
/// that <c>p</c> is a prefix of. Dropping every entry leaves the property <c>""</c>, not absent. Three deliberate
/// differences. Parts are compared case-insensitively, where Spark compares them case-sensitively: its
/// validation resolves an entry case-insensitively, so it accepts <c>ID</c> for a column <c>id</c>, and a
/// case-sensitive DROP then leaves <c>ID</c> behind to fail the next ALTER. That cannot hit the wrong column,
/// since a schema never holds two names that differ only in case (<see cref="CommitSchemaValidation"/>).
/// Entries are always re-written with Spark's RENAME quoting (<c>quoteIfNeeded</c> on each part), where Spark's
/// DROP path writes names with spaces unquoted and corrupts them. And a value this cannot parse is left alone
/// rather than read up to the junk, as Spark's prefix parse does.</para>
/// </remarks>
internal static class DataSkippingStatsColumns
{
    internal const string Key = "delta.dataSkippingStatsColumns";

    /// <summary>
    /// The entries of <paramref name="value"/>, each as its name parts, or null when it does not parse. An empty
    /// or blank value is an empty list (Spark: stats on no columns).
    /// </summary>
    internal static List<IReadOnlyList<string>>? Parse(string value)
    {
        var entries = new List<IReadOnlyList<string>>();
        int i = 0;
        SkipBlank(value, ref i);
        if (i == value.Length)
        {
            return entries;
        }

        while (true)
        {
            var parts = new List<string>();
            while (true)
            {
                SkipBlank(value, ref i);
                string? part = ReadPart(value, ref i);
                if (part is null)
                {
                    return null;
                }
                parts.Add(part);
                SkipBlank(value, ref i);
                if (i < value.Length && value[i] == '.')
                {
                    i++;
                    continue;
                }
                break;
            }
            entries.Add(parts);

            if (i == value.Length)
            {
                return entries;
            }
            if (value[i] != ',')
            {
                return null;
            }
            i++;
        }
    }

    // A backquoted part (`` stands for `), or a bare identifier of letters, digits and underscores that is not
    // all digits (the SQL lexer reads that as a number).
    private static string? ReadPart(string value, ref int i)
    {
        if (i < value.Length && value[i] == '`')
        {
            var sb = new StringBuilder();
            i++;
            while (i < value.Length)
            {
                if (value[i] == '`')
                {
                    if (i + 1 < value.Length && value[i + 1] == '`')
                    {
                        sb.Append('`');
                        i += 2;
                        continue;
                    }
                    i++;
                    return sb.ToString();
                }
                sb.Append(value[i++]);
            }
            return null;
        }

        int start = i;
        while (i < value.Length && (char.IsLetterOrDigit(value[i]) || value[i] == '_'))
        {
            i++;
        }
        if (i == start)
        {
            return null;
        }
        string bare = value.Substring(start, i - start);
        return bare.All(char.IsDigit) ? null : bare;
    }

    // Whitespace and SQL comments separate tokens, as in Spark's lexer (measured against Spark 4.1's parser): a
    // bracketed comment nests, and one left open runs to the end of the value, which Spark accepts because it
    // parses only a prefix. "/*+" opens a hint, not a comment, so it is left for ReadPart to refuse.
    private static void SkipBlank(string value, ref int i)
    {
        while (i < value.Length)
        {
            if (char.IsWhiteSpace(value[i]))
            {
                i++;
            }
            else if (OpensComment(value, i))
            {
                int depth = 1;
                i += 2;
                while (i < value.Length && depth > 0)
                {
                    if (OpensComment(value, i))
                    {
                        depth++;
                        i += 2;
                    }
                    else if (value[i] == '*' && i + 1 < value.Length && value[i + 1] == '/')
                    {
                        depth--;
                        i += 2;
                    }
                    else
                    {
                        i++;
                    }
                }
            }
            else if (i + 1 < value.Length && value[i] == '-' && value[i + 1] == '-')
            {
                int end = value.IndexOf('\n', i + 2);
                i = end < 0 ? value.Length : end + 1;
            }
            else
            {
                return;
            }
        }
    }

    private static bool OpensComment(string value, int i) =>
        i + 1 < value.Length && value[i] == '/' && value[i + 1] == '*'
        && !(i + 2 < value.Length && value[i + 2] == '+');

    /// <summary>Writes <paramref name="entries"/> back as Spark's RENAME does: <c>quoteIfNeeded</c> on each part,
    /// parts joined by <c>.</c> and entries by <c>,</c>.</summary>
    internal static string Format(IEnumerable<IReadOnlyList<string>> entries) =>
        string.Join(",", entries.Select(parts => string.Join(".", parts.Select(QuoteIfNeeded))));

    private static string QuoteIfNeeded(string part)
    {
        bool plain = part.Length > 0
            && (IsAsciiLetter(part[0]) || part[0] == '_')
            && part.All(c => IsAsciiLetter(c) || (c >= '0' && c <= '9') || c == '_');
        return plain ? part : "`" + part.Replace("`", "``") + "`";
    }

    private static bool IsAsciiLetter(char c) => (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');

    /// <summary>
    /// The configuration after a DROP of the field at <paramref name="path"/>: entries it is a prefix of removed.
    /// Returns <paramref name="configuration"/> itself when the property is absent, unparseable or untouched.
    /// </summary>
    internal static IReadOnlyDictionary<string, string>? AfterDrop(
        IReadOnlyDictionary<string, string>? configuration, IReadOnlyList<string> path) =>
        Rewrite(configuration, entries =>
        {
            var kept = entries.Where(e => !StartsWith(e, path)).ToList();
            return kept.Count == entries.Count ? null : kept;
        });

    /// <summary>
    /// The configuration after a RENAME of the field at <paramref name="oldPath"/> to <paramref name="newPath"/>
    /// (the same path but for its last part): entries under it moved. Returns <paramref name="configuration"/>
    /// itself when the property is absent, unparseable or untouched.
    /// </summary>
    internal static IReadOnlyDictionary<string, string>? AfterRename(
        IReadOnlyDictionary<string, string>? configuration, IReadOnlyList<string> oldPath,
        IReadOnlyList<string> newPath) =>
        Rewrite(configuration, entries =>
        {
            if (!entries.Any(e => StartsWith(e, oldPath)))
            {
                return null;
            }
            return entries
                .Select(e => StartsWith(e, oldPath)
                    ? (IReadOnlyList<string>)newPath.Concat(e.Skip(oldPath.Count)).ToList()
                    : e)
                .ToList();
        });

    /// <summary>
    /// The configuration after a schema replacement: entries the property would no longer be valid with removed,
    /// whether one names a column <paramref name="schema"/> lacks, a top-level column retyped to a type with no
    /// statistics, or a column an earlier entry already covers. So the property Spark validates on its next ALTER
    /// still holds. Returns <paramref name="configuration"/> itself when the property is absent, unparseable or
    /// untouched.
    /// </summary>
    internal static IReadOnlyDictionary<string, string>? AfterReplace(
        IReadOnlyDictionary<string, string>? configuration, StructType schema,
        IReadOnlyList<string>? partitionColumns) =>
        Rewrite(configuration, entries =>
        {
            var kept = new List<IReadOnlyList<string>>(entries.Count);
            foreach (var entry in entries)
            {
                if (Problem(schema, partitionColumns, [.. kept, entry]) is null)
                {
                    kept.Add(entry);
                }
            }
            return kept.Count == entries.Count ? null : kept;
        });

    // Applies `change` to the parsed entries; it returns null for "nothing to change".
    private static IReadOnlyDictionary<string, string>? Rewrite(
        IReadOnlyDictionary<string, string>? configuration,
        Func<List<IReadOnlyList<string>>, List<IReadOnlyList<string>>?> change)
    {
        if (configuration is null || !configuration.TryGetValue(Key, out var value)
            || Parse(value) is not { } entries || change(entries) is not { } changed)
        {
            return configuration;
        }

        var result = configuration.ToDictionary(kv => kv.Key, kv => kv.Value);
        result[Key] = Format(changed);
        return result;
    }

    private static bool StartsWith(IReadOnlyList<string> entry, IReadOnlyList<string> prefix)
    {
        if (entry.Count < prefix.Count)
        {
            return false;
        }
        for (int i = 0; i < prefix.Count; i++)
        {
            if (!string.Equals(entry[i], prefix[i], StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }
        return true;
    }

    /// <summary>
    /// Refuses a <c>delta.dataSkippingStatsColumns</c> value Spark would refuse at CREATE TABLE
    /// (<c>validateDeltaStatsColumns</c>): one that does not parse, or that names a partition column, a column
    /// the schema lacks, a top-level column of a type with no statistics, or one column twice.
    /// </summary>
    /// <remarks>Where Spark compares an entry's case with the schema's before the type and duplicate checks (so
    /// <c>FLAG</c> for a boolean <c>flag</c>, or <c>id,ID</c>, pass), this checks the column the entry resolves to,
    /// as Spark's own stats collection then does. A nested column of such a type is accepted, as in Spark, and gets
    /// no statistics.</remarks>
    internal static void Validate(
        StructType schema, IReadOnlyList<string>? partitionColumns,
        IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null || !configuration.TryGetValue(Key, out var value))
        {
            return;
        }

        var entries = Parse(value) ?? throw new DeltaFormatException(
            DeltaTableErrorCodes.InvalidDataSkippingStatsColumns,
            $"{Key} must be a comma-separated list of column names, a part in backquotes when it is not a plain "
            + $"identifier; '{value}' is not.");

        if (Problem(schema, partitionColumns, entries) is { } problem)
        {
            throw problem;
        }
    }

    // The first reason `entries` is not a valid value for `schema`, or null.
    private static DeltaFormatException? Problem(
        StructType schema, IReadOnlyList<string>? partitionColumns, IReadOnlyList<IReadOnlyList<string>> entries)
    {
        var partitions = new HashSet<string>(partitionColumns ?? [], StringComparer.OrdinalIgnoreCase);
        var leaves = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in entries)
        {
            string name = Format([entry]);
            if (entry.Count == 1 && partitions.Contains(entry[0]))
            {
                return new DeltaFormatException(
                    DeltaTableErrorCodes.DataSkippingPartitionColumn,
                    $"Data skipping is not supported for partition column '{name}' ({Key}).");
            }

            if (Resolve(schema, entry, out var type, out var resolved) is { } missing)
            {
                return new DeltaFormatException(
                    DeltaTableErrorCodes.ColumnNotFound, $"{Key} names '{name}', but {missing}.");
            }

            if (entry.Count == 1 && type is not StructType && !IsEligible(type))
            {
                return new DeltaFormatException(
                    DeltaTableErrorCodes.DataSkippingUnsupportedType,
                    $"Data skipping is not supported for column '{name}' of type {Describe(type)} ({Key}).");
            }

            foreach (string leaf in Leaves(type, resolved))
            {
                if (!leaves.Add(leaf))
                {
                    return new DeltaFormatException(
                        DeltaTableErrorCodes.DuplicateDataSkippingColumns,
                        $"{Key} names column '{leaf}' more than once.");
                }
            }
        }
        return null;
    }

    // Walks `path` through `schema`: struct fields by name, case-insensitively (Delta names are); an array's
    // element as `element`, a map's as `key`/`value`, exactly. Returns null when it resolves, else why not.
    private static string? Resolve(
        StructType schema, IReadOnlyList<string> path, out DeltaDataType type, out List<string> resolved)
    {
        type = schema;
        resolved = new List<string>(path.Count);
        foreach (string part in path)
        {
            switch (type)
            {
                case StructType st:
                    var field = st.Fields.FirstOrDefault(
                        f => string.Equals(f.Name, part, StringComparison.OrdinalIgnoreCase));
                    if (field is null)
                    {
                        return resolved.Count == 0
                            ? "the table has no such column"
                            : $"struct '{Format([resolved])}' has no field '{part}'";
                    }
                    type = field.Type;
                    resolved.Add(field.Name);
                    break;
                case ArrayType at when part == "element":
                    type = at.ElementType;
                    resolved.Add(part);
                    break;
                case MapType mt when part is "key" or "value":
                    type = part == "key" ? mt.KeyType : mt.ValueType;
                    resolved.Add(part);
                    break;
                case ArrayType:
                    return $"'{Format([resolved])}' is an array, whose element is named 'element'";
                case MapType:
                    return $"'{Format([resolved])}' is a map, whose parts are named 'key' and 'value'";
                default:
                    return $"'{Format([resolved])}' is not a struct";
            }
        }
        return null;
    }

    // The leaf paths an entry covers: a struct stands for every field under it.
    private static IEnumerable<string> Leaves(DeltaDataType type, List<string> path)
    {
        if (type is not StructType st)
        {
            yield return Format([path]);
            yield break;
        }
        foreach (var field in st.Fields)
        {
            foreach (string leaf in Leaves(field.Type, [.. path, field.Name]))
            {
                yield return leaf;
            }
        }
    }

    // Spark's SkippingEligibleDataType: numbers, dates, timestamps, strings and variants.
    private static bool IsEligible(DeltaDataType type) =>
        type is PrimitiveType p
        && (p.TypeName is "byte" or "short" or "integer" or "long" or "float" or "double" or "date"
                or "timestamp" or "timestamp_ntz" or "string" or "variant"
            || p.TypeName.StartsWith("decimal", StringComparison.Ordinal));

    private static string Describe(DeltaDataType type) => type switch
    {
        PrimitiveType p => p.TypeName,
        ArrayType => "array",
        MapType => "map",
        _ => type.GetType().Name,
    };
}
