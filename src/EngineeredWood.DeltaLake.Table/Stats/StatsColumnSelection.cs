// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Table.Stats;

/// <summary>
/// Which columns of a data file get statistics, and how long a string bound may be: the table's
/// <c>delta.dataSkippingStatsColumns</c>, <c>delta.dataSkippingNumIndexedCols</c> and
/// <c>delta.dataSkippingStringPrefixLength</c>, as delta-spark 4.4.0 applies them (read from its bytecode and
/// measured on tables it wrote).
/// </summary>
/// <remarks>
/// <para>Both column properties select by LOGICAL name; statistics are keyed by physical name, so the selection is
/// built from the logical schema and held as physical names. A column left out gets no <c>minValues</c>,
/// <c>maxValues</c> or <c>nullCount</c>; <c>numRecords</c> is always written.</para>
/// <para><c>delta.dataSkippingStatsColumns</c>, when set, wins: each entry selects the column it names, a struct
/// entry its whole subtree, matched case-insensitively; an entry reaching through an array or a map selects
/// nothing, and <c>""</c> selects no column at all. Otherwise <c>delta.dataSkippingNumIndexedCols</c> (default 32,
/// <c>-1</c> for every column) takes the first N leaves of the data schema, depth-first in schema order: partition
/// columns are skipped, every non-struct field takes one slot whatever its type, and a struct is cut part-way when
/// the count runs out inside it.</para>
/// <para>A value another writer left that Spark would refuse to set is read as the default, which only ever widens
/// the selection: extra statistics are never wrong.</para>
/// </remarks>
internal sealed class StatsColumnSelection
{
    internal const string NumIndexedColsKey = "delta.dataSkippingNumIndexedCols";
    internal const string StringPrefixLengthKey = "delta.dataSkippingStringPrefixLength";
    internal const int DefaultNumIndexedCols = 32;
    internal const int DefaultStringPrefixLength = 32;

    /// <summary>Every column, with the default string prefix: what EW collected before it read the properties.</summary>
    internal static StatsColumnSelection All { get; } = new(null, DefaultStringPrefixLength);

    // Physical name -> the selection below it; a null value selects the whole subtree, and a null map selects
    // every field.
    private readonly Dictionary<string, Node?>? _fields;

    private StatsColumnSelection(Dictionary<string, Node?>? fields, int stringPrefixLength)
    {
        _fields = fields;
        StringPrefixLength = stringPrefixLength;
    }

    /// <summary>The longest a string bound may be, in UTF-16 code units.</summary>
    internal int StringPrefixLength { get; }

    /// <summary>
    /// Whether the top-level column <paramref name="physicalName"/> is selected; <paramref name="below"/> is then
    /// the selection inside it, or null when all of it is.
    /// </summary>
    internal bool Includes(string physicalName, out Node? below) => Node.Lookup(_fields, physicalName, out below);

    /// <summary>The selection inside a struct.</summary>
    internal sealed class Node
    {
        private readonly Dictionary<string, Node?> _fields = new(StringComparer.Ordinal);

        internal bool Includes(string physicalName, out Node? below) => Lookup(_fields, physicalName, out below);

        internal static bool Lookup(Dictionary<string, Node?>? fields, string name, out Node? below)
        {
            below = null;
            return fields is null || fields.TryGetValue(name, out below);
        }

        internal static void Add(Dictionary<string, Node?> fields, string name, Node? below)
        {
            // A whole subtree already selected stays whole; otherwise merge into what is there.
            if (fields.TryGetValue(name, out var existing) && existing is null)
            {
                return;
            }
            fields[name] = below;
        }

        internal Dictionary<string, Node?> Fields => _fields;
    }

    /// <summary>The selection for a data file of a table with <paramref name="metadata"/>, whose logical schema is
    /// <paramref name="schema"/> (which may be a pending one, ahead of the metadata's own).</summary>
    internal static StatsColumnSelection For(StructType schema, MetadataAction metadata)
    {
        var config = metadata.Configuration;
        var mode = ColumnMapping.GetMode(config);
        int prefix = ReadInt(config, StringPrefixLengthKey, min: 0) ?? DefaultStringPrefixLength;
        var partitions = new HashSet<string>(metadata.PartitionColumns, StringComparer.OrdinalIgnoreCase);
        var dataFields = schema.Fields.Where(f => !partitions.Contains(f.Name)).ToList();

        if (config is not null && config.TryGetValue(DataSkippingStatsColumns.Key, out var listed)
            && DataSkippingStatsColumns.Parse(listed) is { } entries)
        {
            var top = new Dictionary<string, Node?>(StringComparer.Ordinal);
            foreach (var entry in entries)
            {
                AddEntry(top, dataFields, entry, 0, mode);
            }
            return new StatsColumnSelection(top, prefix);
        }

        int n = ReadInt(config, NumIndexedColsKey, min: -1) ?? DefaultNumIndexedCols;
        if (n == -1)
        {
            return new StatsColumnSelection(
                dataFields.ToDictionary(f => ColumnMapping.GetPhysicalName(f, mode), _ => (Node?)null), prefix);
        }

        var first = new Dictionary<string, Node?>(StringComparer.Ordinal);
        Truncate(first, dataFields, ref n, mode);
        return new StatsColumnSelection(first, prefix);
    }

    // Spark's filterSchema: an entry selects the field it names (all of it), through structs only.
    private static void AddEntry(
        Dictionary<string, Node?> into, IReadOnlyList<StructField> fields, IReadOnlyList<string> entry, int depth,
        ColumnMappingMode mode)
    {
        var field = fields.FirstOrDefault(f => string.Equals(f.Name, entry[depth], StringComparison.OrdinalIgnoreCase));
        if (field is null)
        {
            return;
        }

        string physical = ColumnMapping.GetPhysicalName(field, mode);
        if (depth == entry.Count - 1)
        {
            into[physical] = null;
            return;
        }
        if (field.Type is not StructType st)
        {
            return;
        }
        if (into.TryGetValue(physical, out var existing) && existing is null)
        {
            return; // the whole struct is already selected
        }

        var node = existing ?? new Node();
        AddEntry(node.Fields, st.Fields, entry, depth + 1, mode);
        if (node.Fields.Count > 0)
        {
            into[physical] = node;
        }
    }

    // Spark's truncateSchema: the first n leaves, depth-first; a struct is kept, cut part-way, when the count runs out
    // inside it.
    private static void Truncate(
        Dictionary<string, Node?> into, IReadOnlyList<StructField> fields, ref int remaining, ColumnMappingMode mode)
    {
        foreach (var field in fields)
        {
            if (remaining <= 0)
            {
                return;
            }

            string physical = ColumnMapping.GetPhysicalName(field, mode);
            if (field.Type is StructType st)
            {
                var node = new Node();
                Truncate(node.Fields, st.Fields, ref remaining, mode);
                Node.Add(into, physical, node);
            }
            else
            {
                Node.Add(into, physical, null);
                remaining--;
            }
        }
    }

    /// <summary>
    /// Refuses a <c>delta.dataSkippingNumIndexedCols</c> below -1, or a <c>delta.dataSkippingStringPrefixLength</c>
    /// below 0, or either one not an integer: Spark refuses each when the property is set.
    /// </summary>
    internal static void Validate(IReadOnlyDictionary<string, string>? configuration)
    {
        Check(configuration, NumIndexedColsKey, -1, "an integer of at least -1 (-1 for every column)");
        Check(configuration, StringPrefixLengthKey, 0, "an integer of at least 0");
    }

    private static void Check(IReadOnlyDictionary<string, string>? configuration, string key, int min, string what)
    {
        if (configuration is not null && configuration.TryGetValue(key, out var value)
            && ReadInt(configuration, key, min) is null)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.InvalidDataSkippingProperty, $"{key} must be {what}; '{value}' is not.");
        }
    }

    // Java's Integer.parseInt, as Spark reads the property: an optional sign, then digits, nothing else.
    private static int? ReadInt(IReadOnlyDictionary<string, string>? configuration, string key, int min)
    {
        if (configuration is null || !configuration.TryGetValue(key, out var value)
            || value.Length == 0 || char.IsWhiteSpace(value[0]) || char.IsWhiteSpace(value[value.Length - 1])
            || !int.TryParse(value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int n)
            || n < min)
        {
            return null;
        }
        return n;
    }
}
