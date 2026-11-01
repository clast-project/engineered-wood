// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Table;

public sealed partial class DeltaTable
{
    /// <summary>The <c>typeWidening</c> reader+writer feature a widened column needs. Spark's preview name for
    /// it is accepted too: a table that declares that one can already be read widened.</summary>
    private const string TypeWideningFeature = "typeWidening";
    private const string TypeWideningPreviewFeature = "typeWidening-preview";

    /// <summary>
    /// The schema change a <c>mergeSchema</c> write makes so that <paramref name="batches"/> fit the table, or
    /// null when they already do. Computed against <paramref name="snapshot"/> (a transaction's pending schema,
    /// or the table's) and applied by the caller in the SAME commit as the data: Spark's
    /// <c>mergeSchema=true</c>, opt-in, because a typo in a column name should not silently add a column.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    /// <item><b>A new top-level column</b> is added at the end through <see cref="ComputeAddColumn(StructField, MetadataAction, ProtocolAction)"/>,
    /// so under column mapping it gets an id and a physical name as <see cref="AddColumnAsync(Field, CancellationToken)"/>
    /// would give it.</item>
    /// <item><b>A new field of a struct column</b>, at any depth of structs, is added through
    /// <see cref="ComputeAddField(IReadOnlyList{string}, StructField, MetadataAction, ProtocolAction)"/>. Inside an
    /// array element or a map key or value it is refused: there is no path to it that a nested ADD can name.</item>
    /// <item><b>Everything is added nullable</b>, nested fields and elements included, whatever the batch declares,
    /// as Spark adds them (<c>asNullable</c>): rows already in the table have no value for the column. A NOT NULL
    /// column cannot be added by a merge.</item>
    /// <item><b>A column the batch omits</b> is written as NULL, as without the option.</item>
    /// <item><b>A wider type</b> in the batch widens the column, only when the table enables
    /// <c>delta.enableTypeWidening</c> and only for the changes Spark applies automatically during schema
    /// evolution: a wider integer, float to double, a decimal no narrower in either integer or fractional digits,
    /// and date to timestamp_ntz. Integer to double or decimal is a change Spark makes only on an explicit ALTER,
    /// so it is refused here too. The change is recorded in <c>delta.typeChanges</c>, and files written before it
    /// read widened. A partition column, a column a CHECK constraint or generation expression reads, and a value
    /// inside an array or map are not widened. Any other type difference is left to the write's own type check,
    /// which refuses it.</item>
    /// <item><b>A name differing from a column's only in case</b> (<c>ID</c> against <c>id</c>) is refused. Delta
    /// column names are case-insensitive, so it cannot be a new column, and writing it into <c>id</c> would be
    /// guessing.</item>
    /// <item><b>A read's metadata column</b> (<c>_metadata.*</c>, the transient row address) is never added; the
    /// write refuses it as it does without the option.</item>
    /// </list>
    /// </remarks>
    internal DeferredSchemaChange? ComputeSchemaMerge(
        Snapshot.Snapshot snapshot, IReadOnlyList<RecordBatch> batches)
    {
        // First, as the write itself does: once a duplicate's first copy is added, the second would match it.
        foreach (var batch in batches)
            ThrowIfDuplicateColumns(batch.Schema, snapshot.Schema, "Write");

        var merge = new SchemaMerge(this, snapshot);
        foreach (var batch in batches)
        {
            foreach (var field in batch.Schema.FieldsList)
                merge.MergeTopLevel(field);
        }
        return merge.Result();
    }

    /// <summary>The running state of one <see cref="ComputeSchemaMerge"/>: each change composes on the
    /// previous one's pending metadata and protocol, so the commit carries only the final pair.</summary>
    private sealed class SchemaMerge
    {
        private readonly DeltaTable _table;
        private readonly Snapshot.Snapshot _snapshot;
        private MetadataAction _metadata;
        private ProtocolAction _protocol;
        private StructType _schema;
        private bool _changed;
        private bool _protocolChanged;
        // Paths (logical, one segment per name) this merge added. A later batch that disagrees about one of their
        // types is not a widening — no file was ever written with the first type — so it is left to the type
        // check. Kept as segments: a column may itself be named "a.b", so a dot-joined path is ambiguous.
        private readonly List<string[]> _added = [];

        public SchemaMerge(DeltaTable table, Snapshot.Snapshot snapshot)
        {
            _table = table;
            _snapshot = snapshot;
            _metadata = snapshot.Metadata;
            _protocol = snapshot.Protocol;
            _schema = snapshot.Schema;
        }

        public void MergeTopLevel(Field field)
        {
            var declared = FindDeclared(_schema.Fields, field.Name);
            if (declared is null)
            {
                if (LooksLikeReadMetadata(field.Name))
                    return; // ThrowIfUndeclaredColumns refuses it, with the hint that it is the read's
                ThrowIfCaseOnly(_schema.Fields, field.Name, field.Name);

                var added = AsNullable(ToDeltaField(field));
                Apply(_table.ComputeAddColumn(added, _metadata, _protocol));
                _added.Add([added.Name]);
                return;
            }

            var supplied = ToDeltaField(field);
            Merge([declared.Name], declared.Type, supplied.Type, field.Name, inCollection: false);
        }

        /// <param name="path">The LOGICAL path of the value being merged, as a nested ADD or a type change names
        /// it.</param>
        /// <param name="display">The path as the batch spells it, for messages.</param>
        /// <param name="inCollection">The value sits inside an array element or a map key or value.</param>
        private void Merge(
            List<string> path, DeltaDataType declared, DeltaDataType supplied, string display, bool inCollection)
        {
            switch (declared, supplied)
            {
                case (StructType d, StructType s):
                    foreach (var child in s.Fields)
                    {
                        string childDisplay = display + "." + child.Name;
                        var match = FindDeclared(d.Fields, child.Name);
                        if (match is null)
                        {
                            ThrowIfCaseOnly(d.Fields, child.Name, childDisplay);
                            if (inCollection)
                            {
                                throw new DeltaFormatException(
                                    DeltaTableErrorCodes.UnsupportedSchemaMerge,
                                    $"mergeSchema: the batch has a field '{childDisplay}' the table does not "
                                    + "declare, inside an array element or a map key or value. A merge adds nested "
                                    + "fields only inside struct columns; declare it with SetSchemaAsync first.");
                            }
                            var added = AsNullable(child);
                            Apply(_table.ComputeAddField(path, added, _metadata, _protocol));
                            _added.Add([.. path, added.Name]);
                            continue;
                        }
                        Merge([.. path, match.Name], match.Type, child.Type, childDisplay, inCollection);
                    }
                    break;

                case (ArrayType d, ArrayType s):
                    Merge(path, d.ElementType, s.ElementType, display + ".element", inCollection: true);
                    break;

                case (MapType d, MapType s):
                    Merge(path, d.KeyType, s.KeyType, display + ".key", inCollection: true);
                    Merge(path, d.ValueType, s.ValueType, display + ".value", inCollection: true);
                    break;

                case (PrimitiveType d, PrimitiveType s)
                    when !string.Equals(d.TypeName, s.TypeName, StringComparison.Ordinal):
                    Widen(path, d, s, display, inCollection);
                    break;

                // The same type, or a shape difference (a struct against a primitive): the write's type check
                // refuses the latter with its own message.
            }
        }

        private static bool IsPrefix(string[] prefix, List<string> path)
        {
            if (prefix.Length > path.Count)
                return false;
            for (int i = 0; i < prefix.Length; i++)
            {
                if (!string.Equals(prefix[i], path[i], StringComparison.Ordinal))
                    return false;
            }
            return true;
        }

        private void Widen(
            List<string> path, PrimitiveType declared, PrimitiveType supplied, string display, bool inCollection)
        {
            var config = _metadata.Configuration;
            bool iceberg = IcebergCompat.GetVersion(config) != IcebergCompatVersion.None;
            if (!IsSchemaEvolutionWidening(declared.TypeName, supplied.TypeName, iceberg))
                return; // not a widening: ThrowIfMistypedColumns refuses it

            if (_added.Any(a => IsPrefix(a, path)))
                return; // added by this merge, so there is nothing to widen; the type check judges the disagreement

            string change = $"'{display}' is {declared.TypeName} in the table and {supplied.TypeName} in the batch";
            // Before the table setting: inside a collection no setting would make it work, so the error says so.
            if (inCollection)
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.UnsupportedSchemaMerge,
                    $"mergeSchema: {change}, inside an array element or a map key or value, which a merge does "
                    + $"not widen. Cast the column to {declared.TypeName}.");
            }
            if (!Schema.TypeWidening.IsEnabled(config))
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.MergeIncompatibleDataType,
                    $"mergeSchema: {change}. Widening the column needs type widening; set "
                    + $"{Schema.TypeWidening.EnableKey}=true on the table, or cast the column to {declared.TypeName}.");
            }
            if (_metadata.PartitionColumns.Contains(path[0]))
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.MergeIncompatibleDataType,
                    $"mergeSchema: {change}, and '{path[0]}' is a partition column, which a merge does not widen. "
                    + $"Cast the column to {declared.TypeName}.");
            }
            SchemaChangeDependents.EnsureChangeable(
                _schema, _metadata, _snapshot.DomainMetadata, path, isDrop: false, verb: "change the type of");

            string name = path[path.Count - 1];
            var newSchema = TransformStructAt(_schema, path.Take(path.Count - 1).ToList(), 0, fields =>
                fields.Select(f => string.Equals(f.Name, name, StringComparison.Ordinal)
                    ? Schema.TypeWidening.AddTypeChange(
                        new StructField { Name = f.Name, Type = supplied, Nullable = f.Nullable, Metadata = f.Metadata },
                        declared.TypeName, supplied.TypeName)
                    : f).ToList());
            CommitSchemaValidation.EnsureValid(newSchema);

            _schema = newSchema;
            _metadata = _metadata with { SchemaString = DeltaSchemaSerializer.Serialize(newSchema) };
            _changed = true;

            var features = new List<string>();
            if (_protocol.ReaderFeatures?.Contains(TypeWideningPreviewFeature) != true
                || _protocol.WriterFeatures?.Contains(TypeWideningPreviewFeature) != true)
            {
                features.Add(TypeWideningFeature);
            }
            features.AddRange(RequiredSchemaFeatures(supplied));
            if (UpgradeProtocolForFeatures(_protocol, features) is { } upgrade)
            {
                _protocol = upgrade;
                _protocolChanged = true;
            }
        }

        private void Apply(DeferredSchemaChange change)
        {
            _metadata = change.Metadata;
            _schema = change.NewSchema;
            _changed = true;
            if (change.ProtocolUpgrade is { } upgrade)
            {
                _protocol = upgrade;
                _protocolChanged = true;
            }
        }

        public DeferredSchemaChange? Result()
        {
            if (!_changed)
                return null;
            var actions = new List<DeltaAction>();
            ProtocolAction? upgrade = _protocolChanged ? _protocol : null;
            if (upgrade is not null)
                actions.Add(upgrade);
            actions.Add(_metadata);
            return new DeferredSchemaChange(actions, _metadata, upgrade, _schema);
        }
    }

    /// <summary>
    /// The type changes Spark applies by itself during schema evolution
    /// (<c>TypeWidening.isTypeChangeSupportedForSchemaEvolution</c>, delta-spark 4.4.0): a wider integer, float to
    /// double, a decimal at least as wide in both its integer and its fractional digits, and date to
    /// timestamp_ntz. Narrower than <see cref="Schema.TypeWidening.IsSupported"/>, which also allows integer to double and
    /// to decimal; Spark makes those only on an explicit ALTER. On an IcebergCompat table only the changes Iceberg
    /// can follow: integer to long, float to double, and a decimal's precision at the same scale.
    /// </summary>
    internal static bool IsSchemaEvolutionWidening(string from, string to, bool icebergCompat)
    {
        if (icebergCompat)
        {
            return (from, to) is ("integer", "long") or ("float", "double")
                || (TryParseDecimal(from, out int fp, out int fs) && TryParseDecimal(to, out int tp, out int ts)
                    && ts == fs && tp > fp);
        }

        if (IntegralWidth(from) is int fromWidth && IntegralWidth(to) is int toWidth)
            return toWidth > fromWidth;
        if ((from, to) is ("float", "double") or ("date", "timestamp_ntz"))
            return true;
        if (TryParseDecimal(from, out int p1, out int s1) && TryParseDecimal(to, out int p2, out int s2))
            return p2 - s2 >= p1 - s1 && s2 >= s1 && (p2, s2) != (p1, s1);
        return false;
    }

    private static int? IntegralWidth(string type) => type switch
    {
        "byte" => 1,
        "short" => 2,
        "integer" => 4,
        "long" => 8,
        _ => null,
    };

    private static bool TryParseDecimal(string type, out int precision, out int scale)
    {
        precision = scale = 0;
        if (!type.StartsWith("decimal(", StringComparison.Ordinal) || !type.EndsWith(")", StringComparison.Ordinal))
            return false;
        var parts = type.Substring(8, type.Length - 9).Split(',');
        return parts.Length == 2
            && int.TryParse(parts[0].Trim(), out precision)
            && int.TryParse(parts[1].Trim(), out scale);
    }

    private static bool LooksLikeReadMetadata(string name) =>
        name.StartsWith(DeltaMetadataColumns.DefaultPrefix, StringComparison.Ordinal)
        || string.Equals(name, TransientRowAddress.ColumnName, StringComparison.Ordinal);

    /// <summary>Refuses a batch name that matches a declared one only when case is ignored.</summary>
    private static void ThrowIfCaseOnly(IReadOnlyList<StructField> declared, string name, string display)
    {
        foreach (var f in declared)
        {
            if (string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.DuplicateColumnsFound,
                    $"mergeSchema: the batch's '{display}' differs only in case from the table's '{f.Name}'. Delta "
                    + "column names are case-insensitive, so it cannot be added as a new column, and it is not "
                    + $"written into '{f.Name}' either. Rename it in the batch to '{f.Name}'.");
            }
        }
    }

    /// <summary>The field as a merge adds it: nullable, and its nested fields, elements and map values too
    /// (Spark's <c>asNullable</c>).</summary>
    private static StructField AsNullable(StructField field) => new()
    {
        Name = field.Name,
        Type = AsNullable(field.Type),
        Nullable = true,
        Metadata = field.Metadata,
    };

    private static DeltaDataType AsNullable(DeltaDataType type) => type switch
    {
        StructType st => new StructType { Fields = st.Fields.Select(AsNullable).ToList() },
        ArrayType at => new ArrayType { ElementType = AsNullable(at.ElementType), ContainsNull = true },
        MapType mt => new MapType
        {
            KeyType = AsNullable(mt.KeyType), ValueType = AsNullable(mt.ValueType), ValueContainsNull = true,
        },
        _ => type,
    };

    /// <summary>
    /// The actions a <c>mergeSchema</c> write commits: its own, computed against the merged schema, plus the
    /// merge's protocol upgrade and <c>metaData</c>. A write that already emits a <c>metaData</c> (identity
    /// high-water marks, a repartition) built it on the merged one, so it is kept and the merge's is not added: a
    /// commit carries one.
    /// </summary>
    internal static IReadOnlyList<DeltaAction> WithSchemaMerge(
        IReadOnlyList<DeltaAction> actions, DeferredSchemaChange merge)
    {
        var result = new List<DeltaAction>(actions.Count + 2);
        if (merge.ProtocolUpgrade is { } protocol)
            result.Add(protocol);
        if (!actions.Any(a => a is MetadataAction))
            result.Add(merge.Metadata);
        result.AddRange(actions);
        return result;
    }
}
