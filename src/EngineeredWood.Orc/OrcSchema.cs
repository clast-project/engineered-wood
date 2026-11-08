// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Orc.Proto;
using Type = EngineeredWood.Orc.Proto.Type;

namespace EngineeredWood.Orc;

/// <summary>
/// Represents the schema of an ORC file as a tree of typed columns.
/// </summary>
public sealed class OrcSchema
{
    public int ColumnId { get; }
    public Type.Types.Kind Kind { get; }
    public string? Name { get; }
    public IReadOnlyList<OrcSchema> Children { get; }
    public uint MaximumLength { get; }
    public uint Precision { get; }
    public uint Scale { get; }

    private OrcSchema(int columnId, Type.Types.Kind kind, string? name, IReadOnlyList<OrcSchema> children, Type type)
    {
        ColumnId = columnId;
        Kind = kind;
        Name = name;
        Children = children;
        MaximumLength = type.HasMaximumLength ? type.MaximumLength : 0;
        Precision = type.HasPrecision ? type.Precision : 0;
        Scale = type.HasScale ? type.Scale : 0;

        // ORC's decimal is at most 38 digits with 0 <= scale <= precision, and the reader builds a
        // Decimal128 for it. Precision 0 is a Hive 0.11 decimal, read with fixed defaults whatever
        // its scale says. Anything else would hand the footer's scale to Arrow as it is, and the
        // reader, ArrowRowEvaluator.Rescale and the Spark casts size a power of ten by it (#763).
        if (kind == Type.Types.Kind.Decimal && Precision > 0 && (Precision > MaxDecimalPrecision || Scale > Precision))
            throw new InvalidDataException(
                $"ORC column {columnId} declares decimal({Precision},{Scale}); ORC requires precision <= {MaxDecimalPrecision} and scale <= precision.");
    }

    private const uint MaxDecimalPrecision = 38;

    public static OrcSchema FromFooter(Footer footer)
    {
        var types = footer.Types_;
        if (types.Count == 0)
            throw new InvalidDataException("ORC file has no types in footer.");
        return BuildTree(types, 0, null);
    }

    private static OrcSchema BuildTree(IReadOnlyList<Type> types, int typeId, string? name)
    {
        var type = types[typeId];
        var children = new List<OrcSchema>();

        for (int i = 0; i < type.Subtypes.Count; i++)
        {
            var childId = (int)type.Subtypes[i];
            var childName = i < type.FieldNames.Count ? type.FieldNames[i] : null;
            children.Add(BuildTree(types, childId, childName));
        }

        return new OrcSchema(typeId, type.Kind, name, children, type);
    }

    /// <summary>
    /// Converts this ORC schema to an Apache Arrow schema.
    /// </summary>
    public Schema ToArrowSchema()
    {
        if (Kind != Type.Types.Kind.Struct)
            throw new InvalidOperationException("Root type must be STRUCT to convert to Arrow schema.");

        var fields = new List<Field>();
        foreach (var child in Children)
        {
            fields.Add(new Field(child.Name ?? $"col_{child.ColumnId}", child.ToArrowType(), nullable: true));
        }
        return new Schema(fields, null);
    }

    public IArrowType ToArrowType()
    {
        return Kind switch
        {
            Type.Types.Kind.Boolean => BooleanType.Default,
            Type.Types.Kind.Byte => Int8Type.Default,
            Type.Types.Kind.Short => Int16Type.Default,
            Type.Types.Kind.Int => Int32Type.Default,
            Type.Types.Kind.Long => Int64Type.Default,
            Type.Types.Kind.Float => FloatType.Default,
            Type.Types.Kind.Double => DoubleType.Default,
            Type.Types.Kind.String => StringType.Default,
            Type.Types.Kind.Varchar => StringType.Default,
            Type.Types.Kind.Char => StringType.Default,
            Type.Types.Kind.Binary => BinaryType.Default,
            Type.Types.Kind.Date => Date32Type.Default,
            Type.Types.Kind.Timestamp => new TimestampType(TimeUnit.Nanosecond, (string?)null),
            Type.Types.Kind.TimestampInstant => new TimestampType(TimeUnit.Nanosecond, "UTC"),
            // Hive 0.11 wrote decimals with no precision or scale: read them as the C++ reader does.
            Type.Types.Kind.Decimal => Precision > 0
                ? new Decimal128Type((int)Precision, (int)Scale)
                : new Decimal128Type(ColumnReaders.DecimalColumnReader.Hive11Precision, ColumnReaders.DecimalColumnReader.Hive11Scale),
            Type.Types.Kind.List => new ListType(Children[0].ToArrowType()),
            Type.Types.Kind.Map => new MapType(
                Children[0].ToArrowType(),
                Children[1].ToArrowType()),
            Type.Types.Kind.Struct => new StructType(
                Children.Select(c => new Field(c.Name ?? $"col_{c.ColumnId}", c.ToArrowType(), nullable: true)).ToList()),
            Type.Types.Kind.Union => new UnionType(
                Children.Select((c, i) => new Field(c.Name ?? $"col_{c.ColumnId}", c.ToArrowType(), nullable: true)).ToList(),
                Children.Select((_, i) => i).ToArray(),
                UnionMode.Dense),
            _ => throw new NotSupportedException($"ORC type {Kind} is not yet supported.")
        };
    }

    /// <summary>
    /// Gets the set of column IDs needed to read the given column names.
    /// Includes all descendant columns for complex types.
    /// </summary>
    public HashSet<int> GetColumnIds(IReadOnlyList<string>? columnNames)
    {
        if (columnNames == null || columnNames.Count == 0)
        {
            // All columns
            var all = new HashSet<int>();
            CollectAllIds(this, all);
            return all;
        }

        var result = new HashSet<int> { ColumnId }; // always include root struct
        foreach (var name in columnNames)
        {
            var child = Children.FirstOrDefault(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));
            if (child == null)
                throw new ArgumentException($"Column '{name}' not found in schema.");
            CollectAllIds(child, result);
        }
        return result;
    }

    private static void CollectAllIds(OrcSchema node, HashSet<int> ids)
    {
        ids.Add(node.ColumnId);
        foreach (var child in node.Children)
            CollectAllIds(child, ids);
    }
}
