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
    }

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
            Type.Types.Kind.Decimal => new Decimal128Type(
                Precision > 0 ? (int)Precision : 38,
                Scale > 0 ? (int)Scale : 0),
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
