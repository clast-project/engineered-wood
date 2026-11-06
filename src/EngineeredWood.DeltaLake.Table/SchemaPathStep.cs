// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Table;

/// <summary>What one <see cref="SchemaPathStep"/> steps into.</summary>
internal enum SchemaPathStepKind
{
    /// <summary>A named field of a struct.</summary>
    Field,

    /// <summary>An array's element.</summary>
    Element,

    /// <summary>A map's key.</summary>
    Key,

    /// <summary>A map's value.</summary>
    Value,
}

/// <summary>
/// One step of a LOGICAL path into a schema that may go through collections: a struct field by name, or an array
/// element or a map key or value. Kept apart from a field name because a field may itself be called
/// <c>element</c>.
/// </summary>
internal readonly record struct SchemaPathStep(SchemaPathStepKind Kind, string Name)
{
    public static SchemaPathStep Field(string name) => new(SchemaPathStepKind.Field, name);

    public static readonly SchemaPathStep Element = new(SchemaPathStepKind.Element, "element");
    public static readonly SchemaPathStep Key = new(SchemaPathStepKind.Key, "key");
    public static readonly SchemaPathStep Value = new(SchemaPathStepKind.Value, "value");

    public bool IsField => Kind == SchemaPathStepKind.Field;
}
