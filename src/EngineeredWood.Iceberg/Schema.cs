// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// An Iceberg table schema consisting of a unique identifier and a list of top-level fields.
/// Each schema version is immutable; schema evolution produces new <see cref="Schema"/> instances.
/// </summary>
/// <param name="SchemaId">The unique identifier for this schema version.</param>
/// <param name="Fields">The top-level columns in the schema.</param>
/// <param name="IdentifierFieldIds">Optional set of field IDs that form the row identifier.</param>
public sealed record Schema(
    int SchemaId,
    IReadOnlyList<NestedField> Fields,
    IReadOnlyList<int>? IdentifierFieldIds = null)
{
    /// <summary>Returns the schema fields wrapped in a <see cref="StructType"/>.</summary>
    public StructType AsStruct() => new(Fields);
}
