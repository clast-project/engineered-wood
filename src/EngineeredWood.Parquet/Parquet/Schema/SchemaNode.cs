// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Parquet.Schema;

/// <summary>
/// A node in the reconstructed schema tree.
/// Group nodes have children; leaf nodes have a physical type.
/// </summary>
public sealed class SchemaNode
{
    /// <summary>The underlying schema element from the flattened Parquet metadata.</summary>
    public required SchemaElement Element { get; init; }

    /// <summary>Parent node. Null for the root.</summary>
    public SchemaNode? Parent { get; init; }

    /// <summary>Child nodes. Empty for leaf nodes.</summary>
    public required IReadOnlyList<SchemaNode> Children { get; init; }

    /// <summary>Whether this node is a leaf (has a physical type).</summary>
    public bool IsLeaf => Element.Type.HasValue;

    /// <summary>The name of this schema element.</summary>
    public string Name => Element.Name;
}
