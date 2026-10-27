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
    /// <summary>
    /// The underlying schema element from the flattened Parquet metadata, less any type annotation
    /// the reader does not honour (an unrecognized logical type, or one the spec does not allow on
    /// this physical type). <c>FileMetaData.Schema</c> keeps the annotations as written.
    /// </summary>
    public required SchemaElement Element { get; init; }

    /// <summary>
    /// Whether the annotation that defined this column's sort order was dropped (see
    /// <see cref="Element"/>). Its min/max statistics were ordered by that annotation, so they are
    /// not used either.
    /// </summary>
    internal bool AnnotationIgnored { get; init; }

    /// <summary>Parent node. Null for the root.</summary>
    public SchemaNode? Parent { get; init; }

    /// <summary>Child nodes. Empty for leaf nodes.</summary>
    public required IReadOnlyList<SchemaNode> Children { get; init; }

    /// <summary>Whether this node is a leaf (has a physical type).</summary>
    public bool IsLeaf => Element.Type.HasValue;

    /// <summary>The name of this schema element.</summary>
    public string Name => Element.Name;
}
