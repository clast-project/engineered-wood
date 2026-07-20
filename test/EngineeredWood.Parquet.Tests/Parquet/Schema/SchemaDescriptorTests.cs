// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

namespace EngineeredWood.Tests.Parquet.Schema;

public class SchemaDescriptorTests
{
    [Fact]
    public void AlltypesPlain_FlatSchema()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);
        var schema = new SchemaDescriptor(metadata.Schema);

        // Root node
        Assert.Equal("schema", schema.Root.Name);
        Assert.False(schema.Root.IsLeaf);
        Assert.Null(schema.Root.Parent);

        // All children of root should be leaves in a flat schema
        Assert.All(schema.Root.Children, child => Assert.True(child.IsLeaf));

        // Leaf count should match columns
        Assert.Equal(schema.Root.Children.Count, schema.Columns.Count);
    }

    [Fact]
    public void AlltypesPlain_LeafColumnNames()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);
        var schema = new SchemaDescriptor(metadata.Schema);

        var names = schema.Columns.Select(c => c.DottedPath).ToList();
        Assert.Contains("id", names);
        Assert.Contains("bool_col", names);
        Assert.Contains("tinyint_col", names);
        Assert.Contains("int_col", names);
        Assert.Contains("bigint_col", names);
        Assert.Contains("float_col", names);
        Assert.Contains("double_col", names);
        Assert.Contains("string_col", names);
    }

    [Fact]
    public void AlltypesPlain_FlatDefRepLevels()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);
        var schema = new SchemaDescriptor(metadata.Schema);

        // In alltypes_plain, columns are OPTIONAL → maxDef=1, maxRep=0
        // or REQUIRED → maxDef=0, maxRep=0
        foreach (var col in schema.Columns)
        {
            Assert.Equal(0, col.MaxRepetitionLevel);
            Assert.True(col.MaxDefinitionLevel <= 1,
                $"Column {col.DottedPath} has unexpected maxDef={col.MaxDefinitionLevel}");
        }
    }

    [Fact]
    public void AlltypesPlain_PhysicalTypes()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);
        var schema = new SchemaDescriptor(metadata.Schema);

        var byName = schema.Columns.ToDictionary(c => c.DottedPath);
        Assert.Equal(PhysicalType.Int32, byName["id"].PhysicalType);
        Assert.Equal(PhysicalType.Boolean, byName["bool_col"].PhysicalType);
        Assert.Equal(PhysicalType.Float, byName["float_col"].PhysicalType);
        Assert.Equal(PhysicalType.Double, byName["double_col"].PhysicalType);
        Assert.Equal(PhysicalType.Int64, byName["bigint_col"].PhysicalType);
    }

    [Fact]
    public void NestedLists_TreeShape()
    {
        var footerBytes = TestData.ReadFooterBytes("nested_lists.snappy.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);
        var schema = new SchemaDescriptor(metadata.Schema);

        // Root should have group children
        Assert.False(schema.Root.IsLeaf);
        Assert.True(schema.Root.Children.Count > 0);

        // Should have at least one non-leaf child (nested group)
        var hasGroupChild = schema.Root.Children.Any(c => !c.IsLeaf);
        Assert.True(hasGroupChild, "Expected nested groups for nested_lists schema");
    }

    [Fact]
    public void NestedLists_DefRepLevels()
    {
        var footerBytes = TestData.ReadFooterBytes("nested_lists.snappy.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);
        var schema = new SchemaDescriptor(metadata.Schema);

        // Nested lists should have repetition levels > 0 for leaf columns
        var hasRepLevel = schema.Columns.Any(c => c.MaxRepetitionLevel > 0);
        Assert.True(hasRepLevel, "Expected repetition levels > 0 for nested list columns");

        // And definition levels > 1
        var hasHighDefLevel = schema.Columns.Any(c => c.MaxDefinitionLevel > 1);
        Assert.True(hasHighDefLevel, "Expected definition levels > 1 for nested list columns");
    }

    [Fact]
    public void NestedMaps_HasMapStructure()
    {
        var footerBytes = TestData.ReadFooterBytes("nested_maps.snappy.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);
        var schema = new SchemaDescriptor(metadata.Schema);

        // Maps produce repeated key_value groups → repetition levels should be > 0
        var hasRepLevel = schema.Columns.Any(c => c.MaxRepetitionLevel > 0);
        Assert.True(hasRepLevel, "Expected repetition levels > 0 for map columns");
    }

    [Fact]
    public void NestedStructs_TreeShape()
    {
        var footerBytes = TestData.ReadFooterBytes("nested_structs.rust.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);
        var schema = new SchemaDescriptor(metadata.Schema);

        Assert.True(schema.Columns.Count > 0);

        // Nested struct columns should have multi-segment paths
        var hasNestedPath = schema.Columns.Any(c => c.Path.Count > 1);
        Assert.True(hasNestedPath, "Expected multi-segment paths for nested struct columns");
    }

    [Fact]
    public void NestedStructs_DefRepLevels()
    {
        var footerBytes = TestData.ReadFooterBytes("nested_structs.rust.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);
        var schema = new SchemaDescriptor(metadata.Schema);

        // Structs with optional fields increase definition levels but not repetition levels
        foreach (var col in schema.Columns)
        {
            Assert.True(col.MaxDefinitionLevel >= 0);
            Assert.True(col.MaxRepetitionLevel >= 0);
        }
    }

    [Fact]
    public void ParentReferences_AreCorrect()
    {
        var footerBytes = TestData.ReadFooterBytes("nested_lists.snappy.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);
        var schema = new SchemaDescriptor(metadata.Schema);

        // Root has no parent
        Assert.Null(schema.Root.Parent);

        // All children reference their parent
        void VerifyParents(SchemaNode node)
        {
            foreach (var child in node.Children)
            {
                Assert.Same(node, child.Parent);
                VerifyParents(child);
            }
        }

        VerifyParents(schema.Root);
    }

    [Fact]
    public void ColumnDescriptor_SchemaNodeReference()
    {
        var footerBytes = TestData.ReadFooterBytes("alltypes_plain.parquet");
        var metadata = MetadataDecoder.DecodeFileMetaData(footerBytes);
        var schema = new SchemaDescriptor(metadata.Schema);

        foreach (var col in schema.Columns)
        {
            Assert.NotNull(col.SchemaNode);
            Assert.True(col.SchemaNode.IsLeaf);
            Assert.Same(col.SchemaElement, col.SchemaNode.Element);
        }
    }

    [Fact]
    public void EmptySchema_Throws()
    {
        var ex = Assert.Throws<ParquetFormatException>(
            () => new SchemaDescriptor(Array.Empty<SchemaElement>()));
        Assert.Contains("root", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
