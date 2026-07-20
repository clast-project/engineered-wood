// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg.Tests;

public class SchemaUpdateTests
{
    private readonly TableMetadata _metadata;

    public SchemaUpdateTests()
    {
        var schema = new Schema(0, [
            new NestedField(1, "id", IcebergType.Long, true),
            new NestedField(2, "name", IcebergType.String, true),
            new NestedField(3, "score", IcebergType.Float, false),
        ]);

        _metadata = TableMetadata.Create(schema, location: "/test");
    }

    [Fact]
    public void AddColumn_AppendsFieldWithNewId()
    {
        var updated = new SchemaUpdate(_metadata)
            .AddColumn("email", IcebergType.String, doc: "user email")
            .Apply();

        Assert.Equal(1, updated.CurrentSchemaId);
        var schema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        Assert.Equal(4, schema.Fields.Count);

        var email = schema.Fields.Last();
        Assert.Equal(4, email.Id);
        Assert.Equal("email", email.Name);
        Assert.Equal(IcebergType.String, email.Type);
        Assert.False(email.IsRequired);
        Assert.Equal("user email", email.Doc);
        Assert.Equal(4, updated.LastColumnId);
    }

    [Fact]
    public void AddMultipleColumns_AssignsSequentialIds()
    {
        var updated = new SchemaUpdate(_metadata)
            .AddColumn("a", IcebergType.Int)
            .AddColumn("b", IcebergType.Int)
            .AddColumn("c", IcebergType.Int)
            .Apply();

        var schema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        Assert.Equal(6, schema.Fields.Count);
        Assert.Equal(4, schema.Fields[3].Id);
        Assert.Equal(5, schema.Fields[4].Id);
        Assert.Equal(6, schema.Fields[5].Id);
    }

    [Fact]
    public void DropColumn_RemovesField()
    {
        var updated = new SchemaUpdate(_metadata)
            .DropColumn("score")
            .Apply();

        var schema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        Assert.Equal(2, schema.Fields.Count);
        Assert.DoesNotContain(schema.Fields, f => f.Name == "score");
    }

    [Fact]
    public void RenameColumn_ChangesName()
    {
        var updated = new SchemaUpdate(_metadata)
            .RenameColumn("name", "full_name")
            .Apply();

        var schema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        var field = schema.Fields.First(f => f.Id == 2);
        Assert.Equal("full_name", field.Name);
    }

    [Fact]
    public void RenameColumn_PreservesId()
    {
        var updated = new SchemaUpdate(_metadata)
            .RenameColumn("name", "user_name")
            .Apply();

        var schema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        var field = schema.Fields.First(f => f.Name == "user_name");
        Assert.Equal(2, field.Id);
    }

    [Fact]
    public void UpdateType_IntToLong_Succeeds()
    {
        var schema = new Schema(0, [
            new NestedField(1, "count", IcebergType.Int, true),
        ]);
        var metadata = TableMetadata.Create(schema, location: "/test");

        var updated = new SchemaUpdate(metadata)
            .UpdateColumnType("count", IcebergType.Long)
            .Apply();

        var newSchema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        Assert.Equal(IcebergType.Long, newSchema.Fields[0].Type);
    }

    [Fact]
    public void UpdateType_FloatToDouble_Succeeds()
    {
        var updated = new SchemaUpdate(_metadata)
            .UpdateColumnType("score", IcebergType.Double)
            .Apply();

        var schema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        var field = schema.Fields.First(f => f.Name == "score");
        Assert.Equal(IcebergType.Double, field.Type);
    }

    [Fact]
    public void UpdateType_DecimalPrecisionIncrease_Succeeds()
    {
        var schema = new Schema(0, [
            new NestedField(1, "amount", IcebergType.Decimal(10, 2), true),
        ]);
        var metadata = TableMetadata.Create(schema, location: "/test");

        var updated = new SchemaUpdate(metadata)
            .UpdateColumnType("amount", IcebergType.Decimal(20, 2))
            .Apply();

        var newSchema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        var dec = (DecimalType)newSchema.Fields[0].Type;
        Assert.Equal(20, dec.Precision);
        Assert.Equal(2, dec.Scale);
    }

    [Fact]
    public void UpdateType_InvalidPromotion_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new SchemaUpdate(_metadata)
                .UpdateColumnType("name", IcebergType.Int)
                .Apply());
    }

    [Fact]
    public void UpdateType_DecimalScaleChange_Throws()
    {
        var schema = new Schema(0, [
            new NestedField(1, "amount", IcebergType.Decimal(10, 2), true),
        ]);
        var metadata = TableMetadata.Create(schema, location: "/test");

        Assert.Throws<ArgumentException>(() =>
            new SchemaUpdate(metadata)
                .UpdateColumnType("amount", IcebergType.Decimal(10, 4))
                .Apply());
    }

    [Fact]
    public void MakeOptional_ChangesRequiredToOptional()
    {
        var updated = new SchemaUpdate(_metadata)
            .MakeOptional("name")
            .Apply();

        var schema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        var field = schema.Fields.First(f => f.Name == "name");
        Assert.False(field.IsRequired);
    }

    [Fact]
    public void SetIdentifierFields_SetsIds()
    {
        var updated = new SchemaUpdate(_metadata)
            .SetIdentifierFields("id")
            .Apply();

        var schema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        Assert.NotNull(schema.IdentifierFieldIds);
        Assert.Single(schema.IdentifierFieldIds);
        Assert.Equal(1, schema.IdentifierFieldIds[0]);
    }

    [Fact]
    public void CombinedOperations_AllApplied()
    {
        var updated = new SchemaUpdate(_metadata)
            .AddColumn("email", IcebergType.String)
            .DropColumn("score")
            .RenameColumn("name", "full_name")
            .MakeOptional("full_name")
            .Apply();

        var schema = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        Assert.Equal(3, schema.Fields.Count);

        Assert.Contains(schema.Fields, f => f.Name == "id");
        Assert.Contains(schema.Fields, f => f.Name == "full_name" && !f.IsRequired);
        Assert.Contains(schema.Fields, f => f.Name == "email");
        Assert.DoesNotContain(schema.Fields, f => f.Name == "score");
    }

    [Fact]
    public void Apply_PreservesOriginalSchema()
    {
        var updated = new SchemaUpdate(_metadata)
            .AddColumn("x", IcebergType.Int)
            .Apply();

        Assert.Equal(2, updated.Schemas.Count);

        var original = updated.Schemas.First(s => s.SchemaId == 0);
        Assert.Equal(3, original.Fields.Count);

        var current = updated.Schemas.First(s => s.SchemaId == updated.CurrentSchemaId);
        Assert.Equal(4, current.Fields.Count);
    }

    [Fact]
    public void MultipleEvolutions_ChainCorrectly()
    {
        var v1 = new SchemaUpdate(_metadata)
            .AddColumn("email", IcebergType.String)
            .Apply();

        var v2 = new SchemaUpdate(v1)
            .AddColumn("age", IcebergType.Int)
            .DropColumn("score")
            .Apply();

        Assert.Equal(3, v2.Schemas.Count);
        Assert.Equal(2, v2.CurrentSchemaId);

        var schema = v2.Schemas.First(s => s.SchemaId == 2);
        Assert.Equal(4, schema.Fields.Count);
        Assert.Contains(schema.Fields, f => f.Name == "email");
        Assert.Contains(schema.Fields, f => f.Name == "age");
        Assert.DoesNotContain(schema.Fields, f => f.Name == "score");
    }
}
