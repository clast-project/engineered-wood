// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Schema;

namespace EngineeredWood.DeltaLake.Tests;

public class TypeWideningTests
{
    [Theory]
    [InlineData("byte", "short", true)]
    [InlineData("byte", "integer", true)]
    [InlineData("byte", "long", true)]
    [InlineData("short", "integer", true)]
    [InlineData("short", "long", true)]
    [InlineData("integer", "long", true)]
    [InlineData("float", "double", true)]
    [InlineData("byte", "double", true)]
    [InlineData("short", "double", true)]
    [InlineData("integer", "double", true)]
    [InlineData("date", "timestamp_ntz", true)]
    [InlineData("long", "double", false)]       // Not supported
    [InlineData("string", "long", false)]        // Not supported
    [InlineData("long", "integer", false)]       // Narrowing, not widening
    [InlineData("double", "float", false)]       // Narrowing
    public void IsSupported_Checks(string from, string to, bool expected)
    {
        Assert.Equal(expected, TypeWidening.IsSupported(from, to));
    }

    [Theory]
    [InlineData("decimal(10,2)", "decimal(12,2)", true)]   // Precision increase
    [InlineData("decimal(10,2)", "decimal(12,4)", true)]   // Both increase, k1>=k2
    [InlineData("decimal(10,2)", "decimal(10,2)", true)]   // Same (k1=k2=0)
    [InlineData("decimal(10,4)", "decimal(12,2)", false)]  // Scale decrease
    [InlineData("integer", "decimal(12,2)", true)]         // Int → Decimal
    [InlineData("long", "decimal(22,2)", true)]            // Long → Decimal
    [InlineData("byte", "decimal(10,0)", true)]            // Byte → Decimal
    [InlineData("string", "decimal(10,2)", false)]         // Not supported
    public void IsDecimalWidening_Checks(string from, string to, bool expected)
    {
        Assert.Equal(expected, TypeWidening.IsDecimalWidening(from, to));
    }

    [Fact]
    public void ValidateTypeChange_Supported_NoThrow()
    {
        TypeWidening.ValidateTypeChange("integer", "long");
        TypeWidening.ValidateTypeChange("float", "double");
        TypeWidening.ValidateTypeChange("decimal(10,2)", "decimal(12,2)");
    }

    [Fact]
    public void ValidateTypeChange_Unsupported_Throws()
    {
        Assert.Throws<DeltaFormatException>(
            () => TypeWidening.ValidateTypeChange("string", "long"));
    }

    [Fact]
    public void ParseAndSerialize_TypeChanges_RoundTrip()
    {
        string json = """[{"fromType":"integer","toType":"long"},{"fromType":"float","toType":"double","fieldPath":"value"}]""";

        var changes = TypeWidening.ParseTypeChanges(json);
        Assert.Equal(2, changes.Count);
        Assert.Equal("integer", changes[0].FromType);
        Assert.Equal("long", changes[0].ToType);
        Assert.Null(changes[0].FieldPath);
        Assert.Equal("float", changes[1].FromType);
        Assert.Equal("double", changes[1].ToType);
        Assert.Equal("value", changes[1].FieldPath);

        string serialized = TypeWidening.SerializeTypeChanges(changes);
        var reparsed = TypeWidening.ParseTypeChanges(serialized);
        Assert.Equal(2, reparsed.Count);
        Assert.Equal("integer", reparsed[0].FromType);
        Assert.Equal("value", reparsed[1].FieldPath);
    }

    [Fact]
    public void AddTypeChange_ToField()
    {
        var field = new StructField
        {
            Name = "amount",
            Type = new PrimitiveType { TypeName = "long" },
            Nullable = false,
        };

        var updated = TypeWidening.AddTypeChange(field, "integer", "long");

        Assert.NotNull(updated.Metadata);
        Assert.True(updated.Metadata!.ContainsKey(TypeWidening.TypeChangesKey));

        var changes = TypeWidening.GetTypeChanges(updated);
        Assert.Single(changes);
        Assert.Equal("integer", changes[0].FromType);
        Assert.Equal("long", changes[0].ToType);
    }

    [Fact]
    public void AddTypeChange_Accumulates()
    {
        var field = new StructField
        {
            Name = "amount",
            Type = new PrimitiveType { TypeName = "long" },
            Nullable = false,
        };

        field = TypeWidening.AddTypeChange(field, "short", "integer");
        field = TypeWidening.AddTypeChange(field, "integer", "long");

        var changes = TypeWidening.GetTypeChanges(field);
        Assert.Equal(2, changes.Count);
        Assert.Equal("short", changes[0].FromType);
        Assert.Equal("integer", changes[1].FromType);
    }

    [Fact]
    public void IsEnabled_Configuration()
    {
        Assert.False(TypeWidening.IsEnabled(null));
        Assert.False(TypeWidening.IsEnabled(new Dictionary<string, string>()));
        Assert.True(TypeWidening.IsEnabled(new Dictionary<string, string>
        {
            { TypeWidening.EnableKey, "true" },
        }));
    }
}
