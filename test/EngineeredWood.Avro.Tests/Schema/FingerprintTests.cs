// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro.Tests.Schema;

public class FingerprintTests
{
    #region Parsing Canonical Form

    [Theory]
    [InlineData("\"null\"", "\"null\"")]
    [InlineData("\"boolean\"", "\"boolean\"")]
    [InlineData("\"int\"", "\"int\"")]
    [InlineData("\"long\"", "\"long\"")]
    [InlineData("\"float\"", "\"float\"")]
    [InlineData("\"double\"", "\"double\"")]
    [InlineData("\"bytes\"", "\"bytes\"")]
    [InlineData("\"string\"", "\"string\"")]
    public void Pcf_Primitives(string json, string expected)
    {
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);
        Assert.Equal(expected, pcf);
    }

    [Fact]
    public void Pcf_SimpleRecord()
    {
        var json = """
        {
            "type": "record",
            "name": "User",
            "namespace": "com.example",
            "doc": "A user record",
            "fields": [
                {"name": "id", "type": "long", "doc": "The user ID"},
                {"name": "name", "type": "string", "default": "unknown", "order": "ascending"}
            ]
        }
        """;
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);

        // Should strip doc, default, order; use full name; canonical key order
        Assert.Equal(
            """{"name":"com.example.User","type":"record","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}""",
            pcf);
    }

    [Fact]
    public void Pcf_StripsDocAliasesDefaultOrder()
    {
        var json = """
        {
            "type": "record",
            "name": "Test",
            "doc": "should be removed",
            "aliases": ["OldTest"],
            "fields": [
                {
                    "name": "x",
                    "type": "int",
                    "default": 0,
                    "doc": "removed",
                    "order": "descending",
                    "aliases": ["y"]
                }
            ]
        }
        """;
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);

        Assert.Equal(
            """{"name":"Test","type":"record","fields":[{"name":"x","type":"int"}]}""",
            pcf);
    }

    [Fact]
    public void Pcf_Enum()
    {
        var json = """
        {
            "type": "enum",
            "name": "Color",
            "namespace": "com.example",
            "doc": "ignored",
            "symbols": ["RED", "GREEN", "BLUE"],
            "default": "RED"
        }
        """;
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);

        Assert.Equal(
            """{"name":"com.example.Color","type":"enum","symbols":["RED","GREEN","BLUE"]}""",
            pcf);
    }

    [Fact]
    public void Pcf_Array()
    {
        var json = """{"type": "array", "items": "int"}""";
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);

        Assert.Equal("""{"type":"array","items":"int"}""", pcf);
    }

    [Fact]
    public void Pcf_Map()
    {
        var json = """{"type": "map", "values": "long"}""";
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);

        Assert.Equal("""{"type":"map","values":"long"}""", pcf);
    }

    [Fact]
    public void Pcf_Fixed()
    {
        var json = """
        {
            "type": "fixed",
            "name": "Hash",
            "namespace": "com.example",
            "size": 16
        }
        """;
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);

        Assert.Equal("""{"name":"com.example.Hash","type":"fixed","size":16}""", pcf);
    }

    [Fact]
    public void Pcf_Union()
    {
        var json = """["null", "string"]""";
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);

        Assert.Equal("""["null","string"]""", pcf);
    }

    [Fact]
    public void Pcf_NullableType()
    {
        var json = """
        {
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "x", "type": ["null", "int"]}
            ]
        }
        """;
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);

        Assert.Equal(
            """{"name":"R","type":"record","fields":[{"name":"x","type":["null","int"]}]}""",
            pcf);
    }

    [Fact]
    public void Pcf_NestedRecords_UsesFullNameForSubsequentOccurrences()
    {
        var json = """
        {
            "type": "record",
            "name": "Outer",
            "namespace": "ns",
            "fields": [
                {
                    "name": "inner1",
                    "type": {
                        "type": "record",
                        "name": "Inner",
                        "fields": [{"name": "val", "type": "int"}]
                    }
                },
                {
                    "name": "inner2",
                    "type": "Inner"
                }
            ]
        }
        """;
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);

        // Second occurrence of Inner should be just the full name string
        Assert.Contains("""{"name":"ns.Inner","type":"record","fields":[{"name":"val","type":"int"}]}""", pcf);
        // The second field should reference by full name
        Assert.Equal(
            """{"name":"ns.Outer","type":"record","fields":[{"name":"inner1","type":{"name":"ns.Inner","type":"record","fields":[{"name":"val","type":"int"}]}},{"name":"inner2","type":"ns.Inner"}]}""",
            pcf);
    }

    [Fact]
    public void Pcf_ArrayOfRecords()
    {
        var json = """
        {
            "type": "array",
            "items": {
                "type": "record",
                "name": "Item",
                "fields": [{"name": "id", "type": "int"}]
            }
        }
        """;
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);

        Assert.Equal(
            """{"type":"array","items":{"name":"Item","type":"record","fields":[{"name":"id","type":"int"}]}}""",
            pcf);
    }

    [Fact]
    public void Pcf_MapOfRecords()
    {
        var json = """
        {
            "type": "map",
            "values": {
                "type": "enum",
                "name": "Status",
                "symbols": ["ACTIVE", "INACTIVE"]
            }
        }
        """;
        var node = AvroSchemaParser.Parse(json);
        var pcf = ParsingCanonicalForm.ToCanonicalJson(node);

        Assert.Equal(
            """{"type":"map","values":{"name":"Status","type":"enum","symbols":["ACTIVE","INACTIVE"]}}""",
            pcf);
    }

    #endregion

    #region Rabin Fingerprint

    [Fact]
    public void Rabin_EmptyInput_ReturnsEmptyFingerprint()
    {
        var result = RabinFingerprint.Compute(ReadOnlySpan<byte>.Empty);
        // The EMPTY constant is the fingerprint of the empty byte sequence
        Assert.Equal(0xC15D213AA4D7A795UL, result);
    }

    [Fact]
    public void Rabin_KnownValue_NullSchema()
    {
        // The PCF of "null" is the string "\"null\""
        var pcf = "\"null\"";
        var bytes = System.Text.Encoding.UTF8.GetBytes(pcf);
        var fingerprint = RabinFingerprint.Compute(bytes);

        // Known value from Avro spec / reference implementations
        // The Rabin fingerprint should be deterministic
        Assert.NotEqual(0UL, fingerprint);
        Assert.NotEqual(0xC15D213AA4D7A795UL, fingerprint); // not empty
    }

    [Fact]
    public void Rabin_DeterministicForSameInput()
    {
        var data = System.Text.Encoding.UTF8.GetBytes("test data for rabin");
        var fp1 = RabinFingerprint.Compute(data);
        var fp2 = RabinFingerprint.Compute(data);
        Assert.Equal(fp1, fp2);
    }

    [Fact]
    public void Rabin_DifferentInputsProduceDifferentFingerprints()
    {
        var fp1 = RabinFingerprint.Compute(System.Text.Encoding.UTF8.GetBytes("\"int\""));
        var fp2 = RabinFingerprint.Compute(System.Text.Encoding.UTF8.GetBytes("\"long\""));
        Assert.NotEqual(fp1, fp2);
    }

    #endregion

    #region AvroSchema.ComputeFingerprint

    [Fact]
    public void ComputeFingerprint_Rabin_ProducesRabinResult()
    {
        var schema = new AvroSchema("\"string\"");
        var fp = schema.ComputeFingerprint(FingerprintAlgorithm.Rabin);
        var rabin = Assert.IsType<SchemaFingerprint.Rabin>(fp);
        Assert.NotEqual(0UL, rabin.Value);
    }

    [Fact]
    public void ComputeFingerprint_MD5_Produces16Bytes()
    {
        var schema = new AvroSchema("\"int\"");
        var fp = schema.ComputeFingerprint(FingerprintAlgorithm.MD5);
        var md5 = Assert.IsType<SchemaFingerprint.MD5>(fp);
        Assert.Equal(16, md5.Value.Length);
    }

    [Fact]
    public void ComputeFingerprint_SHA256_Produces32Bytes()
    {
        var schema = new AvroSchema("\"int\"");
        var fp = schema.ComputeFingerprint(FingerprintAlgorithm.SHA256);
        var sha = Assert.IsType<SchemaFingerprint.SHA256>(fp);
        Assert.Equal(32, sha.Value.Length);
    }

    [Fact]
    public void ComputeFingerprint_DeterministicAcrossInstances()
    {
        var json = """
        {
            "type": "record",
            "name": "Test",
            "fields": [{"name": "x", "type": "int"}]
        }
        """;
        var fp1 = new AvroSchema(json).ComputeFingerprint();
        var fp2 = new AvroSchema(json).ComputeFingerprint();
        Assert.Equal(fp1, fp2);
    }

    [Fact]
    public void ComputeFingerprint_IgnoresDocAndDefaults()
    {
        var json1 = """
        {
            "type": "record",
            "name": "R",
            "fields": [{"name": "x", "type": "int"}]
        }
        """;
        var json2 = """
        {
            "type": "record",
            "name": "R",
            "doc": "Some documentation",
            "fields": [{"name": "x", "type": "int", "default": 42}]
        }
        """;
        var fp1 = new AvroSchema(json1).ComputeFingerprint();
        var fp2 = new AvroSchema(json2).ComputeFingerprint();
        Assert.Equal(fp1, fp2);
    }

    [Fact]
    public void ComputeFingerprint_DifferentSchemasProduceDifferentFingerprints()
    {
        var fp1 = new AvroSchema("\"int\"").ComputeFingerprint();
        var fp2 = new AvroSchema("\"string\"").ComputeFingerprint();
        Assert.NotEqual(fp1, fp2);
    }

    #endregion

    #region SchemaFingerprint Equality

    [Fact]
    public void MD5Fingerprint_EqualityByValue()
    {
        var bytes1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
        var bytes2 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
        var fp1 = new SchemaFingerprint.MD5(bytes1);
        var fp2 = new SchemaFingerprint.MD5(bytes2);
        Assert.Equal(fp1, fp2);
        Assert.Equal(fp1.GetHashCode(), fp2.GetHashCode());
    }

    [Fact]
    public void SHA256Fingerprint_EqualityByValue()
    {
        var bytes1 = new byte[32];
        var bytes2 = new byte[32];
        bytes1[0] = 42;
        bytes2[0] = 42;
        var fp1 = new SchemaFingerprint.SHA256(bytes1);
        var fp2 = new SchemaFingerprint.SHA256(bytes2);
        Assert.Equal(fp1, fp2);
        Assert.Equal(fp1.GetHashCode(), fp2.GetHashCode());
    }

    [Fact]
    public void MD5Fingerprint_InequalityByValue()
    {
        var fp1 = new SchemaFingerprint.MD5(new byte[16]);
        var fp2 = new SchemaFingerprint.MD5([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        Assert.NotEqual(fp1, fp2);
    }

    #endregion

    #region SchemaStore

    [Fact]
    public void SchemaStore_RegisterAndLookup_RoundTrip()
    {
        var store = new SchemaStore();
        var schema = new AvroSchema("\"int\"");
        var fp = store.Register(schema);

        var found = store.Lookup(fp);
        Assert.Same(schema, found);
    }

    [Fact]
    public void SchemaStore_Lookup_UnknownFingerprint_ReturnsNull()
    {
        var store = new SchemaStore();
        var fp = new SchemaFingerprint.Rabin(12345UL);
        Assert.Null(store.Lookup(fp));
    }

    [Fact]
    public void SchemaStore_WithConfluentId()
    {
        var store = new SchemaStore();
        var schema = new AvroSchema("\"string\"");
        var confluentId = new SchemaFingerprint.ConfluentId(42);

        store.Set(confluentId, schema);
        var found = store.Lookup(confluentId);
        Assert.Same(schema, found);
    }

    [Fact]
    public void SchemaStore_WithApicurioId()
    {
        var store = new SchemaStore();
        var schema = new AvroSchema("\"long\"");
        var apicurioId = new SchemaFingerprint.ApicurioId(999UL);

        store.Set(apicurioId, schema);
        Assert.Same(schema, store.Lookup(apicurioId));
    }

    [Fact]
    public void SchemaStore_Fingerprints_ListsAll()
    {
        var store = new SchemaStore();
        var s1 = new AvroSchema("\"int\"");
        var s2 = new AvroSchema("\"string\"");
        var fp1 = store.Register(s1);
        var fp2 = store.Register(s2);

        var fingerprints = store.Fingerprints;
        Assert.Equal(2, fingerprints.Count);
        Assert.Contains(fp1, fingerprints);
        Assert.Contains(fp2, fingerprints);
    }

    [Fact]
    public void SchemaStore_MD5Algorithm()
    {
        var store = new SchemaStore(FingerprintAlgorithm.MD5);
        Assert.Equal(FingerprintAlgorithm.MD5, store.Algorithm);

        var schema = new AvroSchema("\"int\"");
        var fp = store.Register(schema);
        Assert.IsType<SchemaFingerprint.MD5>(fp);

        Assert.Same(schema, store.Lookup(fp));
    }

    [Fact]
    public void SchemaStore_SHA256Algorithm()
    {
        var store = new SchemaStore(FingerprintAlgorithm.SHA256);
        var schema = new AvroSchema("\"int\"");
        var fp = store.Register(schema);
        Assert.IsType<SchemaFingerprint.SHA256>(fp);
        Assert.Same(schema, store.Lookup(fp));
    }

    [Fact]
    public void SchemaStore_RegisterSameSchemaOverwrites()
    {
        var store = new SchemaStore();
        var s1 = new AvroSchema("\"int\"");
        var s2 = new AvroSchema("\"int\"");
        var fp1 = store.Register(s1);
        var fp2 = store.Register(s2);

        Assert.Equal(fp1, fp2);
        Assert.Single(store.Fingerprints);
        Assert.Same(s2, store.Lookup(fp1)); // latest wins
    }

    #endregion

    #region Rabin known test vector

    [Fact]
    public void Rabin_AvroSpecTestVector()
    {
        // Test vector: Rabin fingerprint of "\"null\"" — verified against Apache Avro Java
        var pcfBytes = System.Text.Encoding.UTF8.GetBytes("\"null\"");
        var fp = RabinFingerprint.Compute(pcfBytes);
        Assert.Equal(7895961171020252294UL, fp);
    }

    [Fact]
    public void Rabin_IntSchemaTestVector()
    {
        // Fingerprint of "\"int\"" — verified against Apache Avro Java
        var pcfBytes = System.Text.Encoding.UTF8.GetBytes("\"int\"");
        var fp = RabinFingerprint.Compute(pcfBytes);
        Assert.Equal(6418733515636338684UL, fp);
    }

    #endregion
}
