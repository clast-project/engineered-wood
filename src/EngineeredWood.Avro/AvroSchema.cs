// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro;

/// <summary>
/// Represents a parsed Avro schema (JSON). This is the library's fundamental schema type.
/// </summary>
public sealed class AvroSchema
{
    private readonly string _json;
    private AvroSchemaNode? _parsed;

    /// <summary>The raw JSON text of the Avro schema.</summary>
    public string Json => _json;

    public AvroSchema(string json)
    {
        _json = json ?? throw new ArgumentNullException(nameof(json));
    }

    internal AvroSchemaNode Parsed => _parsed ??= AvroSchemaParser.Parse(_json);

    /// <summary>Convert this Avro schema to an equivalent Arrow schema.</summary>
    public Apache.Arrow.Schema ToArrowSchema()
    {
        if (Parsed is AvroRecordSchema record)
            return ArrowSchemaConverter.ToArrow(record);
        throw new InvalidOperationException("Only record schemas can be converted to Arrow schemas.");
    }

    /// <summary>
    /// Computes a fingerprint of this schema using the specified algorithm.
    /// The fingerprint is computed over the Parsing Canonical Form (PCF) of the schema.
    /// </summary>
    public SchemaFingerprint ComputeFingerprint(FingerprintAlgorithm algorithm = FingerprintAlgorithm.Rabin)
    {
        var pcf = ParsingCanonicalForm.ToCanonicalJson(Parsed);
        var pcfBytes = System.Text.Encoding.UTF8.GetBytes(pcf);

        return algorithm switch
        {
            FingerprintAlgorithm.Rabin => new SchemaFingerprint.Rabin(RabinFingerprint.Compute(pcfBytes)),
#if NET8_0_OR_GREATER
            FingerprintAlgorithm.MD5 => new SchemaFingerprint.MD5(System.Security.Cryptography.MD5.HashData(pcfBytes)),
            FingerprintAlgorithm.SHA256 => new SchemaFingerprint.SHA256(System.Security.Cryptography.SHA256.HashData(pcfBytes)),
#else
            FingerprintAlgorithm.MD5 => new SchemaFingerprint.MD5(ComputeHash<System.Security.Cryptography.MD5>(pcfBytes)),
            FingerprintAlgorithm.SHA256 => new SchemaFingerprint.SHA256(ComputeHash<System.Security.Cryptography.SHA256>(pcfBytes)),
#endif
            _ => throw new ArgumentOutOfRangeException(nameof(algorithm)),
        };
    }

    /// <summary>
    /// Create a projected copy keeping only the specified top-level record field indices.
    /// </summary>
    public AvroSchema Project(ReadOnlySpan<int> fieldIndices)
    {
        if (Parsed is not AvroRecordSchema record)
            throw new InvalidOperationException("Only record schemas can be projected.");

        var projectedFields = new List<AvroFieldNode>();
        foreach (int idx in fieldIndices)
        {
            if (idx < 0 || idx >= record.Fields.Count)
                throw new ArgumentOutOfRangeException(nameof(fieldIndices), $"Field index {idx} out of range.");
            projectedFields.Add(record.Fields[idx]);
        }

        var projectedRecord = new AvroRecordSchema(record.Name, record.Namespace, projectedFields);
        return new AvroSchema(AvroSchemaWriter.ToJson(projectedRecord));
    }

    /// <summary>Create an Avro schema from an Arrow schema.</summary>
    public static AvroSchema FromArrowSchema(
        Apache.Arrow.Schema schema,
        string recordName = "Record",
        string? recordNamespace = null)
    {
        var avroRecord = ArrowSchemaConverter.FromArrow(schema, recordName, recordNamespace);
        var json = AvroSchemaWriter.ToJson(avroRecord);
        return new AvroSchema(json);
    }

#if !NET8_0_OR_GREATER
    private static byte[] ComputeHash<T>(byte[] data) where T : System.Security.Cryptography.HashAlgorithm
    {
        using var alg = System.Security.Cryptography.HashAlgorithm.Create(typeof(T).Name)
            ?? throw new InvalidOperationException($"Hash algorithm {typeof(T).Name} not available.");
        return alg.ComputeHash(data);
    }
#endif
}
