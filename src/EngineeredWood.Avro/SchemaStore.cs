// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Avro;

/// <summary>
/// A registry that maps schema fingerprints to <see cref="AvroSchema"/> instances.
/// Used to resolve schemas during deserialization.
/// </summary>
public sealed class SchemaStore
{
    private readonly Dictionary<SchemaFingerprint, AvroSchema> _schemas = new();

    /// <summary>
    /// Creates a new <see cref="SchemaStore"/> using the specified fingerprint algorithm for auto-registration.
    /// </summary>
    public SchemaStore(FingerprintAlgorithm algorithm = FingerprintAlgorithm.Rabin)
    {
        Algorithm = algorithm;
    }

    /// <summary>The fingerprint algorithm used when registering schemas via <see cref="Register"/>.</summary>
    public FingerprintAlgorithm Algorithm { get; }

    /// <summary>
    /// Registers a schema by computing its fingerprint using <see cref="Algorithm"/>.
    /// Returns the computed fingerprint.
    /// </summary>
    public SchemaFingerprint Register(AvroSchema schema)
    {
#if NET8_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(schema);
#else
        if (schema is null) throw new ArgumentNullException(nameof(schema));
#endif
        var fingerprint = schema.ComputeFingerprint(Algorithm);
        _schemas[fingerprint] = schema;
        return fingerprint;
    }

    /// <summary>
    /// Registers a schema with an explicit fingerprint (e.g., a Confluent Schema Registry ID).
    /// </summary>
    public void Set(SchemaFingerprint fingerprint, AvroSchema schema)
    {
#if NET8_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(fingerprint);
        ArgumentNullException.ThrowIfNull(schema);
#else
        if (fingerprint is null) throw new ArgumentNullException(nameof(fingerprint));
        if (schema is null) throw new ArgumentNullException(nameof(schema));
#endif
        _schemas[fingerprint] = schema;
    }

    /// <summary>
    /// Looks up a schema by its fingerprint.
    /// </summary>
    /// <returns>The schema if found; otherwise <c>null</c>.</returns>
    public AvroSchema? Lookup(SchemaFingerprint fingerprint)
    {
#if NET8_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(fingerprint);
        return _schemas.GetValueOrDefault(fingerprint);
#else
        if (fingerprint is null) throw new ArgumentNullException(nameof(fingerprint));
        return _schemas.TryGetValue(fingerprint, out var schema) ? schema : null;
#endif
    }

    /// <summary>
    /// All registered fingerprints.
    /// </summary>
    public IReadOnlyList<SchemaFingerprint> Fingerprints => _schemas.Keys.ToList();
}
