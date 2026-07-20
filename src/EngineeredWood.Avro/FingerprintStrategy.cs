// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Avro;

/// <summary>
/// Describes how a schema fingerprint is encoded in the wire format header of an Avro message.
/// </summary>
public abstract record FingerprintStrategy
{
    /// <summary>
    /// Avro Single Object Encoding (SOE): 0xC3 0x01 marker followed by an 8-byte little-endian Rabin fingerprint.
    /// </summary>
    public sealed record Soe() : FingerprintStrategy;

    /// <summary>
    /// Confluent wire format: 0x00 magic byte followed by a 4-byte big-endian schema ID.
    /// </summary>
    public sealed record Confluent(uint SchemaId) : FingerprintStrategy;

    /// <summary>
    /// Apicurio wire format: 0x00 magic byte followed by an 8-byte big-endian global ID.
    /// </summary>
    public sealed record Apicurio(ulong GlobalId) : FingerprintStrategy;
}
