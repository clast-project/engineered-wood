// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Avro;

/// <summary>
/// Wire format for streaming Avro messages with schema identification headers.
/// </summary>
public enum AvroWireFormat
{
    /// <summary>
    /// Avro Single Object Encoding: [0xC3, 0x01] + 8-byte LE Rabin fingerprint + payload.
    /// </summary>
    SingleObject,

    /// <summary>
    /// Confluent Schema Registry wire format: [0x00] + 4-byte BE schema ID + payload.
    /// </summary>
    Confluent,

    /// <summary>
    /// Apicurio Registry wire format: [0x00] + 8-byte BE global ID + payload.
    /// </summary>
    Apicurio,

    /// <summary>
    /// Raw Avro binary with no framing header. Assumes a single schema for all messages.
    /// </summary>
    RawBinary,
}
