// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Avro;

/// <summary>
/// Represents a schema fingerprint used to uniquely identify an Avro schema.
/// </summary>
public abstract record SchemaFingerprint
{
    /// <summary>
    /// A 64-bit Rabin fingerprint computed from the Parsing Canonical Form.
    /// </summary>
    public sealed record Rabin(ulong Value) : SchemaFingerprint;

    /// <summary>
    /// An MD5 hash of the Parsing Canonical Form.
    /// </summary>
    public sealed record MD5(byte[] Value) : SchemaFingerprint
    {
        /// <inheritdoc/>
        public bool Equals(MD5? other) => other is not null && Value.AsSpan().SequenceEqual(other.Value);

        /// <inheritdoc/>
        public override int GetHashCode()
        {
#if NET8_0_OR_GREATER
            var hash = new HashCode();
            hash.AddBytes(Value);
            return hash.ToHashCode();
#else
            unchecked
            {
                int hash = 17;
                foreach (byte b in Value)
                    hash = hash * 31 + b;
                return hash;
            }
#endif
        }
    }

    /// <summary>
    /// A SHA-256 hash of the Parsing Canonical Form.
    /// </summary>
    public sealed record SHA256(byte[] Value) : SchemaFingerprint
    {
        /// <inheritdoc/>
        public bool Equals(SHA256? other) => other is not null && Value.AsSpan().SequenceEqual(other.Value);

        /// <inheritdoc/>
        public override int GetHashCode()
        {
#if NET8_0_OR_GREATER
            var hash = new HashCode();
            hash.AddBytes(Value);
            return hash.ToHashCode();
#else
            unchecked
            {
                int hash = 17;
                foreach (byte b in Value)
                    hash = hash * 31 + b;
                return hash;
            }
#endif
        }
    }

    /// <summary>
    /// A Confluent Schema Registry schema ID.
    /// </summary>
    public sealed record ConfluentId(uint Value) : SchemaFingerprint;

    /// <summary>
    /// An Apicurio Registry global ID.
    /// </summary>
    public sealed record ApicurioId(ulong Value) : SchemaFingerprint;
}

/// <summary>
/// Specifies which algorithm to use for computing schema fingerprints.
/// </summary>
public enum FingerprintAlgorithm
{
    /// <summary>64-bit Rabin fingerprint (CRC-64-AVRO).</summary>
    Rabin,

    /// <summary>MD5 hash.</summary>
    MD5,

    /// <summary>SHA-256 hash.</summary>
    SHA256,
}
