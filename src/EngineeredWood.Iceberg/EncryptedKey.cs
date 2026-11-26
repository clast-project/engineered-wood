// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg;

/// <summary>
/// An entry of a v3 table's <c>encryption-keys</c> list. This library does not implement table encryption:
/// it keeps these entries through a metadata rewrite, and refuses to read or write the data of a table that
/// has any (see <see cref="UnsupportedTableFeatureException"/>).
/// </summary>
/// <param name="KeyId">The id of the encryption key.</param>
/// <param name="EncryptedKeyMetadata">The encrypted key and its metadata, base64 encoded.</param>
/// <param name="EncryptedById">The id of the key that encrypts or wraps this one, if any.</param>
/// <param name="Properties">Additional metadata the table's encryption scheme uses.</param>
public sealed record EncryptedKey(
    string KeyId,
    string EncryptedKeyMetadata,
    string? EncryptedById = null,
    IReadOnlyDictionary<string, string>? Properties = null)
{
    /// <summary>Properties this library does not model, kept so that a rewrite does not drop them.</summary>
    [JsonExtensionData, JsonInclude]
    public IDictionary<string, JsonElement>? AdditionalProperties { get; internal set; }
}
