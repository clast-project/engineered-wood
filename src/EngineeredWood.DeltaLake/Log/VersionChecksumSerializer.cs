// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// The on-disk form of a <see cref="VersionChecksum"/>: ONE JSON object, not the log's NDJSON.
/// </summary>
/// <remarks>
/// <para>The bodies of the actions it embeds are written and read by <see cref="ActionSerializer"/>
/// rather than duplicated here — see <see cref="ActionSerializer.WriteActionBody"/> for why that matters
/// more than it looks.</para>
/// <para>Unknown fields are skipped on read. The spec has grown this file twice already
/// (<c>fileSizeHistogram</c>, the deletion-vector counts), and a writer that adds a field must not make
/// its checksums unreadable here.</para>
/// </remarks>
internal static class VersionChecksumSerializer
{
    public static byte[] Serialize(VersionChecksum checksum)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();

            writer.WriteNumber("tableSizeBytes", checksum.TableSizeBytes);
            writer.WriteNumber("numFiles", checksum.NumFiles);
            // Fixed at 1 by the spec — a version has exactly one effective metaData and one effective
            // protocol after reconciliation, whatever the commits contained.
            writer.WriteNumber("numMetadata", 1);
            writer.WriteNumber("numProtocol", 1);

            if (checksum.InCommitTimestamp is { } ict)
                writer.WriteNumber("inCommitTimestampOpt", ict);

            if (checksum.SetTransactions is { } txns)
            {
                writer.WritePropertyName("setTransactions");
                writer.WriteStartArray();
                foreach (var txn in txns)
                    ActionSerializer.WriteActionBody(writer, txn);
                writer.WriteEndArray();
            }

            if (checksum.DomainMetadata is { } domains)
            {
                writer.WritePropertyName("domainMetadata");
                writer.WriteStartArray();
                foreach (var domain in domains)
                    ActionSerializer.WriteActionBody(writer, domain);
                writer.WriteEndArray();
            }

            writer.WritePropertyName("metadata");
            ActionSerializer.WriteActionBody(writer, checksum.Metadata);

            writer.WritePropertyName("protocol");
            ActionSerializer.WriteActionBody(writer, checksum.Protocol);

            writer.WriteEndObject();
        }

        return stream.ToArray();
    }

    /// <summary>
    /// Parses a checksum file body. <paramref name="version"/> comes from the FILE NAME — the body does
    /// not carry one.
    /// </summary>
    /// <exception cref="DeltaFormatException">The JSON is malformed, a required field is missing, or
    /// <c>numMetadata</c> / <c>numProtocol</c> is not 1.</exception>
    public static VersionChecksum Deserialize(ReadOnlySpan<byte> json, long version)
    {
        var reader = new Utf8JsonReader(json);

        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidLogJson,
                $"Version checksum for version {version} is not a JSON object.");
        }

        long? tableSizeBytes = null;
        long? numFiles = null;
        long? numMetadata = null;
        long? numProtocol = null;
        long? inCommitTimestamp = null;
        MetadataAction? metadata = null;
        ProtocolAction? protocol = null;
        List<TransactionId>? setTransactions = null;
        List<DomainMetadata>? domainMetadata = null;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new DeltaFormatException(
                    DeltaErrorCodes.InvalidLogJson,
                    $"Version checksum for version {version} has a malformed property.");
            }

            string property = reader.GetString()!;
            reader.Read();

            switch (property)
            {
                case "tableSizeBytes": tableSizeBytes = reader.GetInt64(); break;
                case "numFiles": numFiles = reader.GetInt64(); break;
                case "numMetadata": numMetadata = reader.GetInt64(); break;
                case "numProtocol": numProtocol = reader.GetInt64(); break;
                case "inCommitTimestampOpt": inCommitTimestamp = reader.GetInt64(); break;
                case "metadata":
                    metadata = (MetadataAction?)ActionSerializer.ReadActionBody(ref reader, "metaData");
                    break;
                case "protocol":
                    protocol = (ProtocolAction?)ActionSerializer.ReadActionBody(ref reader, "protocol");
                    break;
                case "setTransactions":
                    setTransactions = ReadActionArray<TransactionId>(ref reader, "txn");
                    break;
                case "domainMetadata":
                    domainMetadata = ReadActionArray<DomainMetadata>(ref reader, "domainMetadata");
                    break;
                default:
                    reader.Skip();
                    break;
            }
        }

        // REQUIRED, and required to be 1. Both halves are rejected at the parse boundary — as
        // delta-kernel-rs does, where `CrcRaw.num_metadata` / `num_protocol` are plain `i64` with no
        // serde default, so an absent one fails the whole deserialization — because a caller reading a
        // checksum in order to VALIDATE against it must not be handed a file the spec calls malformed.
        // Absent is not a milder problem than wrong here: it means the writer was not describing the
        // shape this file is defined to have, and nothing else it says is more trustworthy for it.
        RequireOne(numMetadata, "numMetadata", version);
        RequireOne(numProtocol, "numProtocol", version);

        return new VersionChecksum
        {
            Version = version,
            TableSizeBytes = tableSizeBytes ?? throw MissingField(version, "tableSizeBytes"),
            NumFiles = numFiles ?? throw MissingField(version, "numFiles"),
            Metadata = metadata ?? throw MissingField(version, "metadata"),
            Protocol = protocol ?? throw MissingField(version, "protocol"),
            InCommitTimestamp = inCommitTimestamp,
            SetTransactions = setTransactions,
            DomainMetadata = domainMetadata,
        };
    }

    /// <summary>
    /// Reads an array of bare action bodies, or null for an explicit JSON <c>null</c>.
    /// </summary>
    /// <remarks>
    /// Null survives as null and an empty array survives as an empty list, because the two say different
    /// things: "not recorded" versus "recorded, and there are none". Collapsing them would turn a
    /// writer's authoritative empty into a reason to replay the log. See
    /// <see cref="VersionChecksum.SetTransactions"/>.
    /// </remarks>
    private static List<T>? ReadActionArray<T>(ref Utf8JsonReader reader, string actionType)
        where T : DeltaAction
    {
        if (reader.TokenType == JsonTokenType.Null)
            return null;

        var items = new List<T>();
        if (reader.TokenType != JsonTokenType.StartArray)
        {
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidLogJson,
                $"Version checksum field for '{actionType}' is not an array.");
        }

        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
        {
            if (ActionSerializer.ReadActionBody(ref reader, actionType) is T item)
                items.Add(item);
        }

        return items;
    }

    /// <summary>
    /// Enforces one of the two count fields the spec pins at 1: present, and equal to 1.
    /// </summary>
    private static void RequireOne(long? value, string field, long version)
    {
        if (value is null)
            throw MissingField(version, field);

        if (value != 1)
        {
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidLogJson,
                $"Version checksum for version {version} has {field} {value}; the spec requires 1.");
        }
    }

    private static DeltaFormatException MissingField(long version, string field) =>
        new(DeltaErrorCodes.MissingRequiredField,
            $"Version checksum for version {version} is missing the required field '{field}'.");
}
