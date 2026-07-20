// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Log;

/// <summary>
/// Serializes and deserializes Delta Lake actions to/from NDJSON format.
/// Each line in a commit file is a JSON object with a single top-level key
/// that identifies the action type (e.g., <c>{"add":{...}}</c>).
/// </summary>
internal static class ActionSerializer
{
    private static readonly JsonSerializerOptions s_options = CreateOptions();

    // The action wrapper format is handled entirely by this converter; we invoke it
    // directly (rather than via the reflection-based JsonSerializer entry points) so the
    // top-level DeltaAction round-trip stays trim/AOT safe without registering the
    // abstract DeltaAction in the source-generated context.
    private static readonly DeltaActionConverter s_converter = new();

    private static readonly JsonTypeInfo<DeletionVector> s_deletionVectorInfo =
        (JsonTypeInfo<DeletionVector>)s_options.GetTypeInfo(typeof(DeletionVector));

    private static JsonSerializerOptions CreateOptions()
    {
        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
            TypeInfoResolver = DeltaJsonContext.Default,
        };
    }

    /// <summary>
    /// Deserializes NDJSON bytes into a list of actions.
    /// </summary>
    public static IReadOnlyList<DeltaAction> Deserialize(ReadOnlySpan<byte> ndjson)
    {
        var actions = new List<DeltaAction>();
        int start = 0;

        for (int i = 0; i <= ndjson.Length; i++)
        {
            if (i == ndjson.Length || ndjson[i] == (byte)'\n')
            {
                var line = ndjson[start..i];
                // Skip empty lines and lines that are only whitespace
                if (!line.IsEmpty && HasNonWhitespace(line))
                {
                    var reader = new Utf8JsonReader(line);
                    if (!reader.Read())
                        continue;
                    var action = s_converter.Read(ref reader, typeof(DeltaAction), s_options);
                    if (action is not null)
                        actions.Add(action);
                }
                start = i + 1;
            }
        }

        return actions;
    }

    /// <summary>
    /// Serializes a list of actions to NDJSON bytes.
    /// </summary>
    public static byte[] Serialize(IReadOnlyList<DeltaAction> actions)
    {
        using var ms = new MemoryStream();
        foreach (var action in actions)
        {
            using (var writer = new Utf8JsonWriter(ms))
            {
                s_converter.Write(writer, action, s_options);
            }
            ms.WriteByte((byte)'\n');
        }
        return ms.ToArray();
    }

    private static bool HasNonWhitespace(ReadOnlySpan<byte> data)
    {
        for (int i = 0; i < data.Length; i++)
            if (data[i] != (byte)' ' && data[i] != (byte)'\t' && data[i] != (byte)'\r')
                return true;
        return false;
    }

    /// <summary>
    /// Custom JSON converter that reads/writes the action wrapper format:
    /// <c>{"add":{...}}</c>, <c>{"remove":{...}}</c>, etc.
    /// </summary>
    private sealed class DeltaActionConverter : JsonConverter<DeltaAction>
    {
        public override DeltaAction? Read(
            ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
                throw new DeltaFormatException("Expected JSON object for action.");

            reader.Read(); // Move to property name
            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new DeltaFormatException("Expected action type property name.");

            string actionType = reader.GetString()!;
            reader.Read(); // Move to the action value

            DeltaAction? action = actionType switch
            {
                "add" => DeserializeAdd(ref reader, options),
                "remove" => DeserializeRemove(ref reader, options),
                "metaData" => DeserializeMetadata(ref reader, options),
                "protocol" => DeserializeProtocol(ref reader, options),
                "commitInfo" => DeserializeCommitInfo(ref reader),
                "txn" => DeserializeTxn(ref reader, options),
                "cdc" => DeserializeCdc(ref reader, options),
                "domainMetadata" => DeserializeDomainMetadata(ref reader, options),
                "checkpointMetadata" => DeserializeCheckpointMetadata(ref reader),
                "sidecar" => DeserializeSidecar(ref reader),
                _ => SkipUnknownAction(ref reader),
            };

            reader.Read(); // End object
            return action;
        }

        public override void Write(
            Utf8JsonWriter writer, DeltaAction value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            switch (value)
            {
                case AddFile add:
                    writer.WritePropertyName("add");
                    SerializeAdd(writer, add, options);
                    break;
                case RemoveFile remove:
                    writer.WritePropertyName("remove");
                    SerializeRemove(writer, remove, options);
                    break;
                case MetadataAction metadata:
                    writer.WritePropertyName("metaData");
                    SerializeMetadata(writer, metadata, options);
                    break;
                case ProtocolAction protocol:
                    writer.WritePropertyName("protocol");
                    SerializeProtocol(writer, protocol, options);
                    break;
                case CommitInfo commitInfo:
                    writer.WritePropertyName("commitInfo");
                    SerializeCommitInfo(writer, commitInfo);
                    break;
                case TransactionId txn:
                    writer.WritePropertyName("txn");
                    SerializeTxn(writer, txn, options);
                    break;
                case CdcFile cdc:
                    writer.WritePropertyName("cdc");
                    SerializeCdc(writer, cdc, options);
                    break;
                case DomainMetadata dm:
                    writer.WritePropertyName("domainMetadata");
                    SerializeDomainMetadata(writer, dm, options);
                    break;
                case CheckpointMetadata cm:
                    writer.WritePropertyName("checkpointMetadata");
                    SerializeCheckpointMetadata(writer, cm);
                    break;
                case SidecarFile sc:
                    writer.WritePropertyName("sidecar");
                    SerializeSidecar(writer, sc);
                    break;
                default:
                    throw new DeltaFormatException(
                        $"Unknown action type: {value.GetType().Name}");
            }

            writer.WriteEndObject();
        }

        #region Deserialization

        private static AddFile DeserializeAdd(
            ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            string? path = null;
            Dictionary<string, string>? partitionValues = null;
            long size = 0;
            long modificationTime = 0;
            bool dataChange = false;
            string? stats = null;
            Dictionary<string, string>? tags = null;
            DeletionVector? deletionVector = null;
            long? baseRowId = null;
            long? defaultRowCommitVersion = null;
            string? clusteringProvider = null;

            ReadObject(ref reader, (ref Utf8JsonReader r, string prop) =>
            {
                switch (prop)
                {
                    case "path": path = r.GetString(); break;
                    case "partitionValues": partitionValues = ReadStringDict(ref r); break;
                    case "size": size = r.GetInt64(); break;
                    case "modificationTime": modificationTime = r.GetInt64(); break;
                    case "dataChange": dataChange = r.GetBoolean(); break;
                    case "stats": stats = r.GetString(); break;
                    case "tags": tags = ReadStringDict(ref r); break;
                    case "deletionVector": deletionVector = JsonSerializer.Deserialize(ref r, s_deletionVectorInfo); break;
                    case "baseRowId": baseRowId = r.GetInt64(); break;
                    case "defaultRowCommitVersion": defaultRowCommitVersion = r.GetInt64(); break;
                    case "clusteringProvider": clusteringProvider = r.GetString(); break;
                    default: r.Skip(); break;
                }
            });

            return new AddFile
            {
                Path = path ?? throw new DeltaFormatException("add.path is required"),
                PartitionValues = partitionValues ?? new Dictionary<string, string>(),
                Size = size,
                ModificationTime = modificationTime,
                DataChange = dataChange,
                Stats = stats,
                Tags = tags,
                DeletionVector = deletionVector,
                BaseRowId = baseRowId,
                DefaultRowCommitVersion = defaultRowCommitVersion,
                ClusteringProvider = clusteringProvider,
            };
        }

        private static RemoveFile DeserializeRemove(
            ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            string? path = null;
            long? deletionTimestamp = null;
            bool dataChange = false;
            bool? extendedFileMetadata = null;
            Dictionary<string, string>? partitionValues = null;
            long? size = null;
            Dictionary<string, string>? tags = null;
            DeletionVector? deletionVector = null;
            long? baseRowId = null;
            long? defaultRowCommitVersion = null;

            ReadObject(ref reader, (ref Utf8JsonReader r, string prop) =>
            {
                switch (prop)
                {
                    case "path": path = r.GetString(); break;
                    case "deletionTimestamp": deletionTimestamp = r.GetInt64(); break;
                    case "dataChange": dataChange = r.GetBoolean(); break;
                    case "extendedFileMetadata": extendedFileMetadata = r.GetBoolean(); break;
                    case "partitionValues": partitionValues = ReadStringDict(ref r); break;
                    case "size": size = r.GetInt64(); break;
                    case "tags": tags = ReadStringDict(ref r); break;
                    case "deletionVector": deletionVector = JsonSerializer.Deserialize(ref r, s_deletionVectorInfo); break;
                    case "baseRowId": baseRowId = r.GetInt64(); break;
                    case "defaultRowCommitVersion": defaultRowCommitVersion = r.GetInt64(); break;
                    default: r.Skip(); break;
                }
            });

            return new RemoveFile
            {
                Path = path ?? throw new DeltaFormatException("remove.path is required"),
                DeletionTimestamp = deletionTimestamp,
                DataChange = dataChange,
                ExtendedFileMetadata = extendedFileMetadata,
                PartitionValues = partitionValues,
                Size = size,
                Tags = tags,
                DeletionVector = deletionVector,
                BaseRowId = baseRowId,
                DefaultRowCommitVersion = defaultRowCommitVersion,
            };
        }

        private static MetadataAction DeserializeMetadata(
            ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            string? id = null;
            string? name = null;
            string? description = null;
            Format? format = null;
            string? schemaString = null;
            List<string>? partitionColumns = null;
            Dictionary<string, string>? configuration = null;
            long? createdTime = null;

            ReadObject(ref reader, (ref Utf8JsonReader r, string prop) =>
            {
                switch (prop)
                {
                    case "id": id = r.GetString(); break;
                    case "name": name = r.GetString(); break;
                    case "description": description = r.GetString(); break;
                    case "format": format = DeserializeFormat(ref r); break;
                    case "schemaString": schemaString = r.GetString(); break;
                    case "partitionColumns": partitionColumns = ReadStringList(ref r); break;
                    case "configuration": configuration = ReadStringDict(ref r); break;
                    case "createdTime": createdTime = r.GetInt64(); break;
                    default: r.Skip(); break;
                }
            });

            return new MetadataAction
            {
                Id = id ?? throw new DeltaFormatException("metaData.id is required"),
                Name = name,
                Description = description,
                Format = format ?? Format.Parquet,
                SchemaString = schemaString ?? throw new DeltaFormatException("metaData.schemaString is required"),
                PartitionColumns = partitionColumns ?? [],
                Configuration = configuration,
                CreatedTime = createdTime,
            };
        }

        private static Format DeserializeFormat(ref Utf8JsonReader reader)
        {
            string? provider = null;
            Dictionary<string, string>? options = null;

            ReadObject(ref reader, (ref Utf8JsonReader r, string prop) =>
            {
                switch (prop)
                {
                    case "provider": provider = r.GetString(); break;
                    case "options": options = ReadStringDict(ref r); break;
                    default: r.Skip(); break;
                }
            });

            return new Format
            {
                Provider = provider ?? "parquet",
                Options = options ?? new Dictionary<string, string>(),
            };
        }

        private static ProtocolAction DeserializeProtocol(
            ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            int minReaderVersion = 0;
            int minWriterVersion = 0;
            List<string>? readerFeatures = null;
            List<string>? writerFeatures = null;

            ReadObject(ref reader, (ref Utf8JsonReader r, string prop) =>
            {
                switch (prop)
                {
                    case "minReaderVersion": minReaderVersion = r.GetInt32(); break;
                    case "minWriterVersion": minWriterVersion = r.GetInt32(); break;
                    case "readerFeatures": readerFeatures = ReadStringList(ref r); break;
                    case "writerFeatures": writerFeatures = ReadStringList(ref r); break;
                    default: r.Skip(); break;
                }
            });

            return new ProtocolAction
            {
                MinReaderVersion = minReaderVersion,
                MinWriterVersion = minWriterVersion,
                ReaderFeatures = readerFeatures,
                WriterFeatures = writerFeatures,
            };
        }

        private static CommitInfo DeserializeCommitInfo(ref Utf8JsonReader reader)
        {
            // Parse the entire object as a JsonElement and extract all properties
            var element = JsonElement.ParseValue(ref reader);
            var values = new Dictionary<string, JsonElement>();

            foreach (var prop in element.EnumerateObject())
                values[prop.Name] = prop.Value.Clone();

            return new CommitInfo { Values = values };
        }

        private static TransactionId DeserializeTxn(
            ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            string? appId = null;
            long version = 0;
            long? lastUpdated = null;

            ReadObject(ref reader, (ref Utf8JsonReader r, string prop) =>
            {
                switch (prop)
                {
                    case "appId": appId = r.GetString(); break;
                    case "version": version = r.GetInt64(); break;
                    case "lastUpdated": lastUpdated = r.GetInt64(); break;
                    default: r.Skip(); break;
                }
            });

            return new TransactionId
            {
                AppId = appId ?? throw new DeltaFormatException("txn.appId is required"),
                Version = version,
                LastUpdated = lastUpdated,
            };
        }

        private static CdcFile DeserializeCdc(
            ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            string? path = null;
            Dictionary<string, string>? partitionValues = null;
            long size = 0;
            bool dataChange = false;
            Dictionary<string, string>? tags = null;

            ReadObject(ref reader, (ref Utf8JsonReader r, string prop) =>
            {
                switch (prop)
                {
                    case "path": path = r.GetString(); break;
                    case "partitionValues": partitionValues = ReadStringDict(ref r); break;
                    case "size": size = r.GetInt64(); break;
                    case "dataChange": dataChange = r.GetBoolean(); break;
                    case "tags": tags = ReadStringDict(ref r); break;
                    default: r.Skip(); break;
                }
            });

            return new CdcFile
            {
                Path = path ?? throw new DeltaFormatException("cdc.path is required"),
                PartitionValues = partitionValues ?? new Dictionary<string, string>(),
                Size = size,
                DataChange = dataChange,
                Tags = tags,
            };
        }

        private static DomainMetadata DeserializeDomainMetadata(
            ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            string? domain = null;
            string? configuration = null;
            bool removed = false;

            ReadObject(ref reader, (ref Utf8JsonReader r, string prop) =>
            {
                switch (prop)
                {
                    case "domain": domain = r.GetString(); break;
                    case "configuration": configuration = r.GetString(); break;
                    case "removed": removed = r.GetBoolean(); break;
                    default: r.Skip(); break;
                }
            });

            return new DomainMetadata
            {
                Domain = domain ?? throw new DeltaFormatException("domainMetadata.domain is required"),
                Configuration = configuration ?? "",
                Removed = removed,
            };
        }

        private static CheckpointMetadata DeserializeCheckpointMetadata(
            ref Utf8JsonReader reader)
        {
            long version = 0;
            Dictionary<string, string>? tags = null;

            ReadObject(ref reader, (ref Utf8JsonReader r, string prop) =>
            {
                switch (prop)
                {
                    case "version": version = r.GetInt64(); break;
                    case "tags": tags = ReadStringDict(ref r); break;
                    default: r.Skip(); break;
                }
            });

            return new CheckpointMetadata
            {
                Version = version,
                Tags = tags,
            };
        }

        private static SidecarFile DeserializeSidecar(ref Utf8JsonReader reader)
        {
            string? path = null;
            long sizeInBytes = 0;
            long modificationTime = 0;
            Dictionary<string, string>? tags = null;

            ReadObject(ref reader, (ref Utf8JsonReader r, string prop) =>
            {
                switch (prop)
                {
                    case "path": path = r.GetString(); break;
                    case "sizeInBytes": sizeInBytes = r.GetInt64(); break;
                    case "modificationTime": modificationTime = r.GetInt64(); break;
                    case "tags": tags = ReadStringDict(ref r); break;
                    default: r.Skip(); break;
                }
            });

            return new SidecarFile
            {
                Path = path ?? throw new DeltaFormatException("sidecar.path is required"),
                SizeInBytes = sizeInBytes,
                ModificationTime = modificationTime,
                Tags = tags,
            };
        }

        private static DeltaAction? SkipUnknownAction(ref Utf8JsonReader reader)
        {
            reader.Skip();
            return null;
        }

        #endregion

        #region Serialization

        private static void SerializeAdd(
            Utf8JsonWriter writer, AddFile add, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("path", add.Path);
            WriteStringDict(writer, "partitionValues", add.PartitionValues);
            writer.WriteNumber("size", add.Size);
            writer.WriteNumber("modificationTime", add.ModificationTime);
            writer.WriteBoolean("dataChange", add.DataChange);
            if (add.Stats is not null) writer.WriteString("stats", add.Stats);
            if (add.Tags is not null) WriteStringDict(writer, "tags", add.Tags);
            if (add.DeletionVector is not null)
            {
                writer.WritePropertyName("deletionVector");
                JsonSerializer.Serialize(writer, add.DeletionVector, s_deletionVectorInfo);
            }
            if (add.BaseRowId.HasValue) writer.WriteNumber("baseRowId", add.BaseRowId.Value);
            if (add.DefaultRowCommitVersion.HasValue) writer.WriteNumber("defaultRowCommitVersion", add.DefaultRowCommitVersion.Value);
            if (add.ClusteringProvider is not null) writer.WriteString("clusteringProvider", add.ClusteringProvider);
            writer.WriteEndObject();
        }

        private static void SerializeRemove(
            Utf8JsonWriter writer, RemoveFile remove, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("path", remove.Path);
            if (remove.DeletionTimestamp.HasValue) writer.WriteNumber("deletionTimestamp", remove.DeletionTimestamp.Value);
            writer.WriteBoolean("dataChange", remove.DataChange);
            if (remove.ExtendedFileMetadata.HasValue) writer.WriteBoolean("extendedFileMetadata", remove.ExtendedFileMetadata.Value);
            if (remove.PartitionValues is not null) WriteStringDict(writer, "partitionValues", remove.PartitionValues);
            if (remove.Size.HasValue) writer.WriteNumber("size", remove.Size.Value);
            if (remove.Tags is not null) WriteStringDict(writer, "tags", remove.Tags);
            if (remove.DeletionVector is not null)
            {
                writer.WritePropertyName("deletionVector");
                JsonSerializer.Serialize(writer, remove.DeletionVector, s_deletionVectorInfo);
            }
            if (remove.BaseRowId.HasValue) writer.WriteNumber("baseRowId", remove.BaseRowId.Value);
            if (remove.DefaultRowCommitVersion.HasValue) writer.WriteNumber("defaultRowCommitVersion", remove.DefaultRowCommitVersion.Value);
            writer.WriteEndObject();
        }

        private static void SerializeMetadata(
            Utf8JsonWriter writer, MetadataAction metadata, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("id", metadata.Id);
            if (metadata.Name is not null) writer.WriteString("name", metadata.Name);
            if (metadata.Description is not null) writer.WriteString("description", metadata.Description);

            writer.WritePropertyName("format");
            writer.WriteStartObject();
            writer.WriteString("provider", metadata.Format.Provider);
            // ALWAYS emit options, even when empty. delta-kernel-rs decodes format.options as a
            // non-nullable map and fails the whole log read ("Encountered unmasked nulls in
            // non-nullable StructArray child") when the field is absent — so omitting it on the
            // common empty case made every EW-written table unreadable by delta-rs and DuckDB.
            WriteStringDict(writer, "options", metadata.Format.Options);
            writer.WriteEndObject();

            writer.WriteString("schemaString", metadata.SchemaString);

            writer.WritePropertyName("partitionColumns");
            writer.WriteStartArray();
            foreach (string col in metadata.PartitionColumns)
                writer.WriteStringValue(col);
            writer.WriteEndArray();

            // Always emitted, like format.options above: delta-kernel-rs decodes configuration as a
            // non-nullable map and fails the log read outright when it is absent.
            WriteStringDict(writer, "configuration",
                metadata.Configuration ?? (IReadOnlyDictionary<string, string>)new Dictionary<string, string>());
            if (metadata.CreatedTime.HasValue) writer.WriteNumber("createdTime", metadata.CreatedTime.Value);
            writer.WriteEndObject();
        }

        private static void SerializeProtocol(
            Utf8JsonWriter writer, ProtocolAction protocol, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteNumber("minReaderVersion", protocol.MinReaderVersion);
            writer.WriteNumber("minWriterVersion", protocol.MinWriterVersion);
            if (protocol.ReaderFeatures is not null)
            {
                writer.WritePropertyName("readerFeatures");
                writer.WriteStartArray();
                foreach (string f in protocol.ReaderFeatures)
                    writer.WriteStringValue(f);
                writer.WriteEndArray();
            }
            if (protocol.WriterFeatures is not null)
            {
                writer.WritePropertyName("writerFeatures");
                writer.WriteStartArray();
                foreach (string f in protocol.WriterFeatures)
                    writer.WriteStringValue(f);
                writer.WriteEndArray();
            }
            writer.WriteEndObject();
        }

        private static void SerializeCommitInfo(
            Utf8JsonWriter writer, CommitInfo commitInfo)
        {
            writer.WriteStartObject();
            foreach (var kvp in commitInfo.Values)
            {
                writer.WritePropertyName(kvp.Key);
                kvp.Value.WriteTo(writer);
            }
            writer.WriteEndObject();
        }

        private static void SerializeTxn(
            Utf8JsonWriter writer, TransactionId txn, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("appId", txn.AppId);
            writer.WriteNumber("version", txn.Version);
            if (txn.LastUpdated.HasValue) writer.WriteNumber("lastUpdated", txn.LastUpdated.Value);
            writer.WriteEndObject();
        }

        private static void SerializeCdc(
            Utf8JsonWriter writer, CdcFile cdc, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("path", cdc.Path);
            WriteStringDict(writer, "partitionValues", cdc.PartitionValues);
            writer.WriteNumber("size", cdc.Size);
            writer.WriteBoolean("dataChange", cdc.DataChange);
            if (cdc.Tags is not null) WriteStringDict(writer, "tags", cdc.Tags);
            writer.WriteEndObject();
        }

        private static void SerializeDomainMetadata(
            Utf8JsonWriter writer, DomainMetadata dm, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("domain", dm.Domain);
            writer.WriteString("configuration", dm.Configuration);
            writer.WriteBoolean("removed", dm.Removed);
            writer.WriteEndObject();
        }

        private static void SerializeCheckpointMetadata(
            Utf8JsonWriter writer, CheckpointMetadata cm)
        {
            writer.WriteStartObject();
            writer.WriteNumber("version", cm.Version);
            if (cm.Tags is { Count: > 0 })
                WriteStringDict(writer, "tags", cm.Tags);
            writer.WriteEndObject();
        }

        private static void SerializeSidecar(
            Utf8JsonWriter writer, SidecarFile sc)
        {
            writer.WriteStartObject();
            writer.WriteString("path", sc.Path);
            writer.WriteNumber("sizeInBytes", sc.SizeInBytes);
            writer.WriteNumber("modificationTime", sc.ModificationTime);
            if (sc.Tags is { Count: > 0 })
                WriteStringDict(writer, "tags", sc.Tags);
            writer.WriteEndObject();
        }

        #endregion

        #region Helpers

        private delegate void PropertyReader(ref Utf8JsonReader reader, string propertyName);

        private static void ReadObject(ref Utf8JsonReader reader, PropertyReader handler)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
                throw new DeltaFormatException("Expected JSON object.");

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                    return;

                if (reader.TokenType != JsonTokenType.PropertyName)
                    throw new DeltaFormatException("Expected property name.");

                string propName = reader.GetString()!;
                reader.Read();

                // An explicit JSON null means "absent" for every Delta action field — there is no
                // field where null and omitted differ. delta-rs writes optional fields out as
                // explicit nulls (`"baseRowId": null, "tags": null, ...`) where EW omits them, so
                // handlers must never see a Null token: they call GetInt64/GetBoolean directly and
                // would throw. Skipping here leaves the handler's local at its default, which is
                // exactly what an omitted field produces, and keeps the `?? throw` checks on
                // required fields reporting a useful message.
                if (reader.TokenType == JsonTokenType.Null)
                    continue;

                handler(ref reader, propName);
            }
        }

        private static Dictionary<string, string> ReadStringDict(ref Utf8JsonReader reader)
        {
            var dict = new Dictionary<string, string>();
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                reader.Skip();
                return dict;
            }

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                    return dict;

                string key = reader.GetString()!;
                reader.Read();
                string? value = reader.TokenType == JsonTokenType.Null
                    ? null!
                    : reader.GetString()!;
                if (value is not null)
                    dict[key] = value;
            }

            return dict;
        }

        private static List<string> ReadStringList(ref Utf8JsonReader reader)
        {
            var list = new List<string>();
            if (reader.TokenType != JsonTokenType.StartArray)
            {
                reader.Skip();
                return list;
            }

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndArray)
                    return list;

                list.Add(reader.GetString()!);
            }

            return list;
        }

        private static void WriteStringDict(
            Utf8JsonWriter writer, string propertyName,
            IReadOnlyDictionary<string, string> dict)
        {
            writer.WritePropertyName(propertyName);
            writer.WriteStartObject();
            foreach (var kvp in dict)
                writer.WriteString(kvp.Key, kvp.Value);
            writer.WriteEndObject();
        }

        #endregion
    }
}
