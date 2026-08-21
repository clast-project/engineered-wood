// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Ipc;

namespace EngineeredWood.Parquet.Data;

/// <summary>
/// Reads and writes the <c>ARROW:schema</c> footer entry, the ecosystem's convention for the parts
/// of an Arrow schema Parquet cannot express.
/// </summary>
/// <remarks>
/// <para>Parquet records an instant and a unit of MILLIS, MICROS or NANOS; it has no place for a
/// zone NAME or for second precision. PyArrow and Polars therefore stash the original Arrow schema,
/// base64-encoded as an IPC schema message, under this key and restore from it on read. DuckDB does
/// not, which is why a <c>timestamp[us, tz=America/New_York]</c> round-trips through those two and
/// comes back as UTC through DuckDB.</para>
///
/// <para>What the reader restores from it is deliberately narrow — timestamp and time units and
/// zones, and nothing else. Restoring a fixed-size list's width from here is the one thing this
/// pass will not do: the width is not verifiable against the data, and PyArrow's attempt to do it
/// is itself a source of read failures on files whose lists are not uniform.</para>
/// </remarks>
internal static class ArrowSchemaMetadata
{
    /// <summary>The footer key both PyArrow and Polars use.</summary>
    internal const string Key = "ARROW:schema";

    /// <summary>
    /// Encodes <paramref name="schema"/> as the base64 IPC schema message that belongs under
    /// <see cref="Key"/>.
    /// </summary>
    /// <param name="schema">The Arrow schema as the caller declared it, before any coercion.</param>
    internal static string Encode(Apache.Arrow.Schema schema)
    {
        using var buffer = new MemoryStream();
        using (var writer = new ArrowStreamWriter(buffer, Recordable(schema), leaveOpen: true))
        {
            writer.WriteStart();
            writer.WriteEnd();
        }

        return Convert.ToBase64String(buffer.ToArray());
    }

    /// <summary>
    /// Strips the parts of a declared schema that describe how the caller held the data rather than
    /// what the file contains.
    /// </summary>
    /// <remarks>
    /// Run-end encoding is a write-side memory layout, not a type: the column is expanded and
    /// written as its value type, and no reader can tell. Recording <c>run_end_encoded</c> here
    /// would promise a shape the file cannot produce, and would make two files holding identical
    /// data differ only by how the caller happened to build the batch.
    /// </remarks>
    private static Apache.Arrow.Schema Recordable(Apache.Arrow.Schema schema)
    {
        Apache.Arrow.Field[]? fields = null;
        for (int i = 0; i < schema.FieldsList.Count; i++)
        {
            var field = schema.FieldsList[i];
            var type = TimeUnitRescaler.RewriteType(field.DataType, Expanded);
            if (!ReferenceEquals(type, field.DataType))
            {
                fields ??= [.. schema.FieldsList];
                fields[i] = new Apache.Arrow.Field(field.Name, type, field.IsNullable);
            }
        }

        return fields is null
            ? schema
            : new Apache.Arrow.Schema(fields, schema.Metadata);
    }

    private static Apache.Arrow.Types.IArrowType? Expanded(Apache.Arrow.Types.IArrowType type) =>
        type is Apache.Arrow.Types.RunEndEncodedType ree
            ? TimeUnitRescaler.RewriteType(ree.ValuesDataType, Expanded)
            : null;

    /// <summary>
    /// Decodes the schema stored under <see cref="Key"/>, or null when the entry is absent or
    /// unreadable. A malformed entry is metadata, not data — it is ignored rather than fatal.
    /// </summary>
    /// <param name="metadata">The file's key/value metadata.</param>
    internal static Apache.Arrow.Schema? Decode(IReadOnlyList<Metadata.KeyValue>? metadata)
    {
        if (metadata is null)
            return null;

        string? encoded = null;
        foreach (var entry in metadata)
        {
            if (string.Equals(entry.Key, Key, StringComparison.Ordinal))
            {
                encoded = entry.Value;
                break;
            }
        }

        if (string.IsNullOrEmpty(encoded))
            return null;

        try
        {
            using var buffer = new MemoryStream(Convert.FromBase64String(encoded));
            using var reader = new ArrowStreamReader(buffer);

            // The payload is a schema message followed by end-of-stream. Reading the (absent) first
            // batch is what makes the reader parse the schema.
            reader.ReadNextRecordBatch();
            return reader.Schema;
        }
        catch (Exception error) when (error is FormatException or IOException or InvalidDataException
            or ArgumentException or OverflowException or NotSupportedException)
        {
            return null;
        }
    }
}
