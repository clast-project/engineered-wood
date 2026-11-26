// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Avro.Encoding;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>A decoded Avro record: its schema and one value per schema field, in schema order.</summary>
internal sealed class AvroRecordValue
{
    public AvroRecordValue(AvroRecordSchema schema, object?[] values)
    {
        Schema = schema;
        Values = values;
    }

    public AvroRecordSchema Schema { get; }
    public object?[] Values { get; }
}

/// <summary>
/// Schema-driven generic Avro values, for manifests. Decoding follows the file's own writer schema, so a
/// manifest from any writer, in any field order, decodes; the codec then picks fields out by field id.
/// </summary>
/// <remarks>
/// Values: <c>null</c>, <see cref="bool"/>, <see cref="int"/>, <see cref="long"/>, <see cref="float"/>,
/// <see cref="double"/>, <see cref="string"/> (string and enum symbol), <see cref="byte"/>[] (bytes and fixed),
/// <see cref="List{T}"/> of values (array), <see cref="List{T}"/> of string-keyed pairs (map), and
/// <see cref="AvroRecordValue"/>. A union decodes to its branch's value.
/// </remarks>
internal static class AvroDatum
{
    /// <summary>
    /// Reads one value of <paramref name="schema"/>. A <c>fixed</c> node in <paramref name="lengthPrefixed"/> is
    /// read with a length prefix, as Avro <c>bytes</c>: that is how Iceberg Java writes a bare <c>fixed</c>
    /// (apache/iceberg#17501), whatever its schema says.
    /// </summary>
    public static object? Read(ref AvroBinaryReader r, AvroSchemaNode schema, HashSet<AvroFixedSchema>? lengthPrefixed = null)
    {
        switch (schema)
        {
            case AvroPrimitiveSchema p:
                return p.Type switch
                {
                    AvroType.Null => null,
                    AvroType.Boolean => r.ReadBoolean(),
                    AvroType.Int => r.ReadInt(),
                    AvroType.Long => r.ReadLong(),
                    AvroType.Float => r.ReadFloat(),
                    AvroType.Double => r.ReadDouble(),
                    AvroType.Bytes => r.ReadBytes().ToArray(),
                    AvroType.String => r.ReadString(),
                    _ => throw new InvalidDataException($"Unexpected Avro primitive {p.Type}."),
                };

            case AvroRecordSchema rec:
            {
                var values = new object?[rec.Fields.Count];
                for (int i = 0; i < values.Length; i++)
                    values[i] = Read(ref r, rec.Fields[i].Schema, lengthPrefixed);
                return new AvroRecordValue(rec, values);
            }

            case AvroEnumSchema e:
            {
                int index = r.ReadInt();
                if ((uint)index >= (uint)e.Symbols.Count)
                    throw new InvalidDataException($"Enum index {index} is out of range for '{e.FullName}'.");
                return e.Symbols[index];
            }

            case AvroArraySchema a:
            {
                var items = new List<object?>();
                for (long count = ReadBlockCount(ref r); count != 0; count = ReadBlockCount(ref r))
                {
                    for (long i = 0; i < count; i++)
                        items.Add(Read(ref r, a.Items, lengthPrefixed));
                }
                return items;
            }

            case AvroMapSchema m:
            {
                var pairs = new List<KeyValuePair<string, object?>>();
                for (long count = ReadBlockCount(ref r); count != 0; count = ReadBlockCount(ref r))
                {
                    for (long i = 0; i < count; i++)
                    {
                        var key = r.ReadString();
                        pairs.Add(new KeyValuePair<string, object?>(key, Read(ref r, m.Values, lengthPrefixed)));
                    }
                }
                return pairs;
            }

            case AvroFixedSchema f when lengthPrefixed is not null && lengthPrefixed.Contains(f):
            {
                var bytes = r.ReadBytes();
                if (bytes.Length != f.Size)
                    throw new InvalidDataException(
                        $"A length-prefixed fixed({f.Size}) value '{f.FullName}' has {bytes.Length} bytes.");
                return bytes.ToArray();
            }

            case AvroFixedSchema f:
                return r.ReadFixed(f.Size).ToArray();

            case AvroUnionSchema u:
            {
                int index = r.ReadUnionIndex();
                if ((uint)index >= (uint)u.Branches.Count)
                    throw new InvalidDataException($"Union branch {index} is out of range.");
                return Read(ref r, u.Branches[index], lengthPrefixed);
            }

            default:
                throw new InvalidDataException($"Unexpected Avro schema node {schema.GetType().Name}.");
        }
    }

    /// <summary>
    /// The item count of the next array or map block. A negative count is followed by the block's size in
    /// bytes, which a reader that decodes every item does not need.
    /// </summary>
    private static long ReadBlockCount(ref AvroBinaryReader r)
    {
        long count = r.ReadLong();
        if (count < 0)
        {
            count = -count;
            r.ReadLong();
        }
        return count;
    }

    public static void Write(AvroBinaryWriter w, AvroSchemaNode schema, object? value)
    {
        switch (schema)
        {
            case AvroUnionSchema u:
            {
                int index = BranchFor(u, value);
                w.WriteUnionIndex(index);
                Write(w, u.Branches[index], value);
                return;
            }

            case AvroPrimitiveSchema p:
                switch (p.Type)
                {
                    case AvroType.Null: return;
                    case AvroType.Boolean: w.WriteBoolean((bool)value!); return;
                    case AvroType.Int: w.WriteInt((int)value!); return;
                    case AvroType.Long: w.WriteLong((long)value!); return;
                    case AvroType.Float: w.WriteFloat((float)value!); return;
                    case AvroType.Double: w.WriteDouble((double)value!); return;
                    case AvroType.Bytes: w.WriteBytes((byte[])value!); return;
                    case AvroType.String: w.WriteString((string)value!); return;
                    default: throw new InvalidOperationException($"Unexpected Avro primitive {p.Type}.");
                }

            case AvroRecordSchema rec:
            {
                var record = (AvroRecordValue)value!;
                for (int i = 0; i < rec.Fields.Count; i++)
                    Write(w, rec.Fields[i].Schema, record.Values[i]);
                return;
            }

            case AvroEnumSchema e:
            {
                int index = IndexOf(e.Symbols, (string)value!);
                if (index < 0)
                    throw new InvalidOperationException($"'{value}' is not a symbol of enum '{e.FullName}'.");
                w.WriteInt(index);
                return;
            }

            case AvroArraySchema a:
            {
                var items = (IReadOnlyList<object?>)value!;
                if (items.Count > 0)
                {
                    w.WriteLong(items.Count);
                    foreach (var item in items)
                        Write(w, a.Items, item);
                }
                w.WriteLong(0);
                return;
            }

            case AvroMapSchema m:
            {
                var pairs = (IReadOnlyList<KeyValuePair<string, object?>>)value!;
                if (pairs.Count > 0)
                {
                    w.WriteLong(pairs.Count);
                    foreach (var pair in pairs)
                    {
                        w.WriteString(pair.Key);
                        Write(w, m.Values, pair.Value);
                    }
                }
                w.WriteLong(0);
                return;
            }

            case AvroFixedSchema f:
            {
                var bytes = (byte[])value!;
                if (bytes.Length != f.Size)
                    throw new InvalidOperationException(
                        $"A fixed({f.Size}) value '{f.FullName}' has {bytes.Length} bytes.");
                w.WriteFixed(bytes);
                return;
            }

            default:
                throw new InvalidOperationException($"Unexpected Avro schema node {schema.GetType().Name}.");
        }
    }

    // Manifests use only ["null", T] unions, so a value goes to the null branch or the first other one.
    private static int BranchFor(AvroUnionSchema u, object? value)
    {
        for (int i = 0; i < u.Branches.Count; i++)
        {
            if ((value is null) == (u.Branches[i].Type == AvroType.Null))
                return i;
        }
        throw new InvalidOperationException(
            value is null ? "A null value for a union with no null branch." : "A value for a union with only a null branch.");
    }

    private static int IndexOf(IReadOnlyList<string> symbols, string symbol)
    {
        for (int i = 0; i < symbols.Count; i++)
        {
            if (symbols[i] == symbol)
                return i;
        }
        return -1;
    }
}
