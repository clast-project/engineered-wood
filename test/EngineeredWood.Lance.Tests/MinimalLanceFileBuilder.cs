// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Lance.Format;
using EngineeredWood.Lance.Proto;
using EngineeredWood.Lance.Proto.V2;
using Google.Protobuf;

namespace EngineeredWood.Lance.Tests;

/// <summary>
/// Hand-crafts minimal in-memory Lance v2.x files for envelope-layer testing.
/// Layout produced (no padding, no compression):
/// <code>
///   [ global buffer 0 (FileDescriptor)  ]
///   [ additional global buffers ...     ]
///   [ column metadata blobs ...         ]
///   [ CMO table (16 B × NumColumns)     ]
///   [ GBO table (16 B × NumGlobalBufs)  ]
///   [ footer (40 B)                     ]
/// </code>
/// </summary>
internal sealed class MinimalLanceFileBuilder
{
    public FileDescriptor FileDescriptor { get; set; } = new FileDescriptor
    {
        Schema = new Proto.Schema(),
        Length = 0,
    };

    public List<byte[]> AdditionalGlobalBuffers { get; } = new();

    public List<ColumnMetadata> Columns { get; } = new();

    public LanceVersion Version { get; set; } = LanceVersion.V2_1;

    /// <summary>When set, overrides the magic bytes written as the last 4 bytes.</summary>
    public byte[]? OverrideMagic { get; set; }

    /// <summary>Add a leaf field of the given Lance logical_type to the FileDescriptor's schema.</summary>
    public MinimalLanceFileBuilder AddLeafField(
        string name, string logicalType, bool nullable = true, int? id = null)
    {
        int assigned = id ?? FileDescriptor.Schema.Fields.Count + 1;
        FileDescriptor.Schema.Fields.Add(new Proto.Field
        {
            Type = Proto.Field.Types.Type.Leaf,
            Name = name,
            Id = assigned,
            ParentId = 0,
            LogicalType = logicalType,
            Nullable = nullable,
        });
        return this;
    }

    /// <summary>Add an empty ColumnMetadata (no pages) so the footer's NumColumns matches.</summary>
    public MinimalLanceFileBuilder AddEmptyColumn()
    {
        Columns.Add(new ColumnMetadata());
        return this;
    }

    public byte[] Build()
    {
        using var stream = new MemoryStream();

        // --- 1. Global buffers ---
        byte[] fileDescriptorBytes = FileDescriptor.ToByteArray();
        var globalBufferEntries = new List<OffsetSizeEntry>(1 + AdditionalGlobalBuffers.Count);

        long gb0Position = stream.Position;
        stream.Write(fileDescriptorBytes, 0, fileDescriptorBytes.Length);
        globalBufferEntries.Add(new OffsetSizeEntry(gb0Position, fileDescriptorBytes.Length));

        foreach (byte[] buf in AdditionalGlobalBuffers)
        {
            long pos = stream.Position;
            stream.Write(buf, 0, buf.Length);
            globalBufferEntries.Add(new OffsetSizeEntry(pos, buf.Length));
        }

        // --- 2. Column metadata blobs ---
        long columnMetaStart = stream.Position;
        var columnEntries = new List<OffsetSizeEntry>(Columns.Count);
        foreach (ColumnMetadata cm in Columns)
        {
            byte[] bytes = cm.ToByteArray();
            long pos = stream.Position;
            stream.Write(bytes, 0, bytes.Length);
            columnEntries.Add(new OffsetSizeEntry(pos, bytes.Length));
        }

        // --- 3. CMO table ---
        long cmoTableOffset = stream.Position;
        WriteOffsetTable(stream, columnEntries);

        // --- 4. GBO table ---
        long gboTableOffset = stream.Position;
        WriteOffsetTable(stream, globalBufferEntries);

        // --- 5. Footer ---
        var footer = new LanceFooter(
            ColumnMetaStart: Columns.Count == 0 ? cmoTableOffset : columnMetaStart,
            CmoTableOffset: cmoTableOffset,
            GboTableOffset: gboTableOffset,
            NumGlobalBuffers: globalBufferEntries.Count,
            NumColumns: Columns.Count,
            Version: Version);

        Span<byte> footerSpan = stackalloc byte[LanceFooter.Size];
        footer.WriteTo(footerSpan);

        if (OverrideMagic is { } magic)
        {
            if (magic.Length != 4)
                throw new ArgumentException("Override magic must be 4 bytes.");
            magic.AsSpan().CopyTo(footerSpan.Slice(36, 4));
        }

        stream.Write(footerSpan.ToArray(), 0, LanceFooter.Size);
        return stream.ToArray();
    }

    private static void WriteOffsetTable(MemoryStream stream, List<OffsetSizeEntry> entries)
    {
        Span<byte> buf = stackalloc byte[OffsetSizeEntry.Bytes];
        foreach (OffsetSizeEntry entry in entries)
        {
            entry.WriteTo(buf);
            stream.Write(buf.ToArray(), 0, OffsetSizeEntry.Bytes);
        }
    }
}
