// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Reflection;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Rewrites a file's footer in place, for tests of what a reader does with metadata a writer would
/// not produce. The metadata classes are init-only, so edits go through <see cref="With"/>.
/// </summary>
internal static class FooterRewrite
{
    /// <summary>Replaces the footer of the file at <paramref name="path"/> with an edited copy.</summary>
    public static void Rewrite(string path, Func<FileMetaData, FileMetaData> edit)
    {
        byte[] bytes = File.ReadAllBytes(path);
        int footerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        int bodyLength = bytes.Length - 8 - footerLength;
        var metadata = MetadataDecoder.DecodeFileMetaData(bytes.AsSpan(bodyLength, footerLength));
        byte[] footer = MetadataEncoder.EncodeFileMetaData(edit(metadata));

        using var output = new MemoryStream();
        output.Write(bytes, 0, bodyLength);
        output.Write(footer, 0, footer.Length);
        var suffix = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(suffix, footer.Length);
        "PAR1"u8.CopyTo(suffix.AsSpan(4));
        output.Write(suffix, 0, suffix.Length);
        File.WriteAllBytes(path, output.ToArray());
    }

    /// <summary>A copy of <paramref name="source"/> with one init-only property replaced.</summary>
    public static T With<T>(T source, string property, object? value)
        where T : class
    {
        var copy = (T)typeof(object).GetMethod("MemberwiseClone", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(source, null)!;
        typeof(T).GetProperty(property)!.SetValue(copy, value);
        return copy;
    }
}
