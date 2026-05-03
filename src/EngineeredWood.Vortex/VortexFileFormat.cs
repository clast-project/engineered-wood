// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Vortex;

/// <summary>
/// File-format constants for Vortex containers.
/// </summary>
internal static class VortexFileFormat
{
    /// <summary>The 4-byte ASCII magic <c>'VTXF'</c> at both the head and tail of every Vortex file.</summary>
    public const uint MagicLE = 0x4658_5456u; // 'V' 'T' 'X' 'F' little-endian

    /// <summary>Length of the leading magic in bytes.</summary>
    public const int LeadingMagicSize = 4;

    /// <summary>
    /// Length of the trailing <c>EndOfFile</c> struct: <c>version:u16 | postscript_len:u16 | magic:4</c>.
    /// </summary>
    public const int EndOfFileSize = 8;

    /// <summary>Maximum allowed postscript length (<c>u16::MAX - 8</c>).</summary>
    public const int MaxPostscriptLen = 65528;

    /// <summary>Default tail-read size used to fetch <c>EndOfFile</c> + postscript in one I/O.</summary>
    public const int DefaultTailReadSize = 64 * 1024;

    /// <summary>The smallest Vortex file is leading magic + EndOfFile = 12 bytes.</summary>
    public const int MinFileSize = LeadingMagicSize + EndOfFileSize;
}
