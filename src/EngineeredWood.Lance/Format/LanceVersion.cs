// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance.Format;

/// <summary>
/// Lance file format version, as encoded in the last 8 bytes before the footer
/// magic (two <c>u16</c> values).
/// </summary>
public readonly record struct LanceVersion(ushort Major, ushort Minor)
{
    /// <summary>
    /// v2.0 with the current footer numbering. Used by writers that emit the
    /// updated version bytes (e.g., when <c>data_storage_version = '2.0'</c>
    /// is explicitly selected in a new-enough writer).
    /// </summary>
    public static LanceVersion V2_0 => new(2, 0);

    /// <summary>
    /// v2.0 with the legacy footer numbering. pylance 4.x writes these bytes
    /// when <c>data_storage_version = '2.0'</c>; readers must treat it as v2.0.
    /// </summary>
    public static LanceVersion V2_0_Legacy => new(0, 3);

    /// <summary>v2.1 — stable since 2025-10-03; default for current writers.</summary>
    public static LanceVersion V2_1 => new(2, 1);

    /// <summary>
    /// Legacy v0.1 layout. Footer bytes are <c>(major, minor) = (0, 2)</c>
    /// for historical reasons; the on-disk format is entirely different and
    /// not supported by this reader.
    /// </summary>
    public static LanceVersion Legacy_V0_1 => new(0, 2);

    public bool IsSupported => IsV2_0 || IsV2_1;

    public bool IsV2_0 => this == V2_0 || this == V2_0_Legacy;

    public bool IsV2_1 => this == V2_1;

    public override string ToString() => $"{Major}.{Minor}";
}
