// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Where one data page of a column chunk lives, as recorded in an <see cref="OffsetIndex"/>.
/// </summary>
/// <param name="Offset">File offset of the page's header.</param>
/// <param name="CompressedPageSize">Size of the page in bytes, header included.</param>
/// <param name="FirstRowIndex">
/// Index, within the row group, of the first row in the page. Pages begin on row boundaries whenever
/// an OffsetIndex is present.
/// </param>
public readonly record struct PageLocation(long Offset, int CompressedPageSize, long FirstRowIndex);
