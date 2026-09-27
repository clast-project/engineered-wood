// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// How many pages of one type in a column chunk used one encoding (the Thrift
/// <c>PageEncodingStats</c> in <c>ColumnMetaData.encoding_stats</c>).
/// </summary>
/// <remarks>
/// This is the only footer field that can prove a chunk is entirely dictionary-encoded.
/// <see cref="ColumnMetaData.Encodings"/> cannot: parquet-cpp, parquet-mr's V2 writer and
/// EngineeredWood list <see cref="Encoding.Plain"/> for the dictionary page of a chunk that never
/// fell back, so the list looks the same as one that did. In parquet-testing,
/// <c>large_string_map.brotli.parquet</c> (parquet-cpp 11.0.0) holds a chunk that fell back, and its
/// list differs from an all-dictionary sibling's only by a repeated <see cref="Encoding.Plain"/>.
/// <para>Compare the two data-page types as one: parquet-cpp counts a V2 data page as
/// <see cref="PageType.DataPage"/> (parquet-testing's <c>page_v2_empty_compressed.parquet</c>,
/// parquet-cpp 14.0.2). EngineeredWood records the type it wrote.</para>
/// </remarks>
/// <param name="PageType">The page type counted.</param>
/// <param name="Encoding">The encoding of those pages' values.</param>
/// <param name="Count">The number of such pages.</param>
public readonly record struct PageEncodingStats(PageType PageType, Encoding Encoding, int Count);
