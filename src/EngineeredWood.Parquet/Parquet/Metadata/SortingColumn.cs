// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// Describes a sorting column within a row group.
/// </summary>
public readonly record struct SortingColumn(int ColumnIndex, bool Descending, bool NullsFirst);
