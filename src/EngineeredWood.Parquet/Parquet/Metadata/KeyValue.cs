// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Parquet.Metadata;

/// <summary>
/// A key-value pair from the file metadata.
/// </summary>
public readonly record struct KeyValue(string Key, string? Value);
