// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;

namespace EngineeredWood.Parquet;

/// <summary>
/// A batch read from a Parquet file, with the position of its first row in the file: rows before it
/// that pruning skipped still count.
/// </summary>
/// <param name="Batch">The rows.</param>
/// <param name="FirstRow">The file position (counted from the file's first row) of the batch's first row;
/// its other rows follow contiguously.</param>
public readonly record struct PositionedRecordBatch(RecordBatch Batch, long FirstRow);
