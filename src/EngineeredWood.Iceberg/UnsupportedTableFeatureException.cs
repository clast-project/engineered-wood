// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// Thrown when a table uses a feature this library cannot honour, where going ahead would misread the table
/// or damage it: a <c>format-version</c> newer than 3, or table encryption. (It is not a
/// <see cref="NotSupportedException"/>: System.Text.Json wraps those when they are thrown while reading.)
/// </summary>
public class UnsupportedTableFeatureException : Exception
{
    /// <summary>Creates the exception with the given message.</summary>
    public UnsupportedTableFeatureException(string message) : base(message) { }
}
