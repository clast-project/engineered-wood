// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Types;

namespace EngineeredWood.Core.Tests.Arrow;

/// <summary>
/// One entry per width in <c>ArrowCompute.FixedWidthBytes</c>, shared by the tests for every operation that
/// dispatches through it. Types are named rather than passed as instances so the theory cases stay
/// individually runnable from a test runner, and so one list can be filtered per-operation.
/// </summary>
internal static class FixedWidthCases
{
    /// <summary>
    /// Every fixed-width type the width table covers, <c>halffloat</c> included. Callers restrict this
    /// themselves: what a given target framework can do with a half-float differs per operation, and the
    /// width table itself is unconditional because it keys off <see cref="HalfFloatType"/>, which exists
    /// everywhere — only <c>HalfFloatArray</c> is missing on netstandard2.0.
    /// </summary>
    public static string[] All { get; } =
    [
        "int8", "uint8",
        "int16", "uint16", "halffloat",
        "int32", "uint32", "float", "date32", "time32",
        "int64", "uint64", "double", "date64", "time64",
        "timestamp_us_utc", "timestamp_ms_naive", "duration_ns",
        "decimal32", "decimal64", "decimal128", "decimal256",
        "fsb7",
    ];

    public static (IArrowType Type, int Width) Resolve(string name) => name switch
    {
        "int8" => (Int8Type.Default, 1),
        "uint8" => (UInt8Type.Default, 1),
        "int16" => (Int16Type.Default, 2),
        "uint16" => (UInt16Type.Default, 2),
        "halffloat" => (HalfFloatType.Default, 2),
        "int32" => (Int32Type.Default, 4),
        "uint32" => (UInt32Type.Default, 4),
        "float" => (FloatType.Default, 4),
        "date32" => (Date32Type.Default, 4),
        "time32" => (new Time32Type(TimeUnit.Millisecond), 4),
        "int64" => (Int64Type.Default, 8),
        "uint64" => (UInt64Type.Default, 8),
        "double" => (DoubleType.Default, 8),
        "date64" => (Date64Type.Default, 8),
        "time64" => (new Time64Type(TimeUnit.Microsecond), 8),
        "timestamp_us_utc" => (new TimestampType(TimeUnit.Microsecond, "UTC"), 8),
        "timestamp_ms_naive" => (new TimestampType(TimeUnit.Millisecond, (string?)null), 8),
        // DurationType exposes only static per-unit instances, no public constructor.
        "duration_ns" => (DurationType.Nanosecond, 8),
        "decimal32" => (new Decimal32Type(9, 2), 4),
        "decimal64" => (new Decimal64Type(18, 4), 8),
        "decimal128" => (new Decimal128Type(38, 10), 16),
        "decimal256" => (new Decimal256Type(76, 20), 32),
        // A width that is neither a power of two nor a multiple of one, to catch slot arithmetic that
        // happens to work only for aligned widths.
        "fsb7" => (new FixedSizeBinaryType(7), 7),
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, "unknown case"),
    };
}
