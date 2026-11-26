// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// Base type for Iceberg partition and sort transforms. Transforms convert source column values
/// into derived values used for partitioning or sorting.
/// </summary>
public abstract record Transform
{
    /// <summary>The identity transform, which returns the source value unchanged.</summary>
    public static readonly Transform Identity = new IdentityTransform();

    /// <summary>Extracts the year from a date or timestamp.</summary>
    public static readonly Transform Year = new YearTransform();

    /// <summary>Extracts the year and month from a date or timestamp.</summary>
    public static readonly Transform Month = new MonthTransform();

    /// <summary>Extracts the date from a date or timestamp.</summary>
    public static readonly Transform Day = new DayTransform();

    /// <summary>Extracts the date and hour from a timestamp.</summary>
    public static readonly Transform Hour = new HourTransform();

    /// <summary>The void transform, which always produces null.</summary>
    public static readonly Transform Void = new VoidTransform();

    /// <summary>Creates a bucket transform that hashes values into the specified number of buckets.</summary>
    public static BucketTransform Bucket(int numBuckets) => numBuckets > 0
        ? new(numBuckets)
        : throw new ArgumentOutOfRangeException(nameof(numBuckets), numBuckets, "The number of buckets must be positive.");

    /// <summary>Creates a truncate transform that truncates values to the specified width.</summary>
    public static TruncateTransform Truncate(int width) => width > 0
        ? new(width)
        : throw new ArgumentOutOfRangeException(nameof(width), width, "The truncate width must be positive.");
}

/// <summary>Returns the source value unchanged.</summary>
public sealed record IdentityTransform : Transform;

/// <summary>Hashes values into a fixed number of buckets using a 32-bit Murmur3 hash.</summary>
/// <param name="NumBuckets">The number of hash buckets.</param>
public sealed record BucketTransform(int NumBuckets) : Transform;

/// <summary>Truncates values to a specified width (integers to multiples, strings to prefixes).</summary>
/// <param name="Width">The truncation width.</param>
public sealed record TruncateTransform(int Width) : Transform;

/// <summary>Extracts the year from a date or timestamp.</summary>
public sealed record YearTransform : Transform;

/// <summary>Extracts the year and month from a date or timestamp.</summary>
public sealed record MonthTransform : Transform;

/// <summary>Extracts the date from a date or timestamp.</summary>
public sealed record DayTransform : Transform;

/// <summary>Extracts the date and hour from a timestamp.</summary>
public sealed record HourTransform : Transform;

/// <summary>Always produces null, used to remove a partition field while preserving field IDs.</summary>
public sealed record VoidTransform : Transform;

/// <summary>
/// A transform this library does not know, kept by name so that metadata naming it round-trips. A v3 reader
/// must accept unknown transforms (including multi-argument ones, which have <c>source-ids</c>); such a field
/// cannot be evaluated, so it must not be used to filter.
/// </summary>
/// <param name="Name">The transform string exactly as the metadata spells it.</param>
public sealed record UnknownTransform(string Name) : Transform;
