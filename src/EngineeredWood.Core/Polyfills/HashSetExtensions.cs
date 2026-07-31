// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

#if NETSTANDARD2_0

namespace System.Collections.Generic;

/// <summary>
/// Stands in for the capacity-aware members netstandard2.0's <see cref="HashSet{T}"/> lacks.
/// <see cref="HashSet{T}"/> gained <c>EnsureCapacity</c> — along with its <c>(int)</c> and
/// <c>(int, IEqualityComparer&lt;T&gt;)</c> constructors — only in netstandard2.1, so on this target
/// a sizing hint has nowhere to go and is discarded.
/// </summary>
/// <remarks>
/// This exists so call sites can state the size they expect without a <c>#if</c> of their own: on
/// net8.0+ the real instance method wins overload resolution and the hint is honored, and here it
/// costs a call the JIT inlines away. .NET Framework consumers bind against this build too, and its
/// <see cref="HashSet{T}"/> has no <c>EnsureCapacity</c> either, so they land on the same stand-in.
/// Delete this file when netstandard2.0 is dropped; every call site then keeps working unchanged.
/// </remarks>
internal static class HashSetExtensions
{
    /// <summary>
    /// Discards <paramref name="capacity"/> and returns the set's current capacity, standing in for
    /// <c>HashSet&lt;T&gt;.EnsureCapacity</c>. Sizing is an optimization, never a correctness
    /// requirement, so dropping the hint changes nothing a caller can observe beyond the rehashes it
    /// would have saved.
    /// </summary>
    /// <returns>
    /// <see cref="HashSet{T}.Count"/> — the only lower bound on the real capacity reachable here,
    /// since netstandard2.0 exposes no way to read the bucket count.
    /// </returns>
    public static int EnsureCapacity<T>(this HashSet<T> set, int capacity)
    {
        if (set is null)
            throw new ArgumentNullException(nameof(set));
        if (capacity < 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Capacity cannot be negative.");
        return set.Count;
    }
}

#endif
