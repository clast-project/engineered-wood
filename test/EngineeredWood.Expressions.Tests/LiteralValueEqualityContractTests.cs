// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Numerics;

namespace EngineeredWood.Expressions.Tests;

/// <summary>
/// The three .NET contracts <see cref="LiteralValue.Equals(LiteralValue)"/> owes, checked over
/// every kind rather than over the pairs that happened to be interesting (#206).
/// </summary>
/// <remarks>
/// All three were broken while equality delegated to the SQL comparison, and each broke in its own
/// way, so each is pinned separately: the relation was not transitive, equal values could hash
/// differently, and <c>Equals</c> THREW for any pair SQL declines to compare. The last was the one
/// the issue did not name and the easiest to hit — <c>Of(1).Equals(Of("x"))</c> raised.
/// </remarks>
public class LiteralValueEqualityContractTests
{
    /// <summary>
    /// At least two values of every <see cref="LiteralValue.Kind"/>, including the pairs that
    /// cross kinds at one numeric value.
    /// </summary>
    private static LiteralValue[] Samples() =>
    [
        LiteralValue.Null,
        LiteralValue.Of(false), LiteralValue.Of(true),
        LiteralValue.Of(1), LiteralValue.Of(42), LiteralValue.Of(-1),
        LiteralValue.Of(1L), LiteralValue.Of(42L), LiteralValue.Of(9007199254740992L),
        LiteralValue.Of(1u), LiteralValue.Of(42u),
        LiteralValue.Of(1ul), LiteralValue.Of(ulong.MaxValue),
        LiteralValue.Of(1.0f), LiteralValue.Of(1.5f), LiteralValue.Of(float.NaN),
        LiteralValue.Of(1.0d), LiteralValue.Of(1.5d), LiteralValue.Of(double.NaN),
        LiteralValue.Of(1m), LiteralValue.Of(1.00m), LiteralValue.Of(1.5m),
        LiteralValue.HighPrecisionDecimalOf(1, 0),
        LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("9007199254740993"), 0),
        LiteralValue.Of("1"), LiteralValue.Of("x"), LiteralValue.Of(""),
        LiteralValue.Of(new byte[] { 1 }), LiteralValue.Of(new byte[] { 1, 2 }), LiteralValue.Of([]),
        LiteralValue.Of(Guid.Empty), LiteralValue.Of(new Guid("00000000-0000-0000-0000-000000000001")),
        LiteralValue.Of(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero)),
        LiteralValue.Of(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.FromHours(1))),
#if NET6_0_OR_GREATER
        LiteralValue.Of((Half)1.0f), LiteralValue.Of((Half)1.5f),
        LiteralValue.Of(new DateOnly(2024, 1, 1)), LiteralValue.Of(new DateOnly(2024, 1, 2)),
        LiteralValue.Of(new TimeOnly(0, 0)), LiteralValue.Of(new TimeOnly(1, 30)),
#endif
    ];

    /// <summary>
    /// <c>Equals</c> answers for every pair of kinds instead of throwing on the ones SQL cannot
    /// compare.
    /// </summary>
    /// <remarks>
    /// Measured before the fix, every one of these raised
    /// <c>InvalidOperationException("Cannot compare LiteralValue of kind String with kind Int32")</c>
    /// and the like: a string against a number, a boolean against a number, a Guid against
    /// anything, and — because the null handling lives in <c>CompareTo</c> rather than in the
    /// cross-type helper that equality reached — the null literal against any value at all.
    /// </remarks>
    [Fact]
    public void Equals_NeverThrows_ForAnyPairOfKinds()
    {
        var samples = Samples();
        foreach (var a in samples)
        {
            foreach (var b in samples)
            {
                _ = a.Equals(b);
                _ = a == b;
                _ = a.Equals((object)b);
                _ = a.GetHashCode();
            }
        }
    }

    /// <summary>Equal values hash equally — the contract a hash-based container relies on.</summary>
    /// <remarks>
    /// The old failure needed no exotic input: <c>Of(1)</c> and <c>Of(1.0d)</c> were <c>Equals</c>
    /// — the cross-type branch widened both to double — while hashing 1 and 1072693248.
    /// </remarks>
    [Fact]
    public void EqualValues_HashEqually()
    {
        var samples = Samples();
        foreach (var a in samples)
        {
            foreach (var b in samples)
            {
                if (a.Equals(b))
                    Assert.Equal(a.GetHashCode(), b.GetHashCode());
            }
        }
    }

    /// <summary>Reflexive, symmetric and transitive, over the same sample set.</summary>
    [Fact]
    public void Equals_IsAnEquivalenceRelation()
    {
        var samples = Samples();
        foreach (var a in samples)
            Assert.True(a.Equals(a), $"not reflexive for {a.Type} {a}");

        foreach (var a in samples)
        {
            foreach (var b in samples)
                Assert.Equal(a.Equals(b), b.Equals(a));
        }

        foreach (var a in samples)
        {
            foreach (var b in samples)
            {
                if (!a.Equals(b))
                    continue;

                foreach (var c in samples)
                {
                    if (b.Equals(c))
                        Assert.True(a.Equals(c), $"not transitive: {a.Type} {a}, {b.Type} {b}, {c.Type} {c}");
                }
            }
        }
    }

    /// <summary>
    /// The issue's sharp transitivity example, showing both relations at once: <c>CompareTo</c>
    /// still gives Spark's three answers, and <c>Equals</c> is transitive over the same triple.
    /// </summary>
    /// <remarks>
    /// The decimal 9007199254740993 (2^53+1) and the double 9007199254740992 compare equal because
    /// both widen to double, where 2^53+1 does not exist. That double compares equal to the long
    /// 9007199254740992. The decimal does not compare equal to that long, because
    /// decimal-against-integer stays exact. All three match Spark; the first two holding while the
    /// third does not is exactly the shape an equivalence relation forbids.
    /// </remarks>
    [Fact]
    public void CompareTo_KeepsSparksNonTransitiveAnswers_WhileEqualsIsTransitive()
    {
        var dec = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("9007199254740993"), 0);
        var dbl = LiteralValue.Of(9007199254740992.0d);
        var lng = LiteralValue.Of(9007199254740992L);

        Assert.Equal(0, dec.CompareTo(dbl));
        Assert.Equal(0, dbl.CompareTo(lng));
        Assert.NotEqual(0, dec.CompareTo(lng));

        Assert.False(dec.Equals(dbl));
        Assert.False(dbl.Equals(lng));
        Assert.False(dec.Equals(lng));
    }

    /// <summary>A literal is usable as a key across mixed kinds.</summary>
    /// <remarks>
    /// Before the fix this set could not even be BUILT: adding a string after a number threw from
    /// <c>Equals</c> on a hash-bucket collision, and a lookup of <c>Of(1)</c> against a stored
    /// <c>Of(1.0d)</c> missed, because the two were equal but hashed apart.
    /// </remarks>
    [Fact]
    public void UsableAsAHashKey_AcrossMixedKinds()
    {
        var set = new HashSet<LiteralValue>(Samples());
        foreach (var v in Samples())
            Assert.Contains(v, set);

        var map = new Dictionary<LiteralValue, string>
        {
            [LiteralValue.Of(1)] = "int",
            [LiteralValue.Of(1L)] = "long",
            [LiteralValue.Of(1.0d)] = "double",
            [LiteralValue.Of("1")] = "string",
            [LiteralValue.Null] = "null",
        };

        Assert.Equal("int", map[LiteralValue.Of(1)]);
        Assert.Equal("long", map[LiteralValue.Of(1L)]);
        Assert.Equal("double", map[LiteralValue.Of(1.0d)]);
        Assert.Equal("string", map[LiteralValue.Of("1")]);
        Assert.Equal("null", map[LiteralValue.Null]);
        Assert.False(map.ContainsKey(LiteralValue.Of(1.5d)));
    }

    /// <summary>
    /// The same value in different numeric kinds gets different hashes, so a hash container does
    /// not pile them into one bucket.
    /// </summary>
    /// <remarks>
    /// A hash is allowed to collide, so this pins a QUALITY property rather than a contract --
    /// but the property was measurably bad. Unmixed, every numeric zero hashed to 0, and so did
    /// the null literal, <c>false</c> and midnight: an eleven-deep bucket on the single most
    /// common value in any predicate. The kinds here all hash deterministically (unlike
    /// <c>string</c>, whose hash is randomised per process), so demanding ZERO collisions across
    /// this population is stable rather than lucky.
    /// </remarks>
    [Fact]
    public void SameValueInDifferentKinds_DoesNotShareAHash()
    {
        var population = new List<LiteralValue>();
        for (int i = 0; i < 64; i++)
        {
            population.Add(LiteralValue.Of(i));
            population.Add(LiteralValue.Of((long)i));
            population.Add(LiteralValue.Of((uint)i));
            population.Add(LiteralValue.Of((ulong)i));
            population.Add(LiteralValue.Of((float)i));
            population.Add(LiteralValue.Of((double)i));
            population.Add(LiteralValue.Of((decimal)i));
            population.Add(LiteralValue.HighPrecisionDecimalOf(i, 0));
        }

        population.Add(LiteralValue.Null);
        population.Add(LiteralValue.Of(false));
        population.Add(LiteralValue.Of(true));

        // Every value is distinct under Equals, so every shared hash is a pure collision.
        Assert.Equal(population.Count, new HashSet<LiteralValue>(population).Count);

        var buckets = population.GroupBy(v => v.GetHashCode()).ToList();
        var worst = buckets.OrderByDescending(b => b.Count()).First();
        Assert.True(
            worst.Count() == 1,
            $"hash {worst.Key} holds {worst.Count()}: "
                + string.Join(", ", worst.Select(v => $"{v.Type}({v})")));
        Assert.Equal(population.Count, buckets.Count);
    }

    /// <summary>
    /// Documents the limitation the type docs warn about: <c>CompareTo</c> is not a total order,
    /// so a sorted collection spanning kinds is unsafe.
    /// </summary>
    /// <remarks>
    /// Documentation rather than a guarantee — making the comparison total later should fail here
    /// and take the warning with it. The evidence is therefore stated about the COMPARER, which is
    /// ours and specified, not about what a container does with a comparer that breaks its
    /// contract, which is not: <see cref="SortedSet{T}"/> given an intransitive comparison has no
    /// defined outcome, so an exact surviving count would pin the red-black tree's insertion order
    /// rather than anything about this type. Only the two container facts that follow from a
    /// single root comparison, and so hold for any binary search tree, are asserted.
    /// </remarks>
    [Fact]
    public void CompareTo_IsNotATotalOrder_SoSortedCollectionsAreUnsafe()
    {
        var dec = LiteralValue.HighPrecisionDecimalOf(BigInteger.Parse("9007199254740993"), 0);
        var dbl = LiteralValue.Of(9007199254740992.0d);
        var lng = LiteralValue.Of(9007199254740992L);

        // The comparer itself, which is the actual claim. It declines some pairs outright, and
        // calls others equal that Equals separates -- so it cannot rank these three.
        Assert.Throws<InvalidOperationException>(
            () => LiteralValue.Of(1).CompareTo(LiteralValue.Of("x")));
        Assert.Equal(0, dec.CompareTo(dbl));
        Assert.False(dec.Equals(dbl));
        Assert.NotEqual(0, dec.CompareTo(lng));
        Assert.Equal(3, new HashSet<LiteralValue> { dec, dbl, lng }.Count);

        // What that costs a sorted container. Adding a second element compares it against the
        // root, so both of these follow from one comparison whatever the tree does afterwards:
        // the throw escapes, and a set of one reports a member it does not hold.
        Assert.Throws<InvalidOperationException>(
            () => new SortedSet<LiteralValue> { LiteralValue.Of(1), LiteralValue.Of("x") });
        Assert.Contains(dec, new SortedSet<LiteralValue> { dbl });

        // At least one of three distinct values is lost. WHICH, and how many, is the container's
        // business under a comparer it was never promised, so that is deliberately not pinned.
        Assert.True(new SortedSet<LiteralValue> { dec, dbl, lng }.Count < 3);
        Assert.True(new SortedSet<LiteralValue> { lng, dbl, dec }.Count < 3);
    }

    /// <summary>
    /// <see cref="SetPredicate"/>, the container the issue names, hashes in step with its own
    /// equality.
    /// </summary>
    /// <remarks>
    /// It folds each literal's hash into its own, so a pair of trees that were <c>Equals</c> with
    /// different hashes made the predicate itself an unreliable key. The IN semantics are a
    /// separate thing and are unchanged — the evaluators ask <c>CompareTo</c>, never
    /// <c>Equals</c>.
    /// </remarks>
    [Fact]
    public void SetPredicate_HashesInStepWithItsEquality()
    {
        var column = new UnboundReference("c");
        SetPredicate In(params LiteralValue[] values) => new(column, values, SetOperator.In);

        var ints = In(LiteralValue.Of(1), LiteralValue.Of(2));
        var same = In(LiteralValue.Of(1), LiteralValue.Of(2));
        var doubles = In(LiteralValue.Of(1.0d), LiteralValue.Of(2.0d));

        Assert.Equal(ints, same);
        Assert.Equal(ints.GetHashCode(), same.GetHashCode());

        // Equal under SQL but a different tree, and now honestly unequal rather than equal with a
        // hash that disagreed.
        Assert.NotEqual(ints, doubles);
    }
}
