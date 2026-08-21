// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Raw bit patterns of floating-point values, for assertions that must distinguish values <c>==</c> calls
/// equal — +0.0 from -0.0 above all, which is what issue #154 turned on.
/// </summary>
/// <remarks>
/// <c>BitConverter.SingleToInt32Bits</c> does not exist on net472, which this test project also targets, so
/// the single-precision side goes the long way round. <c>DoubleToInt64Bits</c> is available everywhere and
/// is wrapped only so the two read alike at the call site.
/// </remarks>
internal static class BitPatterns
{
    public static int Of(float value) => BitConverter.ToInt32(BitConverter.GetBytes(value), 0);

    public static long Of(double value) => BitConverter.DoubleToInt64Bits(value);
}
