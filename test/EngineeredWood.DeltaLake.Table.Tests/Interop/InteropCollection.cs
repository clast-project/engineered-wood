// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Tests.Interop;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// Serializes the interop test classes against each other. They share ONE serve-mode Spark process
/// and one lock (<see cref="InteropDriver"/>), so running the classes in parallel buys no throughput
/// — it only puts more threads into the 600-second wait for that lock.
/// </summary>
/// <remarks>
/// <para>That wait is what turns a slow run into a RED one. A command's timeout covers the queue as
/// well as the work, so with enough classes in flight the last one in line can exhaust 600s having
/// done nothing wrong, and the failure names whichever test happened to be last rather than the one
/// holding the process. It reads as a stalled JVM and is not one.</para>
///
/// <para>The same signature — an ~11 minute run, one arbitrary interop test timing out — was
/// diagnosed once before on .NET Framework, where thread-pool injection starved the driver's stderr
/// drain (fixed 2026-07-28, <c>672dd25</c>, by moving the stdout reader to a dedicated thread). This
/// collection is the other lever that investigation identified and did not need at the time; adding a
/// fourth interop class made it needed. See <c>doc/running-tests.md</c>.</para>
/// </remarks>
[CollectionDefinition("Interop", DisableParallelization = true)]
public sealed class InteropCollection
{
}
