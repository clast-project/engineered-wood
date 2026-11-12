# Arrow buffer spans and object lifetime

A `Span<byte>` taken off an Apache Arrow buffer can outlive the memory it points at. This note
records why, which code is exposed, and the rule the codebase follows.

## The hazard

`MemoryAllocator.Default` is a `NativeMemoryAllocator`, so a buffer built by any Arrow builder
(`StringArray.Builder`, `ArrowBuffer.Builder<T>`, `ArrowBuffer.BitmapBuilder`, …) lives in
`Marshal.AllocHGlobal` memory owned by a `NativeMemoryManager`. That class has a **finalizer** that
frees the allocation:

```csharp
~NativeMemoryManager() => Dispose(false);        // -> _owner.Release(ptr, …) -> Marshal.FreeHGlobal
```

A span over that memory is a raw pointer. Unlike a span over a managed `byte[]` — where the interior
pointer is reported to the GC and roots the array — it keeps **nothing** alive. So:

```csharp
var data = array.Data;
ReadOnlySpan<byte> values = data.Buffers[2].Span;   // `array` and `data` are now dead
for (int i = 0; i < rowCount; i++) { … }            // allocates -> may trigger a GC
```

is a use-after-free. The GC collects `array`, the finalizer frees the buffer, and the loop keeps
reading freed memory.

It fails intermittently rather than always, which is what makes it dangerous. Freed `HGlobal` pages
usually stay mapped, so the span normally returns *correct data* and nothing is noticed; it faults
only when the page is actually returned to the OS. Silent corruption is the common case and
`AccessViolationException` is the rare one.

This was observed as a CI failure: `DictionaryEncoder.TryEncodeByteArray` took an
`AccessViolationException` and killed the net10.0 Parquet test host mid-run. It reproduced on a
docs-only commit, having passed on the three commits before it.

It was seen again in #834: `ContainerResolutionTests.FloatItems_PromoteToDouble` read
`[1.11E+253, 6.94E+228]` for `[1.5, -2.25]`, on net472 only, in one CI run. The test took a span off
an array the Avro reader had built, and nothing used the array after that.

## What is and is not exposed

The distinction is **who allocated the buffer**, not which code reads it.

| Provenance | Backing | Span safe? |
| --- | --- | --- |
| `new ArrowBuffer(byte[])`, as `ArrowCompute` builds its results | managed array | **Yes.** The span's interior pointer roots the array. |
| Arrow builders (`StringArray.Builder`, `ArrowBuffer.BitmapBuilder`, …) | native, finalizable | **No.** Needs an explicit root. |
| EngineeredWood's `NativeBuffer<T>`, handed to Arrow by `Build()` | native, finalizable | **No.** Needs an explicit root. |

Both native kinds are common, **including the arrays EngineeredWood decodes**. This note first
claimed that every array EngineeredWood constructs was managed-backed, and so that read paths were not
exposed. That stopped being true when the decoders moved to native buffers, and #834 is what it cost:
- **Readers:** the Parquet reader (`ColumnBuildState`), the ORC column readers, the Avro
  `RecordBatchAssembler` and the Delta checkpoint builders build their output in `NativeBuffer<T>`.
- **Builders inside `src/`:** Arrow builders are used throughout, for instance for the expression
  evaluator's results, in the Delta table layer, and in the Parquet and Vortex libraries.

So **every consumer of a decoded array is exposed**: tests, a host, and the library's own code
downstream of a reader. A write path is exposed too, since it receives arrays the caller built.

## The rule

> A method that reads spans off an Arrow array must keep that array reachable until after the last
> read, unless it knows the array is managed-backed: `GC.KeepAlive(array)`.

This covers an array the method was given and one it decoded itself. A span over an array that stays
in use afterwards needs nothing: the later use keeps it alive. The hazard is the **last** use. Typical
shapes are `array.Values.ToArray()`, `array.GetBytes(i).ToArray()` and a span stored in a local
(`var raw = MemoryMarshal.Cast<byte, int>(array.Data.Buffers[1].Span)`), when nothing mentions the
array afterwards. A `using var batch` also works, since disposing the batch at the end of the scope is
a use.

`GC.KeepAlive` is a no-op the JIT treats as a use, so it costs nothing at run time and extends the
array's liveness over everything above it — including spans taken by callees, since the buffers stay
reachable through the array. The root therefore goes at the **outermost method that receives the
caller's array**, not at each of the ~325 places a span is taken; one root covers the whole call
tree beneath it.

Where a call tree has more than one way in — `DictionaryEncoder.TryEncode` is reachable both from
`ColumnChunkWriter.WriteColumn` and directly from tests — each entry point roots for itself rather
than relying on its callers, since an unstated "you must keep this alive" contract is exactly what
this class of bug is made of.

## Demonstrating it

The finalizer only runs when the JIT's liveness is precise, so a demonstration needs
`DOTNET_TieredCompilation=0` (tier-0 keeps locals alive to the end of the method, which masks it):

```csharp
static bool Unfixed()
{
    var array = Build(200_000);                                  // StringArray.Builder -> native
    var w = Owner(array);                                        // WeakReference to its NativeMemoryManager
    ReadOnlySpan<byte> values = array.Data.Buffers[2].Span;
    GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();   // what the encoder's own allocations do
    _ = values.Length;
    return w.IsAlive;                                            // False -> buffer freed under the span
}
```

Adding `GC.KeepAlive(array)` before the return makes it `True`.

For #834 the same demonstration was run in Release, the configuration CI tests in, against the Avro
reader's output. The array's owner was finalized once nothing referenced it (a `WeakReference` to it
read dead every time). A span taken before that read freed memory on net472: one run in three returned
`4.24E-314` for `1.5`. On net10.0 the freed pages kept their contents, so the same code returned the
right values, which is why the failure shows up rarely and in one runtime.

## Finding the pattern

`build/span-lifetime-scan` is a Roslyn scan for the pattern. It compiles a project's sources against
its built assemblies, and finds every `Span`/`ReadOnlySpan` an Apache.Arrow member returns. It then
walks the receiver to its root: a local, a parameter or a temporary. It reports a site when nothing
mentions that root later in the enclosing method, lambda or local function. Its `README.md` says how
to run it, and how its fix mode inserts `GC.KeepAlive` after the last read of the span, following the
span into locals (`Cast`, `Slice`).

Its limits, so that a clean run is not read as proof:
- **Liveness is lexical.** An owner mentioned later on another path (in a `throw` after a `return`,
  say) counts as alive.
- **A captured variable is reported.** A lambda's captured local lives in a closure the running
  delegate references, so it is alive. Such reports are false positives.
- **A span indexed or measured where it is taken** (`array.ValueOffsets[i]`, `.Length`) is reported,
  but left alone. No call runs between taking it and reading it, so a collection would have to land
  in that instant.

**Swept so far (#834):** every test project. Spans read off an array that nothing used afterwards now
end with `GC.KeepAlive(owner)`. The shared renderers (`ArrowValues.Render` in the Parquet and Vortex
tests, and the helpers like them) root their array once, at the entry point. The library itself
(`src/`, about 210 sites the scan reports) is the follow-up, tracked in #837.
