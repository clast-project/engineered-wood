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

## What is and is not exposed

The distinction is **who allocated the buffer**, not which code reads it.

| Provenance | Backing | Span safe? |
| --- | --- | --- |
| `new ArrowBuffer(byte[])` — how EngineeredWood builds every buffer it constructs | managed array | **Yes.** The span's interior pointer roots the array. |
| Arrow builders — how callers normally build the arrays they hand us | native, finalizable | **No.** Needs an explicit root. |

Two consequences:

- **Read/decode paths are safe.** Every array EngineeredWood constructs is managed-backed, so the
  Parquet, ORC, Avro, Lance and Vortex decoders — which only ever see arrays reconstructed from
  bytes on disk — are not exposed, and were deliberately left alone.
- **Write and analysis paths are exposed**, because those receive arrays the caller built.

The exceptions inside `src/` — the few places EngineeredWood itself calls an Arrow builder — are
`ArrowBuffer.BitmapBuilder` in the ORC `UnionColumnWriter` and the `Int64Array.Builder` /
`UInt32Array.Builder` uses in the Delta Lake and Lance table layers.

## The rule

> A method that reads spans off an Arrow array it did **not** construct must keep that array
> reachable until after the last read: `GC.KeepAlive(array)`.

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
