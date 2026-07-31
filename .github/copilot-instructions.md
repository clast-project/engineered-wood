# Copilot Instructions — High-Performance Multi-Target C#

This project is a performance-critical C# codebase that targets multiple .NET runtimes. All code contributions — including those from AI assistants — must follow these conventions.

---

## Target Frameworks and Multi-Targeting

| Priority | Framework | TFM | Notes |
|----------|-----------|-----|-------|
| **Primary** | .NET 10+ | `net10.0` | Optimize for this target; use latest APIs directly |
| Near-term | .NET 8 | `net8.0` | LTS release; polyfill or conditional-compile where needed |
| Floor | .NET Standard 2.0 | `netstandard2.0` | The portability floor for every library; fallback implementations are expected to be slower |

**Every library under `src/` targets `net10.0;net8.0;netstandard2.0`.** The
`net472` TFM appears only in the test projects and the `Parquet.TestTool`
executable — do not assume it when writing library code. Use `#if` preprocessor
directives to provide the best implementation for each target:

```csharp
#if NET10_0_OR_GREATER
    // Optimal implementation using the latest .NET APIs
#elif NET8_0_OR_GREATER
    // .NET 8 implementation — use available modern APIs
#else
    // netstandard2.0 fallback — correctness over speed is acceptable
#endif
```

### What netstandard2.0 actually costs you

This is the leg that breaks builds, and it is far more restrictive than
.NET Framework 4.7.2 would be. Recurring traps:

- **No `Half`, `DateOnly`, `TimeOnly`, or `Index`/`Range`.** A `HalfFloatArray`
  cannot even be constructed there, so APIs reaching one must either be
  unreachable on that leg or throw with an explanation.
- **No `Span`-returning BCL APIs**, and no `Encoding.GetString(ReadOnlySpan<byte>)`,
  `Stream.Read(Span<byte>)`, or `Convert.TryFromBase64Chars` — the span overloads
  largely do not exist.
- **No `Array.Fill`**, no `string.Contains(char)`, no `Dictionary.TryAdd`, no
  `HashCode`, no `MathF` in places you expect it.
- **`System.Runtime.CompilerServices.Unsafe` and `System.Memory` come from
  packages**, not the platform, so their behaviour and availability differ.

Guard with `#if NET8_0_OR_GREATER` (not `#if NET472`), and prefer adding a small
polyfill over contorting the shared code path.

When a feature or API does not exist on an older target, it is acceptable — and encouraged — to **copy small, self-contained portions of the .NET runtime** into the project under an `Internal/` or `Polyfills/` folder. Examples include:

- `SequenceReader<T>` for span-based parsing on netstandard2.0
- `Dictionary<TKey, TValue>` with alternate lookup support
- `SearchValues<T>` or `FrozenDictionary<TKey, TValue>` backports
- String interpolation handler patterns

Preserve the original .NET license header when copying runtime code. Keep copies minimal and focused — only bring in what is needed.

---

## C# Language Version

Use **C# 14** (the latest language version). The project uses [PolySharp](https://github.com/Sergio0694/PolySharp) to polyfill language features on older targets (e.g., `required`, `init`, `CallerArgumentExpression`, `IsExternalInit`, `CompilerFeatureRequired`, module initializers, etc.).

Use modern language features freely:

- Primary constructors
- Collection expressions (`[1, 2, 3]`)
- `ref struct` interfaces and `allows ref struct` constraints
- Pattern matching (list patterns, property patterns, relational patterns)
- `nameof` on type parameters
- `field` keyword (where available)
- `params ReadOnlySpan<T>` overloads
- Static abstract/virtual interface members

If a language feature requires runtime support that PolySharp does not cover, add a manual polyfill in the `Polyfills/` folder.

---

## Code Style

- **File-scoped namespaces**: Always use `namespace Foo;` rather than `namespace Foo { }`.
- **Nullable reference types**: Always enabled (`<Nullable>enable</Nullable>`). Annotate all public API signatures. Prefer `!` (null-forgiving) only when the compiler cannot prove non-null and you have verified it — never to silence warnings lazily.
- **Implicit usings are enabled**: every library sets `<ImplicitUsings>enable</ImplicitUsings>`, so the common `System.*` namespaces are already in scope — do not re-declare them. Hand-written `global using` directives are reserved for deliberate type aliases that resolve a name collision (see `EngineeredWood.Orc/GlobalUsings.cs`, which aliases `GrowableBuffer` to the Core type and disambiguates `Stream`). Do not add one to save typing.
- **`var`**: Use `var` when the type is obvious from the right-hand side. Use explicit types when clarity is needed.
- **Expression-bodied members**: Prefer for single-expression properties, methods, and operators.
- **Access modifiers**: Always explicit — never rely on defaults.
- **Target-typed object creation expression**: Avoid these.
- **Single-statement blocks**: Single-statement blocks should use braces.

---

## Performance Guidelines

Performance is the **primary design consideration**. When in doubt, favor the faster approach and document the trade-off.

### General Principles

1. **Allocations are the enemy.** Minimize heap allocations, especially on hot paths. Prefer stack allocation, pooling, and value types.
2. **Measure, don't guess.** Use BenchmarkDotNet to validate performance claims. Micro-optimizations without benchmarks are not trustworthy.
3. **Optimize for .NET 10 first**, then provide a reasonable fallback for older targets. A 2× slower fallback on netstandard2.0 is acceptable if the .NET 10 path is optimal.
4. **Inlining matters.** Keep hot-path methods small. Use `[MethodImpl(MethodImplOptions.AggressiveInlining)]` judiciously — only when benchmarking confirms benefit. Do not use it speculatively.
5. **Branch prediction**: Arrange `if`/`else` so the common case is first. Use `[MethodImpl(MethodImplOptions.NoInlining)]` on cold error-throwing helpers to keep the hot path small.
6. **Sealed classes**: Seal classes that are not designed for inheritance. This enables devirtualization.

### Memory and Span

Use the full spectrum of modern memory APIs:

- **`Span<T>` and `ReadOnlySpan<T>`**: Prefer over arrays for short-lived data processing. Use for parsing, slicing, and transformations.
- **`Memory<T>` and `ReadOnlyMemory<T>`**: Use when data must survive across `async` boundaries (where `Span<T>` cannot be used).
- **`stackalloc`**: Use for small, fixed-size buffers on hot paths. Always guard with a size threshold and fall back to `ArrayPool<T>`:

```csharp
const int StackAllocThreshold = 256;
Span<byte> buffer = length <= StackAllocThreshold
    ? stackalloc byte[length]
    : (rentedArray = ArrayPool<byte>.Shared.Rent(length));
```

- **`ArrayPool<T>.Shared`**: Use for temporary arrays. Always return rented arrays in a `finally` block or use a helper that ensures return.
- **Avoid LINQ on hot paths.** LINQ allocates enumerator objects and delegates. Use `for`/`foreach` with spans or arrays instead.
- **Prefer `string.Create`**, `StringBuilder` with pooling, or interpolated string handlers over string concatenation in loops.

### Struct and Value-Type Design

- Prefer `readonly struct` for small, immutable data types.
- Use `ref struct` for types that must stay on the stack (parsers, enumerators over spans).
- Implement `IEquatable<T>` on value types to avoid boxing in comparisons.
- Keep structs small (generally ≤ 16 bytes) to benefit from pass-by-value optimizations; use `in` or `ref` for larger structs.

### String and Text Processing

- Use `ReadOnlySpan<char>` for parsing and searching within strings.
- Prefer `string.Equals(a, b, StringComparison.Ordinal)` over `==` when the comparison type matters.
- Use `SearchValues<char>` (.NET 8+) or `IndexOfAny` with known value sets over character-by-character scanning.
- For multi-target string search, provide a `SearchValues` path for .NET 8+ and a fallback for netstandard2.0.

---

## SIMD and Vectorization

Leverage hardware intrinsics and the `Vector128<T>` / `Vector256<T>` / `Vector512<T>` APIs from `System.Runtime.Intrinsics` when processing bulk data (searching, hashing, transcoding, etc.).

### Guidelines

- **Prefer the cross-platform `Vector128/256/512` abstractions** over ISA-specific classes like `Sse2` or `Avx2`. The cross-platform API auto-selects the best available instruction set.
- **Use `Vector128.IsHardwareAccelerated` / `Vector256.IsHardwareAccelerated`** to guard SIMD paths and fall back to scalar code gracefully.
- **Structure code in three tiers** when processing buffers:

```csharp
// 1. SIMD path (process Vector256-sized chunks)
while (remaining >= Vector256<byte>.Count) { /* ... */ }

// 2. Smaller SIMD or scalar unrolled (process Vector128-sized chunks or 4-at-a-time)
while (remaining >= Vector128<byte>.Count) { /* ... */ }

// 3. Scalar tail (remaining elements)
while (remaining > 0) { /* ... */ }
```

- **Alignment**: When performance-critical, align input pointers to vector boundaries to avoid split cache-line loads.
- On netstandard2.0, where `System.Runtime.Intrinsics` is unavailable, fall back to scalar code. Do not attempt to polyfill SIMD there — it is not worth the complexity.

---

## Native AOT Compatibility

All code should be compatible with Native AOT compilation. This ensures maximum performance for ahead-of-time compiled deployments.

**This is an enforced build gate, not advice.** `src/Directory.Build.props` sets
`IsAotCompatible` **and** `TreatWarningsAsErrors` for the `net10.0` leg of every
library, so any new trim/AOT incompatibility (the IL2xxx / IL3xxx families)
**fails the build** rather than producing a warning. The gate is scoped to
net10.0 because the analyzers do not run on netstandard2.0.

A practical consequence: `EngineeredWood.Iceberg` and the Delta Lake projects use
source-generated `System.Text.Json` with **no reflection fallback**, so any new
serializable type must be registered in the relevant `JsonSerializerContext` or
serialization will fail at runtime.

### Rules

- **No `System.Reflection.Emit`** or runtime code generation.
- **Minimize reflection.** If reflection is required, annotate with `[DynamicallyAccessedMembers]`, `[RequiresUnreferencedCode]`, or `[UnconditionalSuppressMessage]` as appropriate. Prefer source generators over reflection-based patterns.
- **Source generators over reflection**: Use `System.Text.Json` source generators, `LoggerMessage` generators, regex source generators (`[GeneratedRegex]`), and similar.
- **Trim-safe patterns**: Do not rely on unreferenced types being preserved. Test with `<PublishTrimmed>true</PublishTrimmed>` and `<TrimmerSingleWarn>false</TrimmerSingleWarn>` to surface warnings.
- **No `dynamic` keyword** — it depends on runtime code generation.
- On netstandard2.0, AOT is not applicable. Use `#if` guards if a pattern requires different implementation for AOT vs. non-AOT targets:

```csharp
#if NET8_0_OR_GREATER
    [JsonSerializable(typeof(MyDto))]
    internal partial class MyJsonContext : JsonSerializerContext { }
#endif
```

---

## Unsafe Code

Prefer safe code. `unsafe` is acceptable when **all** of the following are true:

1. A benchmark proves meaningful performance improvement.
2. The unsafe region is small and clearly bounded.
3. A code comment explains **why** unsafe is necessary and what invariants must hold.

Common acceptable uses:

- `Unsafe.As<TFrom, TTo>` for reinterpret casts between compatible types.
- `Unsafe.Add` / `Unsafe.ReadUnaligned` for fast buffer processing.
- `MemoryMarshal.GetReference` to pin a span for pointer arithmetic.
- `Unsafe.SkipInit<T>` to avoid redundant zero-initialization.

Always provide a safe fallback via `#if`, or as the default path, for netstandard2.0 when unsafe helpers (like `System.Runtime.CompilerServices.Unsafe`) are only available as a package reference.

---

## Conditional Compilation Reference

Use these standard preprocessor symbols for multi-targeting:

| Symbol | When defined |
|--------|-------------|
| `NET10_0_OR_GREATER` | .NET 10+ |
| `NET8_0_OR_GREATER` | .NET 8+ — **the usual guard**; its `#else` is the netstandard2.0 leg |
| `NET6_0_OR_GREATER` | .NET 6+ — used where a type arrived in 6 (e.g. `Half`, `DateOnly`) |
| `NETSTANDARD2_0` | The portability floor. Prefer guarding on the positive `NET*_OR_GREATER` form instead |
| `NETFRAMEWORK` / `NET472_OR_GREATER` | .NET Framework — reachable only from the test projects and `Parquet.TestTool`, never from library code |

Define custom symbols in the `.csproj` if finer-grained control is needed:

```xml
<PropertyGroup Condition="'$(TargetFramework)' == 'net10.0'">
  <DefineConstants>$(DefineConstants);HAS_SEARCHVALUES;HAS_SIMD</DefineConstants>
</PropertyGroup>
```

---

## Testing and Benchmarking

### Unit Tests

- Tests must run and pass on **all target frameworks**. Use conditional `[Fact]` / `[Theory]` attributes or `#if` guards to skip tests that exercise framework-specific features.
- Test both the optimized and fallback code paths — not just the one that runs on your dev machine.

### Benchmarks

- Use **BenchmarkDotNet** for all performance-sensitive code.
- Benchmark across the library target frameworks (`--runtimes net10.0 net8.0`).
- Include benchmarks for realistic data sizes (not just micro-benchmarks with 10 elements).
- BenchmarkDotNet writes to `BenchmarkDotNet.Artifacts/`, which is **gitignored**. Quote the numbers that matter into the commit message or a `doc/` note rather than relying on the artifacts being committed.
- When proposing a performance change, include before/after benchmark results.

---

## Project Structure Conventions

```
src/
  EngineeredWood.<Area>/
    Internal/           # Internal helpers, copied runtime code
    Polyfills/          # PolySharp-adjacent manual polyfills
    *.cs
test/                   # NOTE: singular, and benchmarks live here too
  EngineeredWood.<Area>.Tests/
  EngineeredWood.<Area>.Benchmarks/
  EngineeredWood.Parquet.Compatibility/
```

The solution file is `engineered-wood.slnx`. File-per-type, with the namespace
matching the folder structure.

---

## Summary of Key Rules

1. **Multi-target**: net10.0, net8.0, netstandard2.0 for libraries. Optimize for .NET 10.
2. **C# 14** with PolySharp. Use modern features freely.
3. **Implicit usings on**; hand-written `global using` only for alias collisions. File-scoped namespaces. Nullable enabled.
4. **Minimize allocations.** Use Span, ArrayPool, stackalloc, value types.
5. **SIMD via Vector128/256/512** on .NET 8+. Scalar fallback for netstandard2.0.
6. **AOT-compatible.** No reflection.emit, no dynamic, prefer source generators.
7. **Unsafe only when justified** by benchmarks and bounded in scope.
8. **Copy .NET runtime code** when needed to bring modern APIs to older targets.
9. **Benchmark everything** with BenchmarkDotNet across all target frameworks.
10. **Seal your classes** unless designed for inheritance.
