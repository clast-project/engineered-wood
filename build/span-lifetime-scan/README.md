# span-lifetime-scan

A Roslyn scan for the hazard in [doc/arrow-span-lifetime.md](../../doc/arrow-span-lifetime.md): a span read
off an Apache Arrow array after the last use of the array. The array's native memory may then be freed
under the span. Written for #834, which swept the test projects with it.

## What it reports

It compiles one project's sources against that project's built assemblies, signed with the repository's
key so that `InternalsVisibleTo` grants resolve. It finds every `Span<T>`/`ReadOnlySpan<T>` returned by
a member of an `Apache.Arrow` type (`Values`, `GetBytes`, `ValueOffsets`, `ArrowBuffer.Span`, …). It
walks the receiver to its root (a local, a parameter, `this` or a temporary) and reports the site when
nothing mentions the root later in the enclosing method, lambda, local function or accessor. A `using`
local counts as alive, and so does an owner reached through `this` (a field): the instance is the
caller's to keep alive.

Each line gives the site, the owner, and how the span is consumed where it is taken. `indexed` and
`length` mean no call runs between taking the span and reading it, so such sites are left alone.

Its limits:
- **Liveness is lexical.** An owner mentioned later on another path counts as alive.
- **A captured variable is reported.** A lambda's captured local lives in a closure the running delegate
  references, so such reports are false positives.
- **Ownership is not followed into callers.** A helper that reads spans off its parameter is reported even
  when every caller keeps the array alive. In library code the rule is to root at the entry point, so a
  report there is a question about the call tree, not a fix.

## Running it

Build the solution in Release first, so that every project has a `bin/Release/net10.0`. A library's own
`bin` lacks its package dependencies, so scan a `src` project against its test project's output. From the
repository root, in Git Bash:

```bash
dotnet build -c Release
dotnet build build/span-lifetime-scan -c Release
scan=build/span-lifetime-scan/bin/Release/net10.0/span-lifetime-scan.exe
export SPANSCAN_SNK=$PWD/Clast.snk

# One test project:
$scan test/EngineeredWood.Avro.Tests test/EngineeredWood.Avro.Tests/bin/Release/net10.0

# A library, against its tests' output:
$scan src/EngineeredWood.Avro test/EngineeredWood.Avro.Tests/bin/Release/net10.0
```

Sites go to stdout as `path(line): owner kind and name | consumption | source line`. A summary per project
goes to stderr, with the compile-error count. About a dozen errors are normal: native DLLs that can't be
referenced, and linked files outside the project directory. `SPANSCAN_ERRORS=1` lists the most common
ones. A large count means unresolved symbols, and with them, sites missed.

## Fix mode (tests)

`SPANSCAN_FIX=1` rewrites the scanned files. For each reported owner in a method, it inserts
`GC.KeepAlive(owner);` after the statement holding the last read of the span. It follows the span into span
locals (`Cast`, `Slice`) and moves an existing keep-alive that was placed too early. Sites it cannot handle
go to stderr as `MANUAL`: an expression-bodied member, or a read inside a `return` or `throw`. Two steps
remain afterwards:

```bash
# An owner that is a pattern variable (`x is BinaryArray b`) is out of scope after its statement: remove
# the keep-alives the compiler rejects, and fix those sites by hand.
dotnet build -c Release -f net10.0 2>&1 | python build/span-lifetime-scan/prune_uncompilable.py

# Two owners read in one statement can leave a repeated keep-alive: keep each name's last.
python build/span-lifetime-scan/dedupe_runs.py test
```

Then rebuild every target framework, and read the diff. For a shared helper, rooting the array once at its
entry point (as `ArrowValues.Render` does) reads better than a keep-alive in each branch.
