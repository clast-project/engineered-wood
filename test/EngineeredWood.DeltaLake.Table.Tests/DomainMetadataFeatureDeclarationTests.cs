// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A commit that writes a <c>domainMetadata</c> action declares the <c>domainMetadata</c> writer feature.
///
/// <para>The spec requires it, and the reference implementation enforces it on ITS commits:
/// <c>validateDomainMetadataSupportedAndNoDuplicate</c> refuses any commit whose actions carry a domain
/// the protocol being committed does not support. What no engine currently does is reject the resulting
/// TABLE — #224 said Delta refuses to write any domain to one, and that did not reproduce; see
/// <c>Interop.DomainMetadataInteropTests</c> for what was measured. So these are conformance assertions,
/// which is the honest reason to hold them: the library writes what the format defines, and does not rely
/// on every reader in the ecosystem being lax in the same direction.</para>
///
/// <para>The declaration has to ride the SAME commit as the action it authorises. A protocol upgrade in a
/// separate commit leaves a window where the log holds an undeclared domain, which is the malformed state
/// this is about — briefly, but a concurrent reader cannot tell "briefly" from "permanently".</para>
/// </summary>
public class DomainMetadataFeatureDeclarationTests : IDisposable
{
    private readonly string _tempDir;

    public DomainMetadataFeatureDeclarationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dmfeat_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static Apache.Arrow.Schema IdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch IdBatch(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    private async Task<IReadOnlyList<DeltaAction>> CommitAsync(long version) =>
        await new TransactionLog(Fs).ReadCommitAsync(version);

    /// <summary>
    /// Asserts that <paramref name="version"/> both writes a domain and declares the feature that allows
    /// it — in that one commit.
    /// </summary>
    private async Task AssertDeclaredWithTheDomainAsync(long version)
    {
        var actions = await CommitAsync(version);

        Assert.Contains(actions, a => a is DomainMetadata);

        var protocol = Assert.Single(actions.OfType<ProtocolAction>());
        Assert.Equal(7, protocol.MinWriterVersion);
        Assert.Contains("domainMetadata", protocol.WriterFeatures!);
    }

    /// <summary>
    /// The table this produces is legal for a foreign writer to write a domain to. Before the fix it was
    /// a <c>(1, 2)</c> protocol with no writer features at all — which cannot declare one even in
    /// principle, since table features need writer 7.
    /// </summary>
    [Fact]
    public async Task SetDomainMetadata_DeclaresTheFeatureInTheSameCommit()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        Assert.Equal(2, table.CurrentSnapshot.Protocol.MinWriterVersion); // a legacy table, before

        long version = await table.SetDomainMetadataAsync("acme.retention", """{"days":30}""");

        await AssertDeclaredWithTheDomainAsync(version);
        Assert.Equal(7, table.CurrentSnapshot.Protocol.MinWriterVersion);
        Assert.Equal("""{"days":30}""", table.GetDomainMetadata("acme.retention"));
    }

    /// <summary>
    /// The upgrade is written once. A protocol action in every domain commit would be noise in the log
    /// and a needless conflict surface — a concurrent commit aborts on a protocol change.
    /// </summary>
    [Fact]
    public async Task SetDomainMetadata_OnATableThatAlreadyDeclaresIt_WritesNoProtocolAction()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);
        await table.SetDomainMetadataAsync("acme.retention", """{"days":30}""");

        long second = await table.SetDomainMetadataAsync("acme.retention", """{"days":60}""");

        Assert.Empty((await CommitAsync(second)).OfType<ProtocolAction>());
    }

    /// <summary>
    /// A tombstone is a <c>domainMetadata</c> action like any other, and Delta's validation does not
    /// exempt it — so removing a domain from a table that never declared the feature has to declare it on
    /// the way out. Odd-looking, and the alternative is writing the very action this fix exists to stop
    /// writing.
    /// </summary>
    /// <remarks>
    /// Only reachable on a table some other writer left in that state — after the fix above, EW cannot
    /// create one. The log layer stands in for that writer here.
    /// </remarks>
    [Fact]
    public async Task RemoveDomainMetadata_DeclaresTheFeature_OnATableThatNeverDid()
    {
        await WriteUndeclaredDomainTableAsync();

        await using var table = await DeltaTable.OpenAsync(Fs);
        Assert.Equal(2, table.CurrentSnapshot.Protocol.MinWriterVersion);

        long version = await table.RemoveDomainMetadataAsync("acme.retention");

        await AssertDeclaredWithTheDomainAsync(version);
        Assert.Null(table.GetDomainMetadata("acme.retention"));
    }

    /// <summary>
    /// REPLACE tombstones every domain the previous table carried that the new one does not, so a replace
    /// writes <c>domainMetadata</c> actions of its own — and the replacement protocol is built from the
    /// new table's options, which know nothing about the domains being retired.
    /// </summary>
    [Fact]
    public async Task CreateOrReplace_TombstoningACarriedDomain_DeclaresTheFeature()
    {
        await using (var table = await DeltaTable.CreateAsync(Fs, IdSchema))
        {
            await table.SetDomainMetadataAsync("acme.retention", """{"days":30}""");
        }

        await using var replaced = await DeltaTable.CreateOrReplaceAsync(Fs, IdSchema, [IdBatch(1, 2)]);

        await AssertDeclaredWithTheDomainAsync(replaced.CurrentSnapshot.Version);
        Assert.Empty(replaced.GetDomainMetadata());
    }

    /// <summary>
    /// A host staging a <c>domainMetadata</c> of its own through the escape hatch gets the declaration
    /// too. It could not reasonably supply one itself — it would have to stage a protocol action as well
    /// and get the legacy-feature enumeration right — and the transaction already augments staged actions
    /// with the row-tracking high-water mark and the <c>txn</c> actions, so this is where it belongs.
    /// </summary>
    [Fact]
    public async Task AHostStagedDomain_GetsTheDeclarationToo()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var txn = table.StartTransaction();
        txn.StageActions(
        [
            new DomainMetadata
            {
                Domain = "host.own", Configuration = """{"v":1}""", Removed = false,
            },
        ]);
        long version = await txn.CommitAsync();

        await AssertDeclaredWithTheDomainAsync(version);
        Assert.Equal("""{"v":1}""", table.GetDomainMetadata("host.own"));
    }

    /// <summary>
    /// ...but a host that staged its OWN protocol action is refused rather than rewritten. Not rewriting
    /// a host's deliberate statement about the protocol is one thing; letting it through unchecked would
    /// leave the escape hatch able to produce the exact malformed commit this fix exists to stop.
    /// </summary>
    /// <remarks>
    /// The error carries delta-spark's own code and message shape for this condition, since it IS the
    /// condition delta-spark raises it for — see <see cref="DeltaErrorCodes.DomainMetadataNotSupported"/>.
    /// </remarks>
    [Fact]
    public async Task AHostStagedProtocolThatDoesNotDeclareIt_IsRefusedRatherThanRewritten()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var txn = table.StartTransaction();
        txn.StageActions(
        [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new DomainMetadata
            {
                Domain = "host.own", Configuration = """{"v":1}""", Removed = false,
            },
        ]);

        var refused = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await txn.CommitAsync());

        Assert.Equal(DeltaErrorCodes.DomainMetadataNotSupported, refused.ErrorCode);
        Assert.Contains("[host.own]", refused.Message, StringComparison.Ordinal);
        Assert.Contains("domainMetadata", refused.Message, StringComparison.Ordinal);
    }

    /// <summary>And a host that declared it properly is left entirely alone.</summary>
    [Fact]
    public async Task AHostStagedProtocolThatDoesDeclareIt_CommitsUntouched()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, IdSchema);

        var txn = table.StartTransaction();
        txn.StageActions(
        [
            new ProtocolAction
            {
                MinReaderVersion = 1,
                MinWriterVersion = 7,
                WriterFeatures = ["appendOnly", "invariants", "domainMetadata"],
            },
            new DomainMetadata
            {
                Domain = "host.own", Configuration = """{"v":1}""", Removed = false,
            },
        ]);
        long version = await txn.CommitAsync();

        // Exactly the protocol the host staged — one protocol action, and its own.
        var protocol = Assert.Single((await CommitAsync(version)).OfType<ProtocolAction>());
        Assert.Equal(1, protocol.MinReaderVersion);
        Assert.Equal(["appendOnly", "invariants", "domainMetadata"], protocol.WriterFeatures);
    }

    /// <summary>
    /// The cost of the declaration, stated rather than discovered: the FIRST domain written to a table is a
    /// protocol change as well as a domain write, so two writers racing to it no longer both land — the
    /// loser aborts with <see cref="DeltaErrorCodes.ProtocolChanged"/> even though their domains contest
    /// nothing.
    /// </summary>
    /// <remarks>
    /// <para>This is one-time and self-healing: the abort is a retryable answer, the retry succeeds, and
    /// every later domain commit on the table carries no protocol action at all and rebases as it always
    /// did (<c>MetadataCommitConcurrencyTests.SetDomainMetadata_AgainstADifferentDomain_BothLand</c>).</para>
    ///
    /// <para>It is also what delta-spark does — any concurrent protocol change is a
    /// <c>ProtocolChangedException</c> there too. The alternative would be exempting our own upgrade from
    /// the conflict checker's protocol rule, which is the strongest guard it has and not one to weaken so
    /// that a once-per-table race can avoid a retry.</para>
    /// </remarks>
    [Fact]
    public async Task TheFirstDomainWritten_IsAProtocolChange_SoARacingWriterRetries()
    {
        await using (var setup = await DeltaTable.CreateAsync(Fs, IdSchema))
            await setup.WriteAsync([IdBatch(1)]);

        await using var stale = await DeltaTable.OpenAsync(Fs);

        await using (var other = await DeltaTable.OpenAsync(Fs))
            await other.SetDomainMetadataAsync("acme.lineage", """{"v":1}""");

        var conflict = await Assert.ThrowsAsync<DeltaConflictException>(
            async () => await stale.SetDomainMetadataAsync("acme.retention", """{"days":30}"""));
        Assert.Equal(DeltaErrorCodes.ProtocolChanged, conflict.ErrorCode);

        // Retryable, and the retry needs no protocol action of its own — the winner already declared it.
        await stale.RefreshAsync();
        long retried = await stale.SetDomainMetadataAsync("acme.retention", """{"days":30}""");

        Assert.Empty((await CommitAsync(retried)).OfType<ProtocolAction>());
        await using var reopened = await DeltaTable.OpenAsync(Fs);
        Assert.Equal("""{"v":1}""", reopened.GetDomainMetadata("acme.lineage"));
        Assert.Equal("""{"days":30}""", reopened.GetDomainMetadata("acme.retention"));
    }

    /// <summary>
    /// A table carrying a domain on a protocol that does not declare it — what a lax writer produces, and
    /// what this library produced before the fix.
    /// </summary>
    private async Task WriteUndeclaredDomainTableAsync()
    {
        var log = new TransactionLog(Fs);

        await log.WriteCommitAsync(0,
        [
            new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 },
            new MetadataAction
            {
                Id = "undeclared-domain-table",
                Format = Format.Parquet,
                SchemaString =
                    """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}""",
                PartitionColumns = [],
            },
        ]);

        await log.WriteCommitAsync(1,
        [
            new DomainMetadata
            {
                Domain = "acme.retention",
                Configuration = """{"days":30}""",
                Removed = false,
            },
        ]);
    }
}
