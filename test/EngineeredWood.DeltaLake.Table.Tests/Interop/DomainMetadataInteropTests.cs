// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests.Interop;

/// <summary>
/// <para><b>Tier 3.</b> A table EW wrote a metadata domain to is one the reference implementation can
/// still write a domain of its OWN to. See <see cref="Spark"/> for setup and cost.</para>
///
/// <para><b>What this does NOT show.</b> #224 reported that Delta refuses to write a domain to a table
/// carrying an UNDECLARED one (<c>DELTA_DOMAIN_METADATA_NOT_SUPPORTED</c>). That refusal did not
/// reproduce here against delta-spark 4.0.0 — measured across nine operations (CLUSTER BY, enabling row
/// tracking, enabling deletion vectors, INSERT, RESTORE, three REPLACE spellings and SHALLOW CLONE), all
/// of which succeeded, most by silently upgrading the protocol themselves. delta-rs 1.6.2 reads and
/// appends to such a table without complaint too. So this test is a round trip, not a
/// before-and-after: it would catch EW writing a domain in a shape Spark chokes on, and it does not
/// pretend to demonstrate the refusal.</para>
///
/// <para>The bytecode says why nothing refused:
/// <c>DomainMetadataUtilsBase.validateDomainMetadataSupportedAndNoDuplicate</c> throws only when the
/// COMMIT'S OWN actions carry a domain and the protocol BEING COMMITTED does not support the feature —
/// and every Spark path above either upgrades the protocol in that same commit or writes a fresh one.
/// The commit it would reject is the one EW used to make: a domain action with the protocol left alone.
/// Spark exposes no API for an arbitrary user domain, so it cannot be made to make that commit, which is
/// why this cannot be measured from the outside.</para>
/// </summary>
[Collection("Interop")]
public class DomainMetadataInteropTests : IDisposable
{
    private readonly string _tempDir;

    public DomainMetadataInteropTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_dm_interop_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch IdBatch(params long[] ids) =>
        new(IdSchema, [new Int64Array.Builder().AppendRange(ids).Build()], ids.Length);

    /// <summary>
    /// EW writes a user domain; Spark then enables clustering, which is domain-backed, so its commit
    /// writes <c>delta.clustering</c> into a table that already carries a domain of EW's. Both survive,
    /// and the feature EW declared is still declared afterwards.
    /// </summary>
    /// <remarks>
    /// The last assertion is the one with something to catch: Spark REWRITES the protocol to add
    /// <c>clustering</c>, and a protocol rewrite that dropped <c>domainMetadata</c> would leave the table
    /// in exactly the malformed state #224 is about — this time authored by Spark, over EW's domain.
    /// </remarks>
    [SkippableFact]
    public async Task EwWrittenDomain_SparkCanStillWriteADomainOfItsOwn()
    {
        Spark.Require();

        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, IdSchema))
        {
            await table.WriteAsync([IdBatch(1, 2, 3)]);
            await table.SetDomainMetadataAsync("acme.retention", """{"days":30}""");
        }

        Spark.Invoke("sql", new
        {
            path = _tempDir,
            sql = new[] { "ALTER TABLE delta.`{path}` CLUSTER BY (id)" },
        });

        await using var reopened = await DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

        Assert.NotNull(reopened.GetDomainMetadata("delta.clustering"));
        Assert.Equal("""{"days":30}""", reopened.GetDomainMetadata("acme.retention"));
        Assert.Contains("domainMetadata", reopened.CurrentSnapshot.Protocol.WriterFeatures!);
    }
}
