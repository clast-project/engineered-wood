// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using EngineeredWood.IO;
using EngineeredWood.IO.Azure;
using EngineeredWood.IO.Local;

namespace DeltaDotnetComparison;

/// <summary>
/// Where the benchmark tables live. A table is named by a LOCATION string that only this class interprets: a
/// local directory path, or a blob-name prefix within an Azure container. Every benchmark passes locations
/// around; this class turns one into an EW <see cref="ITableFileSystem"/>, into delta-rs's table URI and storage
/// options, and does the fixture housekeeping (copy, delete, ready markers) on the right store.
///
/// <para>Chosen by environment variables, because BenchmarkDotNet runs each benchmark in a child process that
/// inherits the environment but not the command line:</para>
/// <list type="bullet">
/// <item><c>EWDD_STORAGE</c>: <c>local</c> (default) or <c>azure</c>.</item>
/// <item><c>EWDD_AZURE_ACCOUNT</c>, <c>EWDD_AZURE_KEY</c>: the storage account name and its shared key
/// (required for <c>azure</c>, unless <c>EWDD_AZURE_EMULATOR=1</c>, which uses Azurite's well-known
/// account on 127.0.0.1:10000).</item>
/// <item><c>EWDD_AZURE_CONTAINER</c>: the container (default <c>ew-vs-delta-dotnet</c>; created if absent).</item>
/// <item><c>EWDD_AZURE_PREFIX</c>: the blob-name prefix everything goes under (default <c>bench</c>).</item>
/// <item><c>EWDD_AZURE_CONCURRENCY</c>: EW's <see cref="AzureTableFileSystem"/> <c>maxConcurrency</c>
/// (default 16, its own default).</item>
/// </list>
/// </summary>
internal abstract class BenchStorage
{
    public static BenchStorage Current { get; } = Create();

    /// <summary>A one-line description for the run's banner. Never includes the key.</summary>
    public abstract string Description { get; }

    /// <summary>The location everything (templates, scratch) lives under.</summary>
    public abstract string Root { get; }

    public abstract string Combine(string location, string child);

    /// <summary>The last segment of a location.</summary>
    public abstract string NameOf(string location);

    /// <summary>EW's file system rooted at the table.</summary>
    public abstract ITableFileSystem FileSystem(string location);

    /// <summary>The table location as delta-rs / delta-kernel-rs take it.</summary>
    public abstract string DeltaRsUri(string location);

    /// <summary>delta-rs's <c>storage_options</c> (object_store config keys) for <see cref="DeltaRsUri"/>.</summary>
    public abstract Dictionary<string, string> DeltaRsOptions { get; }

    /// <summary>
    /// The content of a small sibling marker (<c>&lt;location&gt;.ready</c>) outside the table, so no vacuum
    /// collects it; null when there is none.
    /// </summary>
    public abstract string? ReadMarker(string location);

    public abstract void WriteMarker(string location, string content);

    /// <summary>Removes every file of the table. Best effort: a failure is left for the next sweep.</summary>
    public abstract void DeleteTree(string location);

    /// <summary>A private copy of every file under <paramref name="source"/>.</summary>
    public abstract void CopyTree(string source, string destination);

    /// <summary>Makes an empty table location ready to be created in.</summary>
    public abstract void CreateEmpty(string location);

    /// <summary>The immediate children of a location (for the scratch sweep).</summary>
    public abstract IEnumerable<string> Children(string location);

    /// <summary>
    /// An exclusive lock on building the template named by <paramref name="location"/>'s marker, held across
    /// the marker re-check, the build and the marker. It covers everyone who shares this store: on local disk,
    /// the processes of this machine; in Azure, every machine using the same container and prefix. Waits until
    /// free. A holder that dies releases it (the OS closes a file; a blob lease expires).
    ///
    /// <para>It saves duplicate builds; correctness does not rest on it. Each build writes a location of its
    /// own, so a holder that lost the lock (a lease that expired while the process stalled) can only finish
    /// and publish a second complete build, never write into someone else's.</para>
    /// </summary>
    public abstract IDisposable AcquireBuildLock(string location, string name);

    /// <summary>Retries <paramref name="tryAcquire"/> until it returns a lock, for up to 30 minutes.</summary>
    protected static IDisposable WaitFor(string name, Func<IDisposable?> tryAcquire)
    {
        var waited = System.Diagnostics.Stopwatch.StartNew();
        bool announced = false;
        while (true)
        {
            if (tryAcquire() is { } acquired)
                return acquired;
            if (waited.Elapsed > TimeSpan.FromMinutes(30))
                throw new TimeoutException($"Fixture {name} was still being built by another run after 30 minutes.");
            if (!announced)
            {
                Console.WriteLine($"// waiting for another run to finish building fixture {name} ...");
                announced = true;
            }
            Thread.Sleep(250);
        }
    }

    private static BenchStorage Create()
    {
        string kind = (Environment.GetEnvironmentVariable("EWDD_STORAGE") ?? "local").Trim();
        if (kind.Equals("local", StringComparison.OrdinalIgnoreCase))
            return new LocalStorage();
        if (kind.Equals("azure", StringComparison.OrdinalIgnoreCase))
            return AzureStorage.FromEnvironment();
        throw new InvalidOperationException($"EWDD_STORAGE must be 'local' or 'azure', not '{kind}'.");
    }
}

internal sealed class LocalStorage : BenchStorage
{
    public override string Root { get; } =
        Environment.GetEnvironmentVariable("EWDD_BENCH_ROOT")
        ?? Path.Combine(Path.GetTempPath(), "ew-vs-delta-dotnet");

    public override string Description => $"local disk, {Root}";

    public override string Combine(string location, string child) => Path.Combine(location, child);

    public override string NameOf(string location) => Path.GetFileName(location);

    public override ITableFileSystem FileSystem(string location) => new LocalTableFileSystem(location);

    public override string DeltaRsUri(string location) => location;

    public override Dictionary<string, string> DeltaRsOptions { get; } = [];

    public override string? ReadMarker(string location) =>
        File.Exists(location + ".ready") ? File.ReadAllText(location + ".ready") : null;

    public override void WriteMarker(string location, string content) => File.WriteAllText(location + ".ready", content);

    public override void DeleteTree(string location)
    {
        if (!Directory.Exists(location))
            return;
        try
        {
            Directory.Delete(location, recursive: true);
        }
        catch (IOException)
        {
            // A native handle released a moment late; the scratch root is swept on the next run anyway.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    public override void CopyTree(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (string dir in Directory.EnumerateDirectories(source, "*", SearchOption.AllDirectories))
            Directory.CreateDirectory(Path.Combine(destination, Path.GetRelativePath(source, dir)));
        foreach (string file in Directory.EnumerateFiles(source, "*", SearchOption.AllDirectories))
        {
            string target = Path.Combine(destination, Path.GetRelativePath(source, file));
            File.Copy(file, target);
            // Keep the source's mtime: vacuum retention and file-age logic key off it.
            File.SetLastWriteTimeUtc(target, File.GetLastWriteTimeUtc(file));
        }
    }

    public override void CreateEmpty(string location) => Directory.CreateDirectory(location);

    public override IEnumerable<string> Children(string location) =>
        Directory.Exists(location) ? Directory.EnumerateDirectories(location) : [];

    /// <summary>A sibling <c>.lock</c> file opened with no sharing.</summary>
    public override IDisposable AcquireBuildLock(string location, string name)
    {
        string lockPath = location + ".lock";
        Directory.CreateDirectory(Path.GetDirectoryName(lockPath)!);
        return WaitFor(name, () =>
        {
            try
            {
                return new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException)
            {
                return null; // held by another process
            }
        });
    }
}

/// <summary>
/// Azure Blob Storage, authorized by the account's shared key. Locations are blob-name prefixes (no trailing
/// slash) within one container.
///
/// <para>delta-rs and delta-kernel-rs reach the same blobs through object_store, as <c>az://container/prefix</c>
/// with the account name and key as storage options. Both talk to the Blob endpoint, so the account does not
/// need a hierarchical namespace, and EW's <see cref="AzureTableFileSystem"/> is the Blob backend too.</para>
/// </summary>
internal sealed class AzureStorage : BenchStorage
{
    // Azurite's documented, public development account.
    private const string EmulatorAccount = "devstoreaccount1";
    private const string EmulatorKey =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private readonly BlobContainerClient _container;
    private readonly string _account;
    private readonly bool _emulator;
    private readonly int _concurrency;
    private readonly string _namespace;

    private AzureStorage(string account, string key, string container, string prefix, bool emulator, int concurrency)
    {
        _account = account;
        _emulator = emulator;
        _concurrency = concurrency;
        Root = prefix;

        var credential = new StorageSharedKeyCredential(account, key);
        var serviceUri = emulator
            ? new Uri($"http://127.0.0.1:10000/{account}")
            : new Uri($"https://{account}.blob.core.windows.net");
        // Azurite refuses the SDK's newest REST version; pin one it accepts.
        var options = emulator
            ? new BlobClientOptions(BlobClientOptions.ServiceVersion.V2024_11_04)
            : new BlobClientOptions();
        var service = new BlobServiceClient(serviceUri, credential, options);
        _container = service.GetBlobContainerClient(container);
        _container.CreateIfNotExists();

        // A hierarchical-namespace (ADLS Gen2) account lists its directories as zero-length blobs, which the
        // housekeeping below must step around; say which kind this is, since it changes what the store does.
        try
        {
            _namespace = service.GetAccountInfo().Value.IsHierarchicalNamespaceEnabled
                ? "hierarchical namespace" : "flat namespace";
        }
        catch (Azure.RequestFailedException)
        {
            _namespace = "namespace unknown";
        }

        DeltaRsOptions = new Dictionary<string, string>
        {
            ["azure_storage_account_name"] = account,
            ["azure_storage_account_key"] = key,
        };
        if (emulator)
            DeltaRsOptions["azure_storage_use_emulator"] = "true";
    }

    public static AzureStorage FromEnvironment()
    {
        bool emulator = Environment.GetEnvironmentVariable("EWDD_AZURE_EMULATOR") is "1" or "true";
        string? account = Env("EWDD_AZURE_ACCOUNT") ?? (emulator ? EmulatorAccount : null);
        string? key = Env("EWDD_AZURE_KEY") ?? (emulator ? EmulatorKey : null);
        if (account is null || key is null)
        {
            throw new InvalidOperationException(
                "EWDD_STORAGE=azure needs EWDD_AZURE_ACCOUNT and EWDD_AZURE_KEY (the storage account name and an "
                + "account key). See the README beside this project, or dot-source azure-env.ps1 to be prompted.");
        }
        string container = Env("EWDD_AZURE_CONTAINER") ?? "ew-vs-delta-dotnet";
        string prefix = (Env("EWDD_AZURE_PREFIX") ?? "bench").Trim('/');
        if (prefix.Length == 0)
        {
            // Every location is "<prefix>/...", and the housekeeping lists blobs by that literal prefix; an empty
            // one would make it look under "/templates/..." while the engines write "templates/...".
            throw new InvalidOperationException(
                "EWDD_AZURE_PREFIX must name a blob-name prefix (such as 'bench'); the container root is not supported.");
        }
        int concurrency = int.TryParse(Env("EWDD_AZURE_CONCURRENCY"), out int c) && c > 0 ? c : 16;
        return new AzureStorage(account, key, container, prefix, emulator, concurrency);
    }

    private static string? Env(string name) =>
        Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value.Trim() : null;

    public override string Root { get; }

    public override string Description =>
        $"Azure Blob{(_emulator ? " (Azurite emulator)" : "")}, account {_account} ({_namespace}), "
        + $"container {_container.Name}, prefix {Root}/, EW maxConcurrency {_concurrency}";

    public override string Combine(string location, string child) => location + "/" + child;

    public override string NameOf(string location) => location[(location.LastIndexOf('/') + 1)..];

    public override ITableFileSystem FileSystem(string location) =>
        new AzureTableFileSystem(_container, location, maxConcurrency: _concurrency);

    public override string DeltaRsUri(string location) => $"az://{_container.Name}/{location}";

    public override Dictionary<string, string> DeltaRsOptions { get; }

    public override string? ReadMarker(string location)
    {
        try
        {
            return _container.GetBlobClient(location + ".ready").DownloadContent().Value.Content.ToString();
        }
        catch (Azure.RequestFailedException e) when (e.Status == 404)
        {
            return null;
        }
    }

    public override void WriteMarker(string location, string content) =>
        _container.GetBlobClient(location + ".ready").Upload(BinaryData.FromString(content), overwrite: true);

    /// <summary>
    /// The files under a location, and separately the directories: on a hierarchical-namespace account a flat
    /// listing returns each directory as a zero-length blob marked <c>hdi_isfolder</c>, which can be neither
    /// copied nor deleted while it has children.
    /// </summary>
    private (List<string> Files, List<string> Directories) BlobsUnder(string location)
    {
        var files = new List<string>();
        var directories = new List<string>();
        foreach (var blob in _container.GetBlobs(BlobTraits.Metadata, BlobStates.None, location + "/", default))
        {
            bool isDirectory = blob.Metadata is { } metadata
                && metadata.TryGetValue("hdi_isfolder", out string? flag)
                && string.Equals(flag, "true", StringComparison.OrdinalIgnoreCase);
            (isDirectory ? directories : files).Add(blob.Name);
        }
        return (files, directories);
    }

    public override void DeleteTree(string location)
    {
        try
        {
            var (files, directories) = BlobsUnder(location);
            Parallel.ForEach(files, new ParallelOptions { MaxDegreeOfParallelism = 32 },
                name => _container.GetBlobClient(name).DeleteIfExists());
            // Deepest first: a directory can only go once it is empty.
            foreach (string directory in directories.OrderByDescending(d => d.Length))
                _container.GetBlobClient(directory).DeleteIfExists();
            _container.GetBlobClient(location).DeleteIfExists(); // the table's own directory, if it has one
        }
        catch (Exception e) when (e is AggregateException or Azure.RequestFailedException)
        {
            // Best effort, as on local disk: the scratch sweep retries.
        }
    }

    /// <summary>
    /// Server-side copy, blob by blob. Within one account the request's shared key authorizes the source, and
    /// the copy completes synchronously. Unlike a local copy, the copies' last-modified times are NOW; the one
    /// fixture whose vacuum keys off file age (<see cref="Fixtures.Compacted"/>) uses a zero retention, so a
    /// fresher copy changes nothing.
    /// </summary>
    public override void CopyTree(string source, string destination)
    {
        // Directories are not copied: writing a blob creates its parent directories on a hierarchical account,
        // and a flat one has none.
        Parallel.ForEach(BlobsUnder(source).Files, new ParallelOptions { MaxDegreeOfParallelism = 32 }, name =>
        {
            var from = _container.GetBlobClient(name);
            var to = _container.GetBlobClient(destination + name[source.Length..]);
            try
            {
                to.StartCopyFromUri(from.Uri).WaitForCompletion();
            }
            catch (Azure.RequestFailedException e)
            {
                throw new InvalidOperationException(
                    $"Copy Blob failed for '{name}' -> '{to.Name}' ({e.Status} {e.ErrorCode}).", e);
            }
        });
    }

    public override void CreateEmpty(string location)
    {
        // Blob storage has no directories; an empty prefix is already an empty table location.
    }

    public override IEnumerable<string> Children(string location) =>
        _container.GetBlobsByHierarchy(BlobTraits.None, BlobStates.None, "/", location + "/", default)
            .Where(item => item.IsPrefix)
            .Select(item => item.Prefix.TrimEnd('/'))
            .ToList();

    /// <summary>
    /// A lease on a sibling <c>&lt;location&gt;.lock</c> blob, so runs on DIFFERENT machines sharing the container
    /// and prefix serialize too (a local file would only cover this one). The lease is taken for
    /// <see cref="LeaseDuration"/> and renewed in the background while the build runs; a holder that dies stops
    /// renewing, and the lease expires on its own.
    /// </summary>
    public override IDisposable AcquireBuildLock(string location, string name)
    {
        var blob = _container.GetBlobClient(location + ".lock");
        try
        {
            blob.Upload(BinaryData.FromString(""), overwrite: false);
        }
        catch (Azure.RequestFailedException e) when (e.Status is 409 or 412)
        {
            // Already there (409), or there and leased by its current holder (412).
        }
        var lease = blob.GetBlobLeaseClient();
        return WaitFor(name, () =>
        {
            try
            {
                lease.Acquire(LeaseDuration);
                return new LeaseLock(lease);
            }
            catch (Azure.RequestFailedException e) when (e.Status == 409)
            {
                return null; // LeaseAlreadyPresent: another run is building it
            }
        });
    }

    private static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(60);

    private sealed class LeaseLock : IDisposable
    {
        private readonly BlobLeaseClient _lease;
        private readonly Timer _renewal;

        public LeaseLock(BlobLeaseClient lease)
        {
            _lease = lease;
            var period = LeaseDuration / 3;
            _renewal = new Timer(_ => Renew(), null, period, period);
        }

        private void Renew()
        {
            try
            {
                _lease.Renew();
            }
            catch (Exception e) when (e is Azure.RequestFailedException or AggregateException)
            {
                // A lost lease costs at most a duplicate build (see AcquireBuildLock), so carry on.
            }
        }

        public void Dispose()
        {
            using (var stopped = new ManualResetEvent(false))
            {
                _renewal.Dispose(stopped); // wait out a renewal in flight, so it cannot follow the release
                stopped.WaitOne();
            }
            try
            {
                _lease.Release();
            }
            catch (Azure.RequestFailedException)
            {
                // Expired or taken over already; nothing of ours to release.
            }
        }
    }
}
