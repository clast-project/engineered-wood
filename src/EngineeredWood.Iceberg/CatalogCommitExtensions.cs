// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;

namespace EngineeredWood.Iceberg;

/// <summary>
/// How <see cref="CatalogCommitExtensions.CommitWithRetryAsync"/> retries a commit that lost a race. The
/// defaults, and the table properties <see cref="FromProperties"/> reads, are Iceberg Java's.
/// </summary>
/// <param name="NumRetries">Retries after the first attempt (<c>commit.retry.num-retries</c>, default 4).</param>
/// <param name="MinWait">The first retry's wait (<c>commit.retry.min-wait-ms</c>, default 100 ms).</param>
/// <param name="MaxWait">The longest wait between attempts (<c>commit.retry.max-wait-ms</c>, default 60 s).</param>
/// <param name="TotalTimeout">No retry starts after this long (<c>commit.retry.total-timeout-ms</c>, default 30 min).</param>
public sealed record CommitRetryOptions(
    int NumRetries,
    TimeSpan MinWait,
    TimeSpan MaxWait,
    TimeSpan TotalTimeout)
{
    /// <summary>Iceberg Java's defaults.</summary>
    public static CommitRetryOptions Default { get; } = new(
        4, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(60), TimeSpan.FromMinutes(30));

    /// <summary>
    /// The options a table's <c>commit.retry.*</c> properties give, with <see cref="Default"/>'s for any
    /// property that is missing or not a non-negative integer.
    /// </summary>
    public static CommitRetryOptions FromProperties(IReadOnlyDictionary<string, string> properties)
    {
        long? Read(string key) =>
            properties.TryGetValue(key, out var text) &&
            long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var value)
                ? value
                : null;

        var d = Default;
        return new CommitRetryOptions(
            Read("commit.retry.num-retries") is { } n ? (int)Math.Min(n, int.MaxValue) : d.NumRetries,
            Read("commit.retry.min-wait-ms") is { } min ? TimeSpan.FromMilliseconds(min) : d.MinWait,
            Read("commit.retry.max-wait-ms") is { } max ? TimeSpan.FromMilliseconds(max) : d.MaxWait,
            Read("commit.retry.total-timeout-ms") is { } total ? TimeSpan.FromMilliseconds(total) : d.TotalTimeout);
    }
}

/// <summary>Optimistic-concurrency commits with retry, over any <see cref="ICatalog"/>.</summary>
public static class CatalogCommitExtensions
{
    /// <summary>
    /// <para>Loads the table, applies <paramref name="update"/> to its metadata and commits the result. When
    /// another writer commits first (<see cref="CommitFailedException"/>), waits, reloads and applies
    /// <paramref name="update"/> again to the newer metadata, up to the retry limit.</para>
    /// <para><paramref name="update"/> must therefore derive everything from the metadata it is given (as
    /// <see cref="Manifest.TableOperations"/>'s methods do: parent snapshot, sequence number and carried-forward
    /// manifests all come from it), and may run more than once. It should throw
    /// <see cref="CommitValidationException"/> when the change no longer applies, which is not retried.
    /// Files an abandoned attempt wrote are left in place, for orphan-file cleanup.</para>
    /// </summary>
    /// <param name="catalog">The catalog holding the table.</param>
    /// <param name="identifier">The table.</param>
    /// <param name="update">Computes the new metadata from the current metadata.</param>
    /// <param name="options">Retry limits; by default, the table's <c>commit.retry.*</c> properties.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The committed table.</returns>
    /// <exception cref="CommitFailedException">Every attempt lost a race, or the time limit passed.</exception>
    public static async Task<Table> CommitWithRetryAsync(
        this ICatalog catalog,
        TableIdentifier identifier,
        Func<TableMetadata, CancellationToken, ValueTask<TableMetadata>> update,
        CommitRetryOptions? options = null,
        CancellationToken ct = default)
    {
        if (catalog is null) throw new ArgumentNullException(nameof(catalog));
        if (update is null) throw new ArgumentNullException(nameof(update));

        var started = DateTimeOffset.UtcNow;
        var random = new Random();
        for (var attempt = 0; ; attempt++)
        {
            var table = await catalog.LoadTableAsync(identifier, ct).ConfigureAwait(false);
            options ??= CommitRetryOptions.FromProperties(table.Metadata.Properties);

            var newMetadata = await update(table.Metadata, ct).ConfigureAwait(false);
            try
            {
                return await catalog.CommitTableAsync(table, newMetadata, ct).ConfigureAwait(false);
            }
            catch (CommitFailedException) when (
                attempt < options.NumRetries && DateTimeOffset.UtcNow - started < options.TotalTimeout)
            {
                // Exponential backoff with +-10% jitter, as Iceberg Java waits.
                var scaled = options.MinWait.TotalMilliseconds * Math.Pow(2, attempt);
                var wait = Math.Min(scaled, options.MaxWait.TotalMilliseconds) * (0.9 + 0.2 * random.NextDouble());
                await Task.Delay(TimeSpan.FromMilliseconds(wait), ct).ConfigureAwait(false);
            }
        }
    }
}
