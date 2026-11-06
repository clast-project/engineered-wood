// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace DeltaDotnetComparison;

/// <summary>
/// Hooks for counting a benchmark's storage requests (#713) from a logging proxy in front of the store. Off
/// unless <c>EWDD_TRACE_URL</c> names the proxy (such as <c>http://127.0.0.1:10000</c>). Around each method
/// <c>--verify</c> runs, it sets the proxy's label to the method (<c>GET /__mark?label=…</c>) and its added
/// per-request latency to <c>EWDD_TRACE_LATENCY_MS</c> (<c>GET /__latency?ms=…</c>), then clears both, so the
/// fixture builds and the fingerprint reads are neither delayed nor attributed. <c>EWDD_VERIFY_METHODS</c>, a
/// comma-separated list of substrings, runs only the matching methods.
/// </summary>
internal static class TraceMarks
{
    private static readonly string? Url = Environment.GetEnvironmentVariable("EWDD_TRACE_URL")?.TrimEnd('/');
    private static readonly int LatencyMs =
        int.TryParse(Environment.GetEnvironmentVariable("EWDD_TRACE_LATENCY_MS"), out int ms) ? ms : 0;
    private static readonly string[] Methods =
        (Environment.GetEnvironmentVariable("EWDD_VERIFY_METHODS") ?? "")
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    private static readonly HttpClient Client = new();

    public static bool Selected(string method) =>
        Methods.Length == 0 || Methods.Any(m => method.Contains(m, StringComparison.OrdinalIgnoreCase));

    public static async Task BeginAsync(string label)
    {
        if (Url is null)
            return;
        await Client.GetAsync($"{Url}/__latency?ms={LatencyMs}");
        await Client.GetAsync($"{Url}/__mark?label={Uri.EscapeDataString($"{label}@{LatencyMs}ms")}");
    }

    public static async Task EndAsync()
    {
        if (Url is null)
            return;
        await Client.GetAsync($"{Url}/__mark?label=-");
        await Client.GetAsync($"{Url}/__latency?ms=0");
    }
}
