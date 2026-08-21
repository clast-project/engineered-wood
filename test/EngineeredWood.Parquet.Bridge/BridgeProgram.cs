// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Reflection;
using Apache.Arrow.Ipc;
using EngineeredWood.IO.Local;

namespace EngineeredWood.Parquet.Bridge;

/// <summary>
/// A <c>parquity.bridge.v1</c> implementation over EngineeredWood's Parquet reader and writer.
/// </summary>
/// <remarks>
/// Exit codes are the contract, not a convenience: 0 succeeded, 1 is a failure of the
/// implementation under test (which Parquity records as evidence), and 2 is a request this bridge
/// could not understand (which must stop the run instead of being filed as a Parquet defect).
/// </remarks>
public static class BridgeProgram
{
    /// <summary>The bridge protocol this executable speaks.</summary>
    public const string Protocol = "parquity.bridge.v1";

    /// <summary>The engine name Parquity must be configured to use for this bridge.</summary>
    public const string EngineName = "engineeredwood";

    private const int Success = 0;
    private const int ProviderFailure = 1;
    private const int RequestRejected = 2;

    /// <summary>
    /// Read options for a reader whose output crosses into the Arrow ecosystem.
    /// </summary>
    /// <remarks>
    /// EngineeredWood defaults to the narrowest Arrow decimal that fits the physical width, so a
    /// <c>decimal(6,2)</c> reads back as <c>decimal32</c>. That is a deliberate EngineeredWood
    /// choice, but every other Parquet-to-Arrow implementation produces <c>decimal128</c>, and the
    /// consumer on the other side of this bridge expects the ecosystem convention.
    /// <see cref="DecimalOutputKind.Decimal128"/> exists for exactly this case.
    /// </remarks>
    private static readonly ParquetReadOptions ReadOptions = ParquetReadOptions.Default with
    {
        DecimalOutput = DecimalOutputKind.Decimal128,
    };

    /// <summary>Runs one bridge operation and returns its exit code.</summary>
    /// <param name="args">The operation and its arguments, as Parquity supplies them.</param>
    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Length == 0)
            return Reject("UsageError", "no operation was supplied");

        try
        {
            return args[0] switch
            {
                "info" => Info(),
                "read" => await ReadAsync(args).ConfigureAwait(false),
                "write" => await WriteAsync(args).ConfigureAwait(false),
                _ => Reject("UsageError", $"unknown operation: {args[0]}"),
            };
        }
        catch (BridgeRequestException error)
        {
            return Reject("UsageError", error.Message);
        }
        catch (Exception error)
        {
            // Everything the implementation under test can throw is evidence, reported under the
            // exception's own type name so that Parquity groups findings by real failure mode.
            return Fail(error.GetType().Name, Flatten(error));
        }
    }

    private static int Info()
    {
        var profiles = string.Join(
            ",",
            WriterProfiles.Supported.Select(
                entry => $"{Json.String(entry.Key)}:{Json.Object(entry.Value)}"));

        Console.Out.Write(
            "{" +
            $"{Json.String("protocol")}:{Json.String(Protocol)}," +
            $"{Json.String("engine")}:{Json.String(EngineName)}," +
            $"{Json.String("version")}:{Json.String(Version())}," +
            $"{Json.String("directions")}:[{Json.String("read")},{Json.String("write")}]," +
            $"{Json.String("writer_profiles")}:{{{profiles}}}" +
            "}");
        return Success;
    }

    private static async Task<int> ReadAsync(string[] args)
    {
        string source = Required(args, "--parquet");
        string target = Required(args, "--arrow");
        Reject(args, "read", "--parquet", "--arrow");

        await using var file = new LocalRandomAccessFile(source);
        await using var reader = new ParquetFileReader(file, ownsFile: false, ReadOptions);

        // Taken before any batch, because a file with no row groups yields no batches at all and
        // its schema would otherwise be unobservable.
        var schema = await reader.GetArrowSchemaAsync().ConfigureAwait(false);

        await using (var output = File.Create(target))
        {
            using var writer = new ArrowFileWriter(output, schema);
            await writer.WriteStartAsync().ConfigureAwait(false);
            await foreach (var batch in reader.ReadAllAsync().ConfigureAwait(false))
            {
                using (batch)
                    await writer.WriteRecordBatchAsync(batch).ConfigureAwait(false);
            }

            await writer.WriteEndAsync().ConfigureAwait(false);
        }

        return Ok();
    }

    private static async Task<int> WriteAsync(string[] args)
    {
        string source = Required(args, "--arrow");
        string target = Required(args, "--parquet");
        string? profile = Optional(args, "--profile");
        Reject(args, "write", "--arrow", "--parquet", "--profile");

        if (profile is not null && !WriterProfiles.Supported.ContainsKey(profile))
            return Reject("UsageError", $"undeclared writer profile: {profile}");

        var options = WriterProfiles.Apply(profile);

        await using var input = File.OpenRead(source);
        using var ipc = new ArrowFileReader(input);

        // Read the first batch before asking for the schema: the reader only parses the schema
        // message on demand, and a zero-row table has no batch to carry it.
        var first = await ipc.ReadNextRecordBatchAsync().ConfigureAwait(false);
        var schema = ipc.Schema ?? first?.Schema
            ?? throw new BridgeRequestException("the Arrow IPC input declares no schema");

        await using (var file = new LocalSequentialFile(target))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, options))
        {
            // A zero-row table produces no row group, so the schema has to be declared or the
            // footer would describe a file with no columns.
            writer.DeclareSchema(schema);

            for (var batch = first;
                 batch is not null;
                 batch = await ipc.ReadNextRecordBatchAsync().ConfigureAwait(false))
            {
                using (batch)
                    await writer.WriteRowGroupAsync(batch).ConfigureAwait(false);
            }

            await writer.CloseAsync().ConfigureAwait(false);
        }

        return Ok();
    }

    private static string Version()
    {
        var assembly = typeof(ParquetFileReader).Assembly;
        return assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
            ?.InformationalVersion
            ?? assembly.GetName().Version?.ToString()
            ?? "0.0.0";
    }

    private static string Required(string[] args, string flag) =>
        Optional(args, flag)
        ?? throw new BridgeRequestException($"{args[0]} requires {flag}");

    private static string? Optional(string[] args, string flag)
    {
        int index = Array.IndexOf(args, flag);
        if (index < 0)
            return null;
        if (index + 1 >= args.Length)
            throw new BridgeRequestException($"{flag} requires a value");
        return args[index + 1];
    }

    private static void Reject(string[] args, string operation, params string[] known)
    {
        for (int i = 1; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
                continue;
            if (Array.IndexOf(known, args[i]) < 0)
                throw new BridgeRequestException($"{operation} does not accept {args[i]}");
            i++;
        }
    }

    private static int Ok()
    {
        Console.Out.Write($"{{{Json.String("status")}:{Json.String("OK")}}}");
        return Success;
    }

    private static int Fail(string kind, string detail) => Report(kind, detail, ProviderFailure);

    private static int Reject(string kind, string detail) => Report(kind, detail, RequestRejected);

    private static int Report(string kind, string detail, int code)
    {
        Console.Out.Write(
            "{" +
            $"{Json.String("status")}:{Json.String("ERROR")}," +
            $"{Json.String("kind")}:{Json.String(Sanitize(kind))}," +
            $"{Json.String("detail")}:{Json.String(detail)}" +
            "}");
        return code;
    }

    /// <summary>
    /// Parquity requires the reported kind to be a bounded identifier, and uses it in finding
    /// identity. A generic type name such as <c>ArgumentException`1</c> would be rejected whole.
    /// </summary>
    private static string Sanitize(string kind)
    {
        var cleaned = new string(kind.Where(
            character => char.IsAsciiLetterOrDigit(character) || character is '_' or '.').ToArray());
        if (cleaned.Length == 0 || char.IsAsciiDigit(cleaned[0]))
            cleaned = "BridgeError";
        return cleaned.Length <= 64 ? cleaned : cleaned[..64];
    }

    private static string Flatten(Exception error)
    {
        var messages = new List<string>();
        for (Exception? current = error; current is not null; current = current.InnerException)
            messages.Add($"{current.GetType().Name}: {current.Message}");
        return string.Join(" -> ", messages);
    }
}

/// <summary>A request this bridge could not understand. Reported as exit 2, never as evidence.</summary>
public sealed class BridgeRequestException(string message) : Exception(message);
