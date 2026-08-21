// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Bridge;

namespace EngineeredWood.Tests.Parquet.Bridge;

/// <summary>
/// The parts of <c>parquity.bridge.v1</c> a caller relies on: what <c>info</c> promises, and which
/// exit code each outcome carries. The split between 1 and 2 is the whole point of the contract —
/// exit 1 says this implementation failed, which is evidence, while exit 2 says the request was not
/// understood, which is an integration mistake that must not be recorded as a Parquet defect.
/// </summary>
public class BridgeContractTests : BridgeHarness
{
    [Fact]
    public async Task Info_DeclaresTheProtocolNameVersionAndDirections()
    {
        var result = await RunAsync("info");

        Assert.Equal(0, result.ExitCode);
        var control = result.Control();
        Assert.Equal(BridgeProgram.Protocol, control.GetProperty("protocol").GetString());
        Assert.Equal(BridgeProgram.EngineName, control.GetProperty("engine").GetString());
        Assert.False(string.IsNullOrWhiteSpace(control.GetProperty("version").GetString()));

        var directions = control.GetProperty("directions")
            .EnumerateArray().Select(item => item.GetString()).ToArray();
        Assert.Equal(["read", "write"], directions);
    }

    [Fact]
    public async Task Info_DeclaresEveryProfileItCanHonorAndNoOthers()
    {
        var control = (await RunAsync("info")).Control();
        var declared = control.GetProperty("writer_profiles");

        var names = declared.EnumerateObject().Select(item => item.Name).OrderBy(name => name);
        Assert.Equal(
            ["compression-brotli", "compression-gzip", "min-max-statistics-off", "row-group-2"],
            names);

        // Every profile Parquity registers. Declaring one the writer cannot honor would record an
        // effective option that never took effect, so each name here is also asserted against the
        // option it maps to.
        Assert.Equal(
            "Gzip", declared.GetProperty("compression-gzip").GetProperty("Compression").GetString());
        Assert.Equal(
            2, declared.GetProperty("row-group-2").GetProperty("RowGroupMaxRows").GetInt32());
        Assert.False(
            declared.GetProperty("min-max-statistics-off").GetProperty("WriteStatistics")
                .GetBoolean());
    }

    [Fact]
    public async Task Info_DeclaresOnlyProfilesTheWriterActuallyMaps()
    {
        // The declaration and the option translation are separate tables; a name in one and not the
        // other would be reported as supported and then rejected mid-run.
        foreach (var name in WriterProfiles.Supported.Keys)
            Assert.NotNull(WriterProfiles.Apply(name));

        Assert.Same(ParquetWriteOptions.Default, WriterProfiles.Apply(null));
    }

    [Theory]
    [InlineData("")]
    [InlineData("bogus")]
    [InlineData("read")]
    [InlineData("read --parquet in.parquet")]
    [InlineData("write --arrow in.arrow")]
    [InlineData("read --parquet")]
    public async Task ARequestItCannotUnderstandIsRejectedWithoutBlamingTheImplementation(
        string request)
    {
        var args = request.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var result = await RunAsync(args);

        Assert.Equal(2, result.ExitCode);
        var (kind, detail) = result.Error();
        Assert.Equal("UsageError", kind);
        Assert.NotEmpty(detail);
    }

    [Fact]
    public async Task AnUnknownFlagIsRejectedRatherThanIgnored()
    {
        // Silently ignoring one would let a future contract argument look honored when it was not.
        var result = await RunAsync(
            "read", "--parquet", Path_("in.parquet"), "--arrow", Path_("out.arrow"), "--surprise", "1");

        Assert.Equal(2, result.ExitCode);
        Assert.Equal("UsageError", result.Error().Kind);
    }

    [Fact]
    public async Task AProfileItDidNotDeclareIsRejected()
    {
        string source = Path_("in.arrow");
        await WriteArrowAsync(SingleColumnBatch(), source);

        // A plausible name that is not one of the four Parquity registers, so it stays undeclared
        // however many of those this bridge grows to support.
        var result = await RunAsync(
            "write", "--arrow", source, "--parquet", Path_("out.parquet"),
            "--profile", "compression-lz4");

        Assert.Equal(2, result.ExitCode);
        var (kind, detail) = result.Error();
        Assert.Equal("UsageError", kind);
        Assert.Contains("compression-lz4", detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFailureOfTheImplementationIsEvidenceUnderItsOwnExceptionType()
    {
        // Exit 1 with the engine's own error class is what lets findings group by failure mode.
        string corrupt = Path_("corrupt.parquet");
        await File.WriteAllTextAsync(corrupt, "this is not a parquet file");

        var result = await RunAsync("read", "--parquet", corrupt, "--arrow", Path_("out.arrow"));

        Assert.Equal(1, result.ExitCode);
        var (kind, detail) = result.Error();
        Assert.NotEqual("UsageError", kind);
        Assert.Matches("^[A-Za-z_][A-Za-z0-9_.]*$", kind);
        Assert.NotEmpty(detail);
    }

    [Fact]
    public async Task AMissingInputIsTheImplementationsFailureRatherThanARejectedRequest()
    {
        // The request was well formed; the file simply is not there, which the caller learns as a
        // provider failure rather than as a contract complaint.
        var result = await RunAsync(
            "read", "--parquet", Path_("absent.parquet"), "--arrow", Path_("out.arrow"));

        Assert.Equal(1, result.ExitCode);
        Assert.Matches("^[A-Za-z_][A-Za-z0-9_.]*$", result.Error().Kind);
    }

    [Fact]
    public async Task SuccessWritesExactlyTheOkObjectAndNothingElse()
    {
        string source = Path_("in.arrow");
        await WriteArrowAsync(SingleColumnBatch(), source);

        var result = await RunAsync("write", "--arrow", source, "--parquet", Path_("out.parquet"));

        Assert.Equal(0, result.ExitCode);
        Assert.Equal("{\"status\":\"OK\"}", result.Stdout);
    }

    [Theory]
    [InlineData("plain", "\"plain\"")]
    [InlineData("with \"quotes\"", "\"with \\\"quotes\\\"\"")]
    [InlineData("back\\slash", "\"back\\\\slash\"")]
    [InlineData("line\nbreak", "\"line\\nbreak\"")]
    [InlineData("tab\there", "\"tab\\there\"")]
    [InlineData("\u0001control", "\"\\u0001control\"")]
    public void JsonStringsEscapeWhatTheyMust(string value, string expected)
    {
        // A detail carries provider text verbatim, so anything in it has to survive encoding.
        Assert.Equal(expected, Json.String(value));
    }

    [Fact]
    public void JsonObjectsAcceptOnlyTheValueKindsTheContractAllows()
    {
        var encoded = Json.Object(new Dictionary<string, object>
        {
            ["text"] = "value",
            ["number"] = 2,
            ["flag"] = true,
        });
        Assert.Equal("{\"text\":\"value\",\"number\":2,\"flag\":true}", encoded);

        Assert.Throws<ArgumentException>(
            () => Json.Object(new Dictionary<string, object> { ["ratio"] = 1.5 }));
    }

    internal static RecordBatch SingleColumnBatch()
    {
        var values = new Int32Array.Builder().Append(1).Append(2).Build();
        var schema = new Apache.Arrow.Schema(
            [new Field("value", Int32Type.Default, nullable: false)], null);
        return new RecordBatch(schema, [values], values.Length);
    }
}
