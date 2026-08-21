// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Data;
using EngineeredWood.Parquet.Metadata;
using EngineeredWood.Parquet.Schema;

// Both namespaces define TimeUnit; every use here means Arrow's.
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Parquet records only <c>isAdjustedToUTC</c> and never a zone name, so an adjusted column can
/// only come back as UTC. Which spelling it comes back as is the whole of this file: Arrow writes
/// that zone as <c>UTC</c>, and building the type from a <see cref="TimeZoneInfo"/> renders the
/// <c>+00:00</c> offset instead — legal, but a spelling no other Parquet-to-Arrow implementation
/// produces, and therefore a type mismatch to every consumer that compares zone strings.
/// </summary>
public class TimestampZoneNameTests : IDisposable
{
    private readonly string _tempDir;

    public TimestampZoneNameTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-tzname-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    private static readonly ParquetWriteOptions Uncompressed =
        new() { Compression = CompressionCodec.Uncompressed };

    private async Task<Apache.Arrow.Types.IArrowType> RoundTripAsync(TimestampType type)
    {
        string path = Path.Combine(_tempDir, $"tz-{Guid.NewGuid().ToString("N")[..8]}.parquet");
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("at", type, nullable: true))
            .Build();
        var values = new TimestampArray.Builder(type)
            .Append(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000))
            .Build();

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false, Uncompressed))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [values], length: 1));
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        return (await reader.ReadRowGroupAsync(0)).Schema.FieldsList[0].DataType;
    }

    [Theory]
    [InlineData(TimeUnit.Millisecond)]
    [InlineData(TimeUnit.Microsecond)]
    public async Task AnAdjustedTimestampNamesItsZoneUtc(TimeUnit unit)
    {
        var read = Assert.IsType<TimestampType>(await RoundTripAsync(new TimestampType(unit, "UTC")));

        Assert.Equal("UTC", read.Timezone);
        // The spelling is the point, so say what it must not be as well as what it must.
        Assert.NotEqual("+00:00", read.Timezone);
    }

    [Fact]
    public async Task AnUnadjustedTimestampStillHasNoZone()
    {
        // Only the adjusted case carries a zone. Naming one on a naive column would invent
        // information the file does not hold.
        var read = Assert.IsType<TimestampType>(
            await RoundTripAsync(new TimestampType(TimeUnit.Microsecond, (string?)null)));

        Assert.Null(read.Timezone);
    }

    [Fact]
    public async Task AZoneThatIsNotUtcIsStillReadAsUtc()
    {
        // Parquet has nowhere to put the name, so an adjusted column written with any zone comes
        // back as UTC. That is a property of the format rather than of this fix, and pinning it
        // keeps the previous assertions from being read as a promise to round-trip zone names.
        var read = Assert.IsType<TimestampType>(
            await RoundTripAsync(new TimestampType(TimeUnit.Microsecond, "America/New_York")));

        Assert.Equal("UTC", read.Timezone);
    }

    [Theory]
    [InlineData(ConvertedType.TimestampMillis, TimeUnit.Millisecond)]
    [InlineData(ConvertedType.TimestampMicros, TimeUnit.Microsecond)]
    public void ALegacyConvertedTypeTimestampAlsoNamesItsZoneUtc(
        ConvertedType converted, TimeUnit unit)
    {
        // A file old enough to carry only a converted type and no LogicalType takes a different
        // branch of the converter, and it was rendering the same "+00:00". Those files are exactly
        // where this matters -- written by a tool old enough that everything else is still reading
        // its output. No fixture can cover it: PyArrow writes both annotations at every format
        // version, so the converted-type-only path is reachable through the converter alone.
        var element = new SchemaElement
        {
            Name = "at",
            Type = PhysicalType.Int64,
            RepetitionType = FieldRepetitionType.Optional,
            ConvertedType = converted,
        };
        var column = new ColumnDescriptor
        {
            Path = ["at"],
            PhysicalType = PhysicalType.Int64,
            MaxDefinitionLevel = 1,
            MaxRepetitionLevel = 0,
            SchemaElement = element,
            SchemaNode = new SchemaNode { Element = element, Children = [] },
        };

        var type = Assert.IsType<TimestampType>(ArrowSchemaConverter.ToArrowType(column));

        Assert.Equal(unit, type.Unit);
        Assert.Equal("UTC", type.Timezone);
    }
}
