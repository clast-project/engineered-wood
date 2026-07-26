// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Avro.Encoding;

namespace EngineeredWood.Avro.Data;

/// <summary>Reads int from writer, appends as long to reader builder.</summary>
internal sealed class PromotingIntToLongBuilder : FixedWidthBuilder<long>
{
    protected override long ReadValue(ref AvroBinaryReader reader) => reader.ReadInt();
}

/// <summary>Reads int from writer, appends as float to reader builder.</summary>
internal sealed class PromotingIntToFloatBuilder : FixedWidthBuilder<float>
{
    protected override float ReadValue(ref AvroBinaryReader reader) => reader.ReadInt();
}

/// <summary>Reads int from writer, appends as double to reader builder.</summary>
internal sealed class PromotingIntToDoubleBuilder : FixedWidthBuilder<double>
{
    protected override double ReadValue(ref AvroBinaryReader reader) => reader.ReadInt();
}

/// <summary>Reads long from writer, appends as float to reader builder.</summary>
internal sealed class PromotingLongToFloatBuilder : FixedWidthBuilder<float>
{
    protected override float ReadValue(ref AvroBinaryReader reader) => reader.ReadLong();
}

/// <summary>Reads long from writer, appends as double to reader builder.</summary>
internal sealed class PromotingLongToDoubleBuilder : FixedWidthBuilder<double>
{
    protected override double ReadValue(ref AvroBinaryReader reader) => reader.ReadLong();
}

/// <summary>Reads float from writer, appends as double to reader builder.</summary>
internal sealed class PromotingFloatToDoubleBuilder : FixedWidthBuilder<double>
{
    protected override double ReadValue(ref AvroBinaryReader reader) => reader.ReadFloat();
}

// String↔bytes promotion is a no-op at the byte level (Avro string is UTF-8 bytes on the
// wire); only the target Arrow type differs, which VarLengthBuilder.Build takes from the field.

/// <summary>Reads string from writer, appends as bytes to reader builder.</summary>
internal sealed class PromotingStringToBytesBuilder : VarLengthBuilder
{
    protected override ReadOnlySpan<byte> ReadRaw(ref AvroBinaryReader reader) => reader.ReadStringBytes();
}

/// <summary>Reads bytes from writer, appends as string to reader builder.</summary>
internal sealed class PromotingBytesToStringBuilder : VarLengthBuilder
{
    protected override ReadOnlySpan<byte> ReadRaw(ref AvroBinaryReader reader) => reader.ReadBytes();
}
