// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Avro.Encoding;

namespace EngineeredWood.Avro.Data;

/// <summary>Reads int from writer, appends as long to reader builder.</summary>
internal sealed class PromotingIntToLongBuilder : IColumnBuilder
{
    private Apache.Arrow.Int64Array.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadInt());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

/// <summary>Reads int from writer, appends as float to reader builder.</summary>
internal sealed class PromotingIntToFloatBuilder : IColumnBuilder
{
    private Apache.Arrow.FloatArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadInt());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

/// <summary>Reads int from writer, appends as double to reader builder.</summary>
internal sealed class PromotingIntToDoubleBuilder : IColumnBuilder
{
    private Apache.Arrow.DoubleArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadInt());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

/// <summary>Reads long from writer, appends as float to reader builder.</summary>
internal sealed class PromotingLongToFloatBuilder : IColumnBuilder
{
    private Apache.Arrow.FloatArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadLong());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

/// <summary>Reads long from writer, appends as double to reader builder.</summary>
internal sealed class PromotingLongToDoubleBuilder : IColumnBuilder
{
    private Apache.Arrow.DoubleArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadLong());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

/// <summary>Reads float from writer, appends as double to reader builder.</summary>
internal sealed class PromotingFloatToDoubleBuilder : IColumnBuilder
{
    private Apache.Arrow.DoubleArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadFloat());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

/// <summary>Reads string from writer, appends as bytes to reader builder.</summary>
internal sealed class PromotingStringToBytesBuilder : IColumnBuilder
{
    private Apache.Arrow.BinaryArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader) => _builder.Append(reader.ReadStringBytes());
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}

/// <summary>Reads bytes from writer, appends as string to reader builder.</summary>
internal sealed class PromotingBytesToStringBuilder : IColumnBuilder
{
    private Apache.Arrow.StringArray.Builder _builder = new();
    public void Append(ref AvroBinaryReader reader)
    {
        var bytes = reader.ReadBytes();
#if NETSTANDARD2_0
        _builder.Append(System.Text.Encoding.UTF8.GetString(bytes.ToArray()));
#else
        _builder.Append(System.Text.Encoding.UTF8.GetString(bytes));
#endif
    }
    public void AppendNull() => _builder.AppendNull();
    public IArrowArray Build(Field field) => _builder.Build();
    public void Reset() => _builder = new();
}
