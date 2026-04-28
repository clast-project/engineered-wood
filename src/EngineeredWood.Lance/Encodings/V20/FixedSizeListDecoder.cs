// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance.Proto.Encodings.V20;

namespace EngineeredWood.Lance.Encodings.V20;

/// <summary>
/// Decodes a <see cref="FixedSizeList"/> v2.0 ArrayEncoding. FSL columns are
/// single-column in v2.0: the inner <c>items</c> encoding produces
/// <c>num_rows × dimension</c> values stored inline in the same column's
/// page buffers. Target type must be <see cref="FixedSizeListType"/>.
/// </summary>
internal sealed class FixedSizeListDecoder : IArrayDecoder
{
    private readonly FixedSizeList _encoding;

    public FixedSizeListDecoder(FixedSizeList encoding) => _encoding = encoding;

    public IArrowArray Decode(long numRows, IArrowType targetType, in PageContext context)
    {
        if (targetType is not FixedSizeListType fslType)
            throw new LanceFormatException(
                $"FixedSizeList encoding used with non-FSL target type {targetType}.");
        if ((int)_encoding.Dimension != fslType.ListSize)
            throw new LanceFormatException(
                $"FixedSizeList dimension mismatch: schema has {fslType.ListSize}, encoding has {_encoding.Dimension}.");
        if (_encoding.HasValidity)
            throw new NotImplementedException(
                "FixedSizeList with has_validity=true is not yet supported.");
        if (_encoding.Items is null)
            throw new LanceFormatException("FixedSizeList encoding is missing the items sub-encoding.");

        int length = checked((int)numRows);
        long innerRows = checked((long)length * fslType.ListSize);

        IArrayDecoder innerDecoder = V20ArrayEncodingDispatcher.Create(_encoding.Items);
        IArrowArray innerArray = innerDecoder.Decode(innerRows, fslType.ValueDataType, context);

        var data = new ArrayData(
            fslType,
            length: length,
            nullCount: 0,
            offset: 0,
            buffers: new[] { ArrowBuffer.Empty },
            children: new[] { innerArray.Data });
        return new FixedSizeListArray(data);
    }
}
