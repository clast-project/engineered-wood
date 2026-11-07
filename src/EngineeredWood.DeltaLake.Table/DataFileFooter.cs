// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.IO;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// Reads a data file's footer, explaining a failure that a wrong recorded size caused (#722).
/// </summary>
/// <remarks>
/// A data file is opened with the size its action records (<see cref="KnownLengthRandomAccessFile"/>), so that
/// nothing asks the store for it before the first read. A wrong size points the footer read at the wrong bytes,
/// and the Parquet error that follows ("missing trailing PAR1 magic", an impossible footer length, a read past
/// the end) says nothing about why. Only then is the store asked for the file's size. When it differs from the
/// recorded one, the read fails with <see cref="DeltaTableErrorCodes.DataFileSizeMismatch"/>, naming both;
/// otherwise the original error stands. A good read never pays for the check.
/// </remarks>
internal static class DataFileFooter
{
    internal static async ValueTask<FileMetaData> ReadAsync(
        ParquetFileReader reader, IRandomAccessFile file, string path, CancellationToken cancellationToken)
    {
        try
        {
            return await reader.ReadMetadataAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (file is KnownLengthRandomAccessFile && ex is not OperationCanceledException)
        {
            var known = (KnownLengthRandomAccessFile)file;
            if (await known.StoredLengthAsync(cancellationToken).ConfigureAwait(false) is { } stored
                && stored != known.KnownLength)
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.DataFileSizeMismatch,
                    $"Data file '{path}' is {stored} bytes, but the log records its size as {known.KnownLength}: " +
                    $"it was changed after it was committed, or the action recording it is wrong. {ex.Message}",
                    ex);
            }
            throw;
        }
    }
}
