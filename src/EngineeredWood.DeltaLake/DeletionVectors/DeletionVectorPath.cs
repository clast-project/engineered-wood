// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.IO;

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// <para>Resolves a <see cref="DeletionVector"/> to the table-relative path of the <c>.bin</c> file
/// backing it.</para>
///
/// <para>Shared deliberately between <see cref="DeletionVectorReader"/> and VACUUM. The reader uses it
/// to FIND a DV file; vacuum uses it to decide a DV file must be KEPT. If those two derivations ever
/// disagreed, vacuum would delete a live deletion vector and every row it masks would silently come
/// back — so there must be exactly one implementation.</para>
/// </summary>
internal static class DeletionVectorPath
{
    /// <summary>
    /// The table-relative path of the DV file, or <see langword="null"/> when the vector has no file
    /// of its own — inline (<c>i</c>) vectors live in the log, and absolute (<c>p</c>) vectors are
    /// addressed outside this scheme (see <see cref="IsAbsolute"/>).
    /// </summary>
    public static string? GetRelativePath(DeletionVector dv) => dv.StorageType switch
    {
        "u" => UuidRelativePath(dv.PathOrInlineDv),
        _ => null,
    };

    /// <summary>
    /// True for the absolute-path storage type (<c>p</c>), whose target cannot be resolved against the
    /// table root from the action alone. Callers that DELETE must treat this as unresolvable rather
    /// than as "no file" — see the vacuum guard.
    /// </summary>
    public static bool IsAbsolute(DeletionVector dv) =>
        string.Equals(dv.StorageType, "p", StringComparison.Ordinal);

    /// <summary>
    /// <para>The location of an absolute-path (<c>p</c>) vector's file. PROTOCOL.md makes it an absolute path, and
    /// delta-spark reads it as a URI: <c>DeletionVectorDescriptor.absolutePath</c> asserts
    /// <c>new URI(pathOrInlineDv).isAbsolute</c> ("Relative URIs are not supported for DVs"), which needs a scheme,
    /// and then opens <c>new Path(uri)</c>, which decodes the path's escapes (READ FROM the delta-spark 4.0.0 jar's
    /// bytecode). Both are matched here: a bare path, even a rooted one, is refused, and the URI's structure is read
    /// before its segments are decoded (<see cref="StorageLocation.ParsePercentEncoded"/>).</para>
    /// <para>A <c>.</c> or <c>..</c> segment in an object-store location, written or decoded, is refused, as
    /// <see cref="StorageLocation"/> refuses it everywhere (#963).</para>
    /// </summary>
    public static StorageLocation AbsoluteLocation(DeletionVector dv)
    {
        string text = dv.PathOrInlineDv;
        if (!HasUriScheme(text))
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector,
                $"Deletion vector storage type 'p' must hold an absolute URI, but it holds '{text}'.");
        try
        {
            return StorageLocation.ParsePercentEncoded(text);
        }
        catch (FormatException ex)
        {
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector,
                $"Deletion vector path '{text}' is not a valid absolute location: {ex.Message}", ex);
        }
    }

    /// <summary>True when <paramref name="text"/> starts with a URI scheme (RFC 3986: a letter, then letters,
    /// digits, <c>+</c>, <c>-</c> or <c>.</c>, then <c>:</c>). A single letter is a Windows drive, not a scheme.</summary>
    private static bool HasUriScheme(string text)
    {
        int colon = text.IndexOf(':');
        if (colon < 2 || !IsAsciiLetter(text[0]))
            return false;
        for (int i = 1; i < colon; i++)
        {
            char c = text[i];
            if (!(IsAsciiLetter(c) || c is >= '0' and <= '9' || c is '+' or '-' or '.'))
                return false;
        }

        return true;
    }

    private static bool IsAsciiLetter(char c) => c is >= 'a' and <= 'z' or >= 'A' and <= 'Z';

    /// <summary>
    /// Spec: <c>pathOrInlineDv = "&lt;random prefix (optional)&gt;&lt;z85-encoded uuid&gt;"</c> (the uuid is
    /// the LAST 20 characters). The file lives in the TABLE ROOT next to the data files — NOT in
    /// <c>_delta_log/</c> — at <c>"&lt;prefix&gt;/deletion_vector_&lt;uuid&gt;.bin"</c>; the prefix, when
    /// present, is a directory, like the random data-file prefixes Spark writes.
    /// </summary>
    private static string UuidRelativePath(string pathOrUuid)
    {
        string uuid = DecodeUuid(pathOrUuid);
        string prefix = pathOrUuid.Length > 20 ? pathOrUuid.Substring(0, pathOrUuid.Length - 20) : "";
        return prefix.Length > 0
            ? $"{prefix}/deletion_vector_{uuid}.bin"
            : $"deletion_vector_{uuid}.bin";
    }

    /// <summary>Decodes the Z85-encoded 16-byte UUID that terminates a DV path.</summary>
    private static string DecodeUuid(string encodedPath)
    {
        // Z85-encoded 16 bytes = 20 characters; anything before that is the directory prefix.
        if (encodedPath.Length < 20)
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector, $"UUID path too short: '{encodedPath}'");

        byte[] uuidBytes = Base85.Decode(encodedPath.Substring(encodedPath.Length - 20));
        if (uuidBytes.Length != 16)
            throw new DeltaFormatException(
                DeltaErrorCodes.InvalidDeletionVector,
                $"Expected 16-byte UUID, got {uuidBytes.Length} bytes.");

        // The file-name UUID is the canonical (BIG-ENDIAN / Java) rendering of the 16 bytes. .NET's
        // Guid(byte[]) shuffles the first three groups little-endian — format by hand instead.
        var sb = new StringBuilder(36);
        for (int i = 0; i < 16; i++)
        {
            if (i is 4 or 6 or 8 or 10)
                sb.Append('-');
            sb.Append(uuidBytes[i].ToString("x2"));
        }

        return sb.ToString();
    }
}
