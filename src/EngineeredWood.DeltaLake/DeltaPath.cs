// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Path escaping for Delta tables. Two DISTINCT layers exist (both matching Spark):
/// <list type="number">
/// <item><see cref="EscapePathName"/> — Hive-style escaping applied to a partition VALUE when building the
/// partition directory name on disk (escapes <c>/</c>, <c>:</c>, <c>%</c>, … as <c>%XX</c>; a space is NOT
/// escaped). This is what makes the physical directory name filesystem-safe.</item>
/// <item><see cref="Encode"/>/<see cref="Decode"/> — the <c>add.path</c> field in the transaction log is the
/// URL-encoded (RFC 2396) form of the on-disk relative path, so a literal <c>%</c> from layer 1 becomes
/// <c>%25</c> and a space becomes <c>%20</c>. Readers (Spark, delta-kernel, delta-rs) URL-DECODE
/// <c>add.path</c> before opening the file.</item>
/// </list>
/// </summary>
public static class DeltaPath
{
    /// <summary>
    /// Escapes a partition value for use as a directory name, matching Spark's
    /// <c>ExternalCatalogUtils.escapePathName</c> (control chars and <c>"#%'*/:=?\{[]}^</c> become
    /// <c>%XX</c>; a space stays literal).
    /// </summary>
    public static string EscapePathName(string value)
    {
        StringBuilder? sb = null;
        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            if (NeedsPathEscaping(c))
            {
                sb ??= new StringBuilder(value.Length + 8).Append(value, 0, i);
                sb.Append('%').Append(((int)c).ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
            }
            else
            {
                sb?.Append(c);
            }
        }
        return sb?.ToString() ?? value;
    }

    private static bool NeedsPathEscaping(char c) => c switch
    {
        < ' ' or '\u007f' => true,
        '"' or '#' or '%' or '\'' or '*' or '/' or ':' or '=' or '?' or '\\' => true,
        '{' or '[' or ']' or '}' or '^' => true,
        _ => false,
    };

    /// <summary>
    /// URL-encodes an on-disk relative path into the <c>add.path</c> log form: <c>%</c>, space, <c>#</c>,
    /// <c>?</c> and control characters become <c>%XX</c>; <c>/</c> and everything else stay literal.
    /// </summary>
    public static string Encode(string fsRelativePath)
    {
        StringBuilder? sb = null;
        for (int i = 0; i < fsRelativePath.Length; i++)
        {
            char c = fsRelativePath[i];
            bool escape = c is '%' or ' ' or '#' or '?' or < ' ' or '\u007f';
            if (escape)
            {
                sb ??= new StringBuilder(fsRelativePath.Length + 8).Append(fsRelativePath, 0, i);
                sb.Append('%').Append(((int)c).ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
            }
            else
            {
                sb?.Append(c);
            }
        }
        return sb?.ToString() ?? fsRelativePath;
    }

    /// <summary>Decodes an <c>add.path</c> log value into the on-disk relative path.</summary>
    public static string Decode(string logPath) =>
        logPath.IndexOf('%') < 0 ? logPath : Uri.UnescapeDataString(logPath);
}
