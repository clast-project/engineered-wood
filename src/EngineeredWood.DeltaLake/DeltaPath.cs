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
    /// URL-encodes an on-disk relative path into the <c>add.path</c> log form: control characters and
    /// <c>&#32;"#%&lt;&gt;?[]^`{|}</c> become <c>%XX</c>; <c>/</c>, non-ASCII and everything else stay
    /// literal.
    /// </summary>
    /// <remarks>
    /// <para>MEASURED, not derived: Spark's layer 2 is <c>new Path(rel).toUri().toString()</c>, i.e. Java
    /// URI quoting of the ASCII characters illegal in a URI path — and <c>toString()</c> rather than
    /// <c>toASCIIString()</c>, which is why <c>café</c> and <c>日本</c> stay literal here while delta-rs
    /// percent-encodes them as UTF-8 bytes. Enumerating <c>Path.toUri().toString()</c> over the ASCII
    /// range on pyspark 4.0.1 reproduces every <c>add.path</c> observed from real Delta writes, including
    /// the <c>%</c> → <c>%25</c> double-encoding of whatever layer 1 already escaped.</para>
    ///
    /// <para><b>Under-escaping here is a correctness bug, and it fails the whole table rather than one
    /// file.</b> This set held only <c>%</c>, space, <c>#</c> and <c>?</c>, which is enough for EW to
    /// round-trip its OWN tables — every other character decodes to itself, so <see cref="Decode"/> still
    /// recovers the right name, and <c>%</c>, the one character where under-escaping would silently
    /// resolve to the WRONG file, was already covered. Spark is not so forgiving. MEASURED on pyspark
    /// 4.0.1 / delta-spark 4.0.0: Delta passes <c>add.path</c> through <c>new URI(…)</c> inside its
    /// <c>CanonicalPathFunction</c> UDF, so one literal URI-illegal character raises
    /// <c>java.net.URISyntaxException: Illegal character in path</c> and the read of the ENTIRE TABLE
    /// fails with <c>FAILED_READ_FILE</c> — no partial result, no skipped file.</para>
    ///
    /// <para>Which characters actually reach here is decided by layer 1, and exactly four did:
    /// <c>'&lt;'</c>, <c>'&gt;'</c>, <c>'|'</c> and <c>'`'</c> are in no layer-1 escape table, so they
    /// arrived literally and this is the only place they are escaped. On Windows the first three cannot
    /// reach a written table at all (Win32 rejects them in a path component — GitHub issue #84), so the
    /// failure this fixes is a POSIX-written table read by Spark, plus the backtick on every platform.
    /// The rest of the set is unreachable because <see cref="EscapePathName"/> escaped it first; it is
    /// included anyway so that this function IS Spark's layer 2, rather than a subset that happens to
    /// suffice for the values layer 1 lets through today.</para>
    ///
    /// <para>Two characters Hadoop treats specially are deliberately absent. It REJECTS a literal
    /// <c>':'</c> outright (it reads as a URI scheme) and rewrites <c>'\'</c> to <c>'/'</c> rather than
    /// escaping it, so there is no Spark encoding to match for either. Neither can arrive here in any
    /// case: layer 1 escapes both, and EW composes relative paths with <c>'/'</c>.</para>
    /// </remarks>
    public static string Encode(string fsRelativePath)
    {
        StringBuilder? sb = null;
        for (int i = 0; i < fsRelativePath.Length; i++)
        {
            char c = fsRelativePath[i];
            bool escape = c is ' ' or '"' or '#' or '%' or '<' or '>' or '?'
                or '[' or ']' or '^' or '`' or '{' or '|' or '}' or < ' ' or '\u007f';
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

    /// <summary>
    /// Builds the Hive-style partition directory a file with these partition values belongs under
    /// (<c>date=2024-01-01/region=us</c>), each component escaped by <see cref="EscapePathName"/> — layer 1.
    /// The result is an ON-DISK relative path fragment; <see cref="Encode"/> it before writing it into
    /// <c>add.path</c>.
    ///
    /// <para>A null value becomes Hive's <c>__HIVE_DEFAULT_PARTITION__</c> sentinel, which is how the Delta
    /// spec and Spark both spell "this partition column is null" in a directory name.</para>
    ///
    /// <para>Components come out in the ENUMERATION order of
    /// <paramref name="partitionValues"/>, so a caller that needs them in the table's declared partition
    /// order must supply a dictionary built in that order.</para>
    /// </summary>
    /// <returns>The directory fragment without a trailing separator, or the empty string when there are no
    /// partition values.</returns>
    public static string BuildPartitionPath(IReadOnlyDictionary<string, string> partitionValues)
    {
        if (partitionValues.Count == 0)
            return "";

        return string.Join("/",
            partitionValues.Select(kv =>
                $"{EscapePathName(kv.Key)}={(kv.Value is null ? "__HIVE_DEFAULT_PARTITION__" : EscapePathName(kv.Value))}"));
    }
}
