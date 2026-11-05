// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;

namespace EngineeredWood.Expressions;

/// <summary>
/// A recognized extraction of one path out of a variant column, cast to a type: Spark's
/// <c>variant_get(v, '$.a', 'int')</c>, its <c>try_variant_get</c> form, and the <c>v:a::int</c> shorthand
/// (<c>cast(variant_get(v, '$.a') AS int)</c>, or <c>try_cast</c>).
/// </summary>
/// <remarks>
/// <para>These stay ordinary <see cref="FunctionCall"/>s, which is what the SQL parser produces and what a
/// function registry dispatches on. This is a read of one, for consumers that reason about the path rather
/// than evaluate it, such as pruning by per-path statistics.</para>
/// <para>What Spark answers, measured against Spark 4.1: a missing path is NULL, and so is a path through a
/// value that is not an object (or array). A value that does not convert to the type is an error
/// (<c>INVALID_VARIANT_CAST</c>), or NULL under <c>try_</c>. A variant null is NULL.</para>
/// </remarks>
public sealed record VariantGetCall(
    UnboundReference Column, VariantPath Path, string TargetType, bool FailOnError)
{
    /// <summary>The type name for no conversion: the result is itself a variant.</summary>
    public const string VariantTypeName = "variant";

    /// <summary>
    /// Reads <paramref name="expression"/> as a variant extraction, or false when it is not one this
    /// recognizes: the variant must be a column, and the path and type must be string literals that parse.
    /// </summary>
    /// <remarks>
    /// An extraction from an extraction that yields a variant, <c>v:a:b</c>, reads as one extraction of the
    /// joined path, which is what Spark answers for it. <see cref="TargetType"/> is canonical (see
    /// <see cref="TryNormalizeTypeName"/>).
    /// </remarks>
    public static bool TryMatch(Expression expression, out VariantGetCall call)
    {
        call = null!;
        if (expression is not FunctionCall fn)
            return false;

        if (IsName(fn, "cast") || IsName(fn, "try_cast"))
        {
            if (fn.Arguments.Count != 2
                || fn.Arguments[1] is not LiteralExpression { Value.Type: LiteralValue.Kind.String } typeLit
                || !TryMatch(fn.Arguments[0], out var inner)
                || inner.TargetType != VariantTypeName
                || !TryNormalizeTypeName(typeLit.Value.AsString, out string castType))
            {
                return false;
            }
            // A variant cast to a type fails on a value that does not convert, as variant_get does; try_cast
            // answers NULL instead. An inner try_variant_get only ever yields a variant, which never fails.
            call = inner with { TargetType = castType, FailOnError = IsName(fn, "cast") };
            return true;
        }

        bool isTry = IsName(fn, "try_variant_get");
        if (!isTry && !IsName(fn, "variant_get"))
            return false;
        if (fn.Arguments.Count is not (2 or 3)
            || fn.Arguments[1] is not LiteralExpression { Value.Type: LiteralValue.Kind.String } pathLit
            || !VariantPath.TryParse(pathLit.Value.AsString, out var path))
        {
            return false;
        }

        string type = VariantTypeName;
        if (fn.Arguments.Count == 3
            && (fn.Arguments[2] is not LiteralExpression { Value.Type: LiteralValue.Kind.String } typeArg
                || !TryNormalizeTypeName(typeArg.Value.AsString, out type)))
        {
            return false;
        }

        switch (fn.Arguments[0])
        {
            case UnboundReference column:
                call = new VariantGetCall(column, path, type, !isTry);
                return true;
            case FunctionCall when TryMatch(fn.Arguments[0], out var outer) && outer.TargetType == VariantTypeName:
                // Extracting a variant never fails, so only this call decides.
                call = new VariantGetCall(outer.Column, outer.Path.Append(path), type, !isTry);
                return true;
            default:
                return false;
        }
    }

    /// <summary>The call this record reads back from: <c>variant_get</c> or <c>try_variant_get</c>.</summary>
    public FunctionCall ToFunctionCall()
    {
        string text = Path.ToPathText()
            ?? throw new InvalidOperationException($"The path {Path} has a key no path text can spell.");
        return new FunctionCall(
            FailOnError ? "variant_get" : "try_variant_get",
            new Expression[]
            {
                Column, new LiteralExpression(LiteralValue.Of(text)), new LiteralExpression(LiteralValue.Of(TargetType)),
            });
    }

    /// <summary>
    /// Canonical spelling of a Spark type name, as <c>DataType.fromDDL</c> reads it: case and surrounding space
    /// ignored, aliases folded (<c>integer</c> to <c>int</c>, <c>long</c> to <c>bigint</c>,
    /// <c>timestamp_ltz</c> to <c>timestamp</c>, <c>decimal</c> to <c>decimal(10,0)</c>, ...). False for a type
    /// outside the scalar types and <c>variant</c>.
    /// </summary>
    public static bool TryNormalizeTypeName(string? text, out string type)
    {
        type = "";
        if (text is null)
            return false;

        string name = text.Trim().ToLowerInvariant();
        type = name switch
        {
            "boolean" or "bool" => "boolean",
            "tinyint" or "byte" => "tinyint",
            "smallint" or "short" => "smallint",
            "int" or "integer" => "int",
            "bigint" or "long" => "bigint",
            "float" or "real" => "float",
            "double" => "double",
            "string" => "string",
            "binary" => "binary",
            "date" => "date",
            "timestamp" or "timestamp_ltz" => "timestamp",
            "timestamp_ntz" => "timestamp_ntz",
            "variant" => VariantTypeName,
            _ => "",
        };
        if (type.Length > 0)
            return true;

        return TryNormalizeDecimal(name, out type);
    }

    private static bool TryNormalizeDecimal(string name, out string type)
    {
        type = "";
        int open = name.IndexOf('(');
        string head = (open < 0 ? name : name.Substring(0, open)).TrimEnd();
        if (head is not ("decimal" or "dec" or "numeric"))
            return false;

        int precision = 10, scale = 0;
        if (open >= 0)
        {
            if (!name.EndsWith(")", StringComparison.Ordinal))
                return false;
            string[] parts = name.Substring(open + 1, name.Length - open - 2).Split(',');
            if (parts.Length is not (1 or 2)
                || !int.TryParse(parts[0].Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out precision)
                || (parts.Length == 2
                    && !int.TryParse(parts[1].Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out scale)))
            {
                return false;
            }
        }

        if (precision is < 1 or > 38 || scale > precision)
            return false;
        type = string.Format(CultureInfo.InvariantCulture, "decimal({0},{1})", precision, scale);
        return true;
    }

    private static bool IsName(FunctionCall fn, string name) =>
        string.Equals(fn.Name, name, StringComparison.OrdinalIgnoreCase);
}
