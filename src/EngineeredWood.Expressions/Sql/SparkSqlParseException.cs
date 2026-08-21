// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Expressions.Sql;

/// <summary>
/// Thrown when a Spark SQL expression cannot be read — because it is malformed, or because it
/// uses a construct this parser does not support.
/// </summary>
/// <remarks>
/// This is deliberately a distinct, quotable failure rather than a generic one. A Delta table can
/// carry a CHECK constraint or generation expression that this parser does not understand, and
/// the required behaviour there is to refuse the write with an explanation — the same
/// fail-closed outcome the table already had — never to commit rows that were not validated. A
/// caller that catches this and reports <see cref="Exception.Message"/> alongside
/// <see cref="Expression"/> tells the user exactly which constraint stopped them and where.
/// </remarks>
public sealed class SparkSqlParseException : Exception
{
    internal SparkSqlParseException(string message, string expression, int position)
        : base($"{message} at position {position} in: {expression}")
    {
        Expression = expression;
        Position = position;
        Reason = message;
    }

    /// <summary>The expression text that could not be read, as supplied.</summary>
    public string Expression { get; }

    /// <summary>Zero-based character offset in <see cref="Expression"/> where reading stopped.</summary>
    public int Position { get; }

    /// <summary>The failure on its own, without the position and expression appended.</summary>
    public string Reason { get; }
}
