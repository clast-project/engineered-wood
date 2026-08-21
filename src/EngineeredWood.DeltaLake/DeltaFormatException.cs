// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake;

/// <summary>
/// Thrown when a Delta Lake table contains data that violates the protocol
/// or cannot be interpreted by this implementation.
/// </summary>
/// <remarks>
/// <para>One type covers several unrelated failures — a table that is not there, a feature we do not
/// implement, a truncated log, genuinely malformed bytes — so <see cref="ErrorCode"/> is how a caller
/// tells them apart. Match on that, never on <see cref="Exception.Message"/>: the codes are stable and
/// the prose is not. <see cref="DeltaErrorCodes"/> lists them.</para>
///
/// <para>Only one condition has its own type. <see cref="DeltaTableNotFoundException"/> exists because
/// "there is no table at this path" is not a *format* failure at all and is the case most likely to be
/// caught inline rather than routed through a translation layer. Every other condition is this type
/// plus a code — a class per condition would be forty of them.</para>
/// </remarks>
public class DeltaFormatException : Exception
{
    /// <summary>
    /// The stable identifier for the condition, or <see langword="null"/> when the exception came from
    /// a constructor that did not supply one.
    /// </summary>
    /// <remarks>
    /// Nullable because the message-only constructors are public and pre-date this property, so a
    /// third-party caller can still construct one without a code. Everything thrown from inside this
    /// library carries one.
    /// </remarks>
    public string? ErrorCode { get; }

    /// <summary>Creates an exception with no error code. Prefer the overload that takes one.</summary>
    public DeltaFormatException(string message) : base(message) { }

    /// <summary>Creates an exception with no error code. Prefer the overload that takes one.</summary>
    public DeltaFormatException(string message, Exception innerException)
        : base(message, innerException) { }

    /// <summary>
    /// Creates an exception identified by <paramref name="errorCode"/>, which should be one of the
    /// constants on <see cref="DeltaErrorCodes"/> rather than a literal.
    /// </summary>
    public DeltaFormatException(string errorCode, string message)
        : base(message) => ErrorCode = errorCode;

    /// <summary>
    /// Creates an exception identified by <paramref name="errorCode"/>, which should be one of the
    /// constants on <see cref="DeltaErrorCodes"/> rather than a literal.
    /// </summary>
    public DeltaFormatException(string errorCode, string message, Exception innerException)
        : base(message, innerException) => ErrorCode = errorCode;
}

/// <summary>
/// Thrown when a path holds no Delta table at all — the log names no version, by commit or by
/// checkpoint.
/// </summary>
/// <remarks>
/// <para>Its own type because the caller's response differs in kind: create the table, or report a bad
/// path. Every other failure in this hierarchy means "there is a table and we cannot use it", which is
/// a code on <see cref="DeltaFormatException"/>.</para>
///
/// <para>Derived from <see cref="DeltaFormatException"/> rather than <see cref="Exception"/> so that
/// existing <c>catch (DeltaFormatException)</c> handlers keep catching it. That is a compromise:
/// a missing table is not a malformed one, and a cleaner hierarchy would not put it here. The
/// compatibility is worth more than the taxonomy while callers exist.</para>
/// </remarks>
public sealed class DeltaTableNotFoundException : DeltaFormatException
{
    /// <summary>
    /// Creates the exception, always carrying <see cref="DeltaErrorCodes.PathDoesNotExist"/>.
    /// </summary>
    public DeltaTableNotFoundException(string message)
        : base(DeltaErrorCodes.PathDoesNotExist, message) { }
}
