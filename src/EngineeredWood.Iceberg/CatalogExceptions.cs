// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>Thrown when an operation references a namespace that does not exist in the catalog.</summary>
public class NoSuchNamespaceException : Exception
{
    public Namespace Namespace { get; }

    public NoSuchNamespaceException(Namespace ns)
        : base($"Namespace does not exist: {ns}")
    {
        Namespace = ns;
    }
}

/// <summary>Thrown when attempting to create a namespace that already exists in the catalog.</summary>
public class NamespaceAlreadyExistsException : Exception
{
    public Namespace Namespace { get; }

    public NamespaceAlreadyExistsException(Namespace ns)
        : base($"Namespace already exists: {ns}")
    {
        Namespace = ns;
    }
}

/// <summary>Thrown when attempting to drop a namespace that still contains tables or child namespaces.</summary>
public class NamespaceNotEmptyException : Exception
{
    public Namespace Namespace { get; }

    public NamespaceNotEmptyException(Namespace ns)
        : base($"Namespace is not empty: {ns}")
    {
        Namespace = ns;
    }
}

/// <summary>Thrown when an operation references a table that does not exist in the catalog.</summary>
public class NoSuchTableException : Exception
{
    public TableIdentifier Identifier { get; }

    public NoSuchTableException(TableIdentifier identifier)
        : base($"Table does not exist: {identifier}")
    {
        Identifier = identifier;
    }
}

/// <summary>Thrown when attempting to create a table that already exists in the catalog.</summary>
public class TableAlreadyExistsException : Exception
{
    public TableIdentifier Identifier { get; }

    public TableAlreadyExistsException(TableIdentifier identifier)
        : base($"Table already exists: {identifier}")
    {
        Identifier = identifier;
    }
}

/// <summary>
/// Thrown when a commit loses a race: the table changed after the metadata the commit was based on was
/// loaded, so the new metadata was not written. Nothing was committed. The commit can be retried by
/// reloading the table and applying the change again, which
/// <see cref="CatalogCommitExtensions.CommitWithRetryAsync"/> does.
/// </summary>
public class CommitFailedException : Exception
{
    /// <summary>The table whose commit failed.</summary>
    public TableIdentifier Identifier { get; }

    /// <summary>Creates the exception for <paramref name="identifier"/>.</summary>
    public CommitFailedException(TableIdentifier identifier, string message)
        : base($"Commit to {identifier} failed: {message}")
    {
        Identifier = identifier;
    }
}

/// <summary>
/// Thrown when an operation cannot be applied to the table as it now stands, for example an overwrite that
/// deletes a file the table no longer contains. Unlike <see cref="CommitFailedException"/>, retrying against
/// newer metadata does not help: the change itself conflicts with the table.
/// </summary>
public class CommitValidationException : Exception
{
    /// <summary>Creates the exception with the given message.</summary>
    public CommitValidationException(string message)
        : base(message)
    {
    }
}
