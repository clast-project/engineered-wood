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
