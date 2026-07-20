// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Actions;

/// <summary>
/// Base type for all Delta Lake transaction log actions.
/// Each line in a commit file is a JSON object wrapping exactly one action.
/// </summary>
public abstract record DeltaAction
{
}
