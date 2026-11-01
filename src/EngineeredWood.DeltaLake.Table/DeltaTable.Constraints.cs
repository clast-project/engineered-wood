// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Concurrency;
using EngineeredWood.Expressions.Arrow;
using EngineeredWood.Expressions.Arrow.Spark;

namespace EngineeredWood.DeltaLake.Table;

public sealed partial class DeltaTable
{
    private const string ConstraintKeyPrefix = "delta.constraints.";
    private const string CheckConstraintsFeature = "checkConstraints";

    /// <summary>
    /// Adds a CHECK constraint to the table: <c>ALTER TABLE … ADD CONSTRAINT name CHECK (expression)</c>. Every
    /// row the table holds must satisfy it, and every later write is checked against it.
    ///
    /// <para>The constraint is stored as <c>delta.constraints.&lt;name&gt;</c> with the name lower-cased, as Spark
    /// stores it, and a name already declared in any case is refused. The expression is Spark SQL, and must be a
    /// boolean this library can evaluate over columns the table has.</para>
    ///
    /// <para>The table is read first, and the constraint is refused, with the number of rows that violate it, if
    /// any row does: a row for which it is false, or null. Only the columns it names are read. A file whose
    /// statistics prove every row satisfies it is skipped when <see cref="DeltaTableOptions.TrustStatisticsProofs"/>
    /// is on; Spark reads every file.</para>
    ///
    /// <para>The commit declares the <c>checkConstraints</c> writer feature: a legacy protocol below writer
    /// version 3 is raised to 3, as Spark raises it, and a table-features protocol gets the feature added. It
    /// conflicts with any concurrent commit that adds or removes data, a blind append included, since a row
    /// committed after the read was never checked: it reads the whole table, and it changes the metadata, which
    /// makes even a blind append one it examines.</para>
    /// </summary>
    /// <param name="name">The constraint's name.</param>
    /// <param name="expression">The condition, in Spark SQL, such as <c>id &gt; 0</c>.</param>
    /// <param name="cancellationToken">Cancels the read or the commit.</param>
    /// <param name="commitProperties">The caller's own metadata for the commit.</param>
    /// <returns>The committed version.</returns>
    /// <exception cref="DeltaFormatException">The name is taken
    /// (<see cref="DeltaTableErrorCodes.ConstraintAlreadyExists"/>), the expression is not one this library can
    /// add (see <see cref="DeltaTableErrorCodes.UnresolvedColumn"/>,
    /// <see cref="DeltaTableErrorCodes.NonBooleanCheckConstraint"/> and
    /// <see cref="DeltaTableErrorCodes.UnevaluableTableExpression"/>), or rows violate it
    /// (<see cref="DeltaTableErrorCodes.NewCheckConstraintViolation"/>).</exception>
    /// <exception cref="DeltaConflictException">A concurrent commit changed the table's data or metadata.</exception>
    public async ValueTask<long> AddConstraintAsync(
        string name, string expression, CancellationToken cancellationToken = default,
        CommitProperties? commitProperties = null)
    {
        ThrowIfDisposed();
        ValidateConstraintName(name);
        if (expression is null)
            throw new ArgumentNullException(nameof(expression));

        var snapshot = CurrentSnapshot;
        ValidateWriteSupport(snapshot);

        string lowered = name.ToLowerInvariant();
        if (FindConstraint(snapshot, name) is { } existing)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.ConstraintAlreadyExists,
                $"Constraint '{lowered}' already exists. Drop the old constraint first. Old constraint: "
                + existing.Value);
        }

        var predicate = DeltaConstraintEnforcer.ParseNewConstraint(lowered, expression, snapshot);
        long violations = await CountConstraintViolationsAsync(
            snapshot, predicate, $"CHECK constraint '{lowered}'", expression, cancellationToken).ConfigureAwait(false);
        if (violations > 0)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.NewCheckConstraintViolation,
                $"{violations} rows in the table violate the new CHECK constraint ({expression}). A row for which "
                + "it is null violates it too. Nothing was committed.");
        }

        var configuration = new Dictionary<string, string>(StringComparer.Ordinal);
        if (snapshot.Metadata.Configuration is { } current)
        {
            foreach (var pair in current)
                configuration[pair.Key] = pair.Value;
        }
        configuration[ConstraintKeyPrefix + lowered] = expression;

        var actions = new List<DeltaAction>(2);
        if (CheckConstraintsProtocol(snapshot.Protocol) is { } upgrade)
            actions.Add(upgrade);
        actions.Add(snapshot.Metadata with { Configuration = configuration });

        // A concurrent blind append conflicts too, though this is WriteSerializable: its rows were written before
        // the constraint existed and never checked against it, and the read above did not see them. A commit that
        // changes the metadata examines blind appends at either level (ConflictChecker.ExamineConcurrentAdds,
        // Delta's own gate), and the whole-table read makes every add one it read.
        return await CommitOccAsync(
            snapshot, actions, new ReadSet { WholeTable = true }, IsolationLevel.WriteSerializable, "ADD CONSTRAINT",
            rebaseSafe: true, cancellationToken, isBlindAppend: false, commitProperties: commitProperties,
            operationParameters: new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["name"] = name,
                ["expr"] = expression,
            }).ConfigureAwait(false);
    }

    /// <summary>
    /// Drops a CHECK constraint: <c>ALTER TABLE … DROP CONSTRAINT [IF EXISTS] name</c>. The name matches in any
    /// case. The <c>checkConstraints</c> feature stays declared, as Spark leaves it.
    ///
    /// <para>With <paramref name="ifExists"/>, dropping a constraint the table does not declare still commits,
    /// changing nothing and recording <c>existed: false</c> in history, as Spark does.</para>
    /// </summary>
    /// <param name="name">The constraint's name.</param>
    /// <param name="ifExists">Whether a constraint the table does not declare is dropped without error.</param>
    /// <param name="cancellationToken">Cancels the commit.</param>
    /// <param name="commitProperties">The caller's own metadata for the commit.</param>
    /// <returns>The committed version.</returns>
    /// <exception cref="DeltaFormatException">The table declares no such constraint and
    /// <paramref name="ifExists"/> is false (<see cref="DeltaTableErrorCodes.ConstraintDoesNotExist"/>).</exception>
    public async ValueTask<long> DropConstraintAsync(
        string name, bool ifExists = false, CancellationToken cancellationToken = default,
        CommitProperties? commitProperties = null)
    {
        ThrowIfDisposed();
        ValidateConstraintName(name);

        var snapshot = CurrentSnapshot;
        ValidateWriteSupport(snapshot);

        var existing = FindConstraint(snapshot, name);
        if (existing is null && !ifExists)
        {
            throw new DeltaFormatException(
                DeltaTableErrorCodes.ConstraintDoesNotExist,
                $"Cannot drop nonexistent constraint {name.ToLowerInvariant()}. Pass ifExists to drop it only if it "
                + "exists.");
        }

        var parameters = new Dictionary<string, string>(StringComparer.Ordinal) { ["name"] = name };
        var metadata = snapshot.Metadata;
        if (existing is { } found)
        {
            parameters["expr"] = found.Value;
            var configuration = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var pair in snapshot.Metadata.Configuration!)
            {
                if (pair.Key != found.Key)
                    configuration[pair.Key] = pair.Value;
            }
            metadata = metadata with { Configuration = configuration };
        }
        parameters["existed"] = existing is null ? "false" : "true";

        // The metadata is derived from the base snapshot's, which the conflict checker's metadata rule keeps
        // sound across a rebase: see CommitMetadataOnlyAsync.
        return await CommitOccAsync(
            snapshot, [metadata], ReadSet.Blind, IsolationLevel.WriteSerializable, "DROP CONSTRAINT",
            rebaseSafe: true, cancellationToken, isBlindAppend: false, commitProperties: commitProperties,
            operationParameters: parameters).ConfigureAwait(false);
    }

    private static void ValidateConstraintName(string name)
    {
        if (name is null)
            throw new ArgumentNullException(nameof(name));
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("A constraint name may not be empty.", nameof(name));
    }

    /// <summary>
    /// The table's constraint named <paramref name="name"/>, matched without regard to case: Spark stores names
    /// lower-cased, but a constraint given in <c>configuration</c> at creation keeps the case it was given.
    /// </summary>
    private static KeyValuePair<string, string>? FindConstraint(Snapshot.Snapshot snapshot, string name)
    {
        if (snapshot.Metadata.Configuration is not { } configuration)
            return null;

        KeyValuePair<string, string>? match = null;
        foreach (var pair in configuration)
        {
            if (pair.Key.Length != ConstraintKeyPrefix.Length + name.Length
                || !pair.Key.StartsWith(ConstraintKeyPrefix, StringComparison.OrdinalIgnoreCase)
                || string.Compare(pair.Key, ConstraintKeyPrefix.Length, name, 0, name.Length,
                    StringComparison.OrdinalIgnoreCase) != 0)
            {
                continue;
            }

            // The lower-cased key is the one Spark reads, so it wins over a differently cased one.
            if (match is null || pair.Key == ConstraintKeyPrefix + name.ToLowerInvariant())
                match = pair;
        }
        return match;
    }

    /// <summary>
    /// The protocol that declares <c>checkConstraints</c>, or null when <paramref name="current"/> already does: a
    /// legacy protocol is raised to writer version 3, which implies it, and a table-features protocol gets the
    /// feature named. Measured against Spark 4.0 / Delta 4.0.0, which does the same.
    /// </summary>
    private static ProtocolAction? CheckConstraintsProtocol(ProtocolAction current)
    {
        if (current.MinWriterVersion >= 7)
            return UpgradeProtocolForWriterFeatures(current, [CheckConstraintsFeature]);
        if (current.MinWriterVersion >= 3)
            return null;
        return current with { MinWriterVersion = 3 };
    }

    /// <summary>
    /// The number of the table's live rows that <paramref name="predicate"/> is not TRUE for. Reads only the
    /// columns the constraint names; files whose statistics prove it TRUE for every row are skipped only under
    /// <see cref="DeltaTableOptions.TrustStatisticsProofs"/>, and only for a constraint the proof can evaluate
    /// everywhere (no function call), as DELETE's proof requires.
    /// </summary>
    private async ValueTask<long> CountConstraintViolationsAsync(
        Snapshot.Snapshot snapshot, Expressions.Predicate predicate, string description, string sql,
        CancellationToken cancellationToken)
    {
        var columns = SchemaChangeDependents.ReferencedTopLevelColumns(snapshot.Schema, description, sql);
        bool prove = _options.TrustStatisticsProofs && EvaluatesEverywhere(predicate, snapshot.Schema);
        var pruner = prove
            ? new DeltaFilePruner(snapshot.Schema, snapshot.Metadata.PartitionColumns, _options.PreferTypedCheckpointStats)
            : null;
        var partitionColumns = snapshot.Metadata.PartitionColumns;
        var partitionSchema = prove && partitionColumns.Count > 0
            ? BuildProjectedSchema(snapshot.ArrowSchema, partitionColumns)
            : null;
        var evaluator = new ArrowRowEvaluator(new SparkFunctionRegistry());
        var proofForm = prove ? ProofForm(predicate, snapshot.Schema) : predicate;

        long violations = 0;
        foreach (var addFile in snapshot.ActiveFiles.Values)
        {
            if (prove && ProvesEveryRow(proofForm, addFile, new DeltaFileStats(addFile), pruner!, partitionSchema, snapshot))
                continue;

            // A constraint that names no column (CHECK (1 = 1)) still has to see each row once.
            await foreach (var batch in ReadFileAsync(
                               addFile, columns.Count > 0 ? columns : null, snapshot, cancellationToken)
                               .ConfigureAwait(false))
            {
                violations += DeltaConstraintEnforcer.CountViolations(evaluator, predicate, batch, description, sql);
            }
        }
        return violations;
    }

    /// <summary>
    /// <paramref name="predicate"/> in the form the statistics proof can answer, for the proof alone: each column
    /// named as the schema names it (the statistics are keyed exactly), and an integer literal compared with an
    /// integer column given the type the column's bounds decode to, when its value fits that type. SQL reads
    /// <c>id &gt; 0</c> with an <c>int</c> literal, which the proof would otherwise decline against a
    /// <c>long</c> column. Spark compares two integers at the wider of their types, so a literal that fits the
    /// narrower one compares the same at either. Anything else is left as it was, and stays unproven if the proof
    /// declines it.
    /// </summary>
    internal static Expressions.Predicate ProofForm(Expressions.Predicate predicate, Schema.StructType schema)
    {
        return Rewrite(predicate);

        Expressions.Predicate Rewrite(Expressions.Predicate p) => p switch
        {
            Expressions.AndPredicate and => new Expressions.AndPredicate([.. and.Children.Select(Rewrite)]),
            Expressions.OrPredicate or => new Expressions.OrPredicate([.. or.Children.Select(Rewrite)]),
            Expressions.ComparisonPredicate { Left: Expressions.UnboundReference column, Right: Expressions.LiteralExpression literal } cmp
                => cmp with { Left = Named(column, out var type), Right = Fitted(literal, type) },
            Expressions.ComparisonPredicate { Left: Expressions.LiteralExpression literal, Right: Expressions.UnboundReference column } cmp
                => cmp with { Right = Named(column, out var type), Left = Fitted(literal, type) },
            Expressions.UnaryPredicate { Operand: Expressions.UnboundReference column } unary
                => unary with { Operand = Named(column, out _) },
            _ => p,
        };

        Expressions.UnboundReference Named(Expressions.UnboundReference column, out string? type)
        {
            type = null;
            if (column.NameParts.Count != 1)
                return column;
            var field = schema.Fields.Where(f => string.Equals(f.Name, column.Name, StringComparison.OrdinalIgnoreCase))
                .Take(2).ToList();
            if (field.Count != 1)
                return column;
            type = (field[0].Type as Schema.PrimitiveType)?.TypeName;
            return new Expressions.UnboundReference(field[0].Name);
        }

        static Expressions.LiteralExpression Fitted(Expressions.LiteralExpression literal, string? type)
        {
            var value = literal.Value;
            return (type, value.Type) switch
            {
                ("long", Expressions.LiteralValue.Kind.Int32)
                    => new Expressions.LiteralExpression(Expressions.LiteralValue.Of((long)value.AsInt32)),
                ("integer" or "short" or "byte", Expressions.LiteralValue.Kind.Int64)
                    when value.AsInt64 is >= int.MinValue and <= int.MaxValue
                    => new Expressions.LiteralExpression(Expressions.LiteralValue.Of((int)value.AsInt64)),
                _ => literal,
            };
        }
    }
}
