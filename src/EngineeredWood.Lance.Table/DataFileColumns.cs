// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Lance.Format;
using EngineeredWood.Lance.Table.Proto;
using LanceField = EngineeredWood.Lance.Proto.Field;

namespace EngineeredWood.Lance.Table;

/// <summary>
/// Maps a dataset's top-level fields to the physical columns of one fragment's data file (#995).
///
/// <para>A data file need not hold the manifest's fields in the manifest's order. <c>drop_columns</c> changes only
/// the manifest, so after dropping <c>a</c> from <c>[a, b, c]</c> the file still holds three columns and the
/// manifest's field 0 (<c>b</c>) is the file's column 1. The data file's <c>fields</c> and <c>column_indices</c>
/// say which column holds which field id, and this reads through them, as lance-rs does.</para>
///
/// <para>What <c>fields</c> lists depends on the file version (measured with pylance 14.0.3): a 2.0 file lists
/// every field, struct and list parents included, each with its own column; a 2.1+ file lists only leaf fields,
/// and lance-rs refuses a 2.1+ data file that gives a non-leaf field a column.</para>
///
/// <para>A field held by no column of this file (one added by <c>add_columns</c>, which pylance reads as nulls,
/// or one stored in another file of a column-split fragment) is refused with <see cref="NotSupportedException"/>,
/// as is a field whose columns are not contiguous (a nested child dropped from a 2.0 file), which the decoder
/// cannot read.</para>
/// </summary>
internal sealed class DataFileColumns
{
    private readonly ulong _fragmentId;
    private readonly DataFile _file;
    private readonly LanceFileReader _reader;
    private readonly IReadOnlyList<LanceField> _roots;
    private readonly Dictionary<int, List<LanceField>> _children;
    private readonly Dictionary<int, LanceField> _byId = new();
    private readonly Dictionary<int, int>? _columnOfField;
    private readonly bool _leavesOnly;

    private DataFileColumns(
        IReadOnlyList<LanceField> manifestFields, ulong fragmentId, DataFile file, LanceFileReader reader)
    {
        _fragmentId = fragmentId;
        _file = file;
        _reader = reader;
        (_roots, _children) = BuildTree(manifestFields);
        foreach (var f in manifestFields) _byId[f.Id] = f;
        _leavesOnly = reader.Version.IsV2_1 || reader.Version.IsV2_2;

        // EW before #995 wrote a 2.1 file's fields as every field of the file in order, with identity
        // column_indices, parents included. Those indices are wrong for a nested schema (a struct's first child
        // is column 0 of the struct, not column 1) and lance-rs refuses them, so such a file is read through its
        // own schema instead, as EW always read it. An empty column_indices is read the same way.
        if (file.ColumnIndices.Count != 0 && !IsLegacyEwLayout(file, reader))
        {
            if (file.ColumnIndices.Count != file.Fields.Count)
                throw new LanceTableFormatException(
                    $"The data file of fragment {fragmentId} lists {file.Fields.Count} fields but "
                    + $"{file.ColumnIndices.Count} column indices.");
            _columnOfField = new Dictionary<int, int>(file.Fields.Count);
            for (int i = 0; i < file.Fields.Count; i++)
            {
                // A negative field id is a tombstone (a field rewritten into another file of the fragment); a
                // negative column index marks a field with no column of its own.
                if (file.Fields[i] < 0 || file.ColumnIndices[i] < 0) continue;
                if (_columnOfField.ContainsKey(file.Fields[i]))
                    throw new LanceTableFormatException(
                        $"The data file of fragment {fragmentId} lists field {file.Fields[i]} twice.");
                _columnOfField[file.Fields[i]] = file.ColumnIndices[i];
            }
        }
    }

    /// <summary>Maps the manifest's fields to the columns of <paramref name="file"/>, open in
    /// <paramref name="reader"/>.</summary>
    public static DataFileColumns Create(
        IReadOnlyList<LanceField> manifestFields, ulong fragmentId, DataFile file, LanceFileReader reader)
        => new(manifestFields, fragmentId, file, reader);

    /// <summary>
    /// Reads the manifest's top-level field <paramref name="topLevelIndex"/>, whose Arrow form (from the manifest
    /// schema) is <paramref name="arrowField"/>.
    /// </summary>
    public Task<IArrowArray> ReadAsync(int topLevelIndex, Field arrowField, CancellationToken cancellationToken)
    {
        LanceField root = _roots[topLevelIndex];
        if (_columnOfField is null)
            return ReadThroughFileSchemaAsync(root, cancellationToken);

        var needed = new List<LanceField>();
        CollectColumnFields(root, needed);
        if (needed.Count == 0)
            throw new NotSupportedException(
                $"Field '{root.Name}' of fragment {_fragmentId} has no columns to read.");

        int start = -1;
        for (int i = 0; i < needed.Count; i++)
        {
            LanceField f = needed[i];
            if (!_columnOfField.TryGetValue(f.Id, out int column))
                throw new NotSupportedException(
                    $"Field '{PathOf(f)}' (id {f.Id}) is not in the data file of fragment {_fragmentId}, which holds "
                    + $"fields [{string.Join(", ", _file.Fields)}]. A field added by add_columns without data, or "
                    + "held in another data file of the fragment, cannot be read.");
            if (i == 0)
                start = column;
            else if (column != start + i)
                throw new NotSupportedException(
                    $"Field '{root.Name}' of fragment {_fragmentId} is not stored in contiguous columns (field "
                    + $"'{PathOf(f)}' is column {column}, not {start + i}), as when a nested field is dropped from a "
                    + "2.0 data file; it cannot be read.");
        }

        return _reader.ReadFieldAtColumnsAsync(arrowField, start, needed.Count, cancellationToken);
    }

    /// <summary>Reads a legacy EW data file's field by its id in the file's own schema.</summary>
    private Task<IArrowArray> ReadThroughFileSchemaAsync(LanceField root, CancellationToken cancellationToken)
    {
        var (fileRoots, fileChildren) = BuildTree(_reader.ProtoSchema.Fields);
        for (int i = 0; i < fileRoots.Count; i++)
        {
            if (fileRoots[i].Id != root.Id) continue;
            var expected = new List<int>();
            CollectIds(root, _children, expected);
            var actual = new List<int>();
            CollectIds(fileRoots[i], fileChildren, actual);
            if (!expected.SequenceEqual(actual))
                throw new NotSupportedException(
                    $"Field '{root.Name}' of fragment {_fragmentId} has fields [{string.Join(", ", expected)}] in "
                    + $"the manifest but [{string.Join(", ", actual)}] in its data file, which lists no column "
                    + "indices to map them by.");
            return _reader.ReadColumnAsync(i, cancellationToken);
        }
        throw new NotSupportedException(
            $"Field '{root.Name}' (id {root.Id}) is not in the data file of fragment {_fragmentId}.");
    }

    /// <summary>The fields of <paramref name="field"/>'s subtree that have a column in this file's version, in
    /// column order: every field in a 2.0 file, only the leaves in a 2.1+ file.</summary>
    private void CollectColumnFields(LanceField field, List<LanceField> sink)
    {
        bool hasChildren = _children.TryGetValue(field.Id, out var children);
        if (!_leavesOnly || !hasChildren) sink.Add(field);
        if (hasChildren)
            foreach (var child in children!) CollectColumnFields(child, sink);
    }

    private static void CollectIds(LanceField field, Dictionary<int, List<LanceField>> children, List<int> sink)
    {
        sink.Add(field.Id);
        if (children.TryGetValue(field.Id, out var list))
            foreach (var child in list) CollectIds(child, children, sink);
    }

    private string PathOf(LanceField field)
    {
        var names = new List<string> { field.Name };
        for (int parent = field.ParentId; _byId.TryGetValue(parent, out var p) && names.Count <= _byId.Count;
             parent = p.ParentId)
            names.Add(p.Name);
        names.Reverse();
        return string.Join(".", names);
    }

    /// <summary>Whether <paramref name="file"/> is a pre-#995 EW 2.1+ data file: its fields are its own schema's
    /// field ids in order, parents included, with identity column indices. A flat schema reads the same either
    /// way, so only a schema with a non-leaf field counts.</summary>
    private static bool IsLegacyEwLayout(DataFile file, LanceFileReader reader)
    {
        if (!(reader.Version.IsV2_1 || reader.Version.IsV2_2)) return false;
        var own = reader.ProtoSchema.Fields;
        if (file.Fields.Count != own.Count || file.ColumnIndices.Count != own.Count) return false;
        var ids = new HashSet<int>();
        for (int i = 0; i < own.Count; i++)
        {
            if (file.Fields[i] != own[i].Id || file.ColumnIndices[i] != i) return false;
            ids.Add(own[i].Id);
        }
        foreach (var f in own)
            if (ids.Contains(f.ParentId)) return true;
        return false;
    }

    /// <summary>Reshapes flat Lance fields into roots and children, as <c>LanceSchemaConverter</c> does: a field
    /// is a root when its parent id names no field.</summary>
    internal static (IReadOnlyList<LanceField> Roots, Dictionary<int, List<LanceField>> Children) BuildTree(
        IEnumerable<LanceField> fields)
    {
        var list = fields as IReadOnlyCollection<LanceField> ?? fields.ToList();
        var ids = new HashSet<int>(list.Select(f => f.Id));
        var roots = new List<LanceField>();
        var children = new Dictionary<int, List<LanceField>>();
        foreach (var f in list)
        {
            if (!ids.Contains(f.ParentId)) { roots.Add(f); continue; }
            if (!children.TryGetValue(f.ParentId, out var c))
                children[f.ParentId] = c = new List<LanceField>();
            c.Add(f);
        }
        return (roots, children);
    }

    /// <summary>
    /// The ids of the manifest's fields in the order a <see cref="LanceFileWriter"/> writing the manifest's
    /// Arrow schema lists them (depth first, each parent before its children). Checked against the file writer's
    /// own fields by name and parent, so a file that does not match the manifest's shape fails here, not on read.
    /// </summary>
    internal static int[] ManifestIdsFor(IEnumerable<LanceField> manifestFields, IReadOnlyList<LanceField> fileFields)
    {
        var (roots, children) = BuildTree(manifestFields);
        var order = new List<LanceField>();
        void Visit(LanceField f)
        {
            order.Add(f);
            if (children.TryGetValue(f.Id, out var list))
                foreach (var c in list) Visit(c);
        }
        foreach (var r in roots) Visit(r);

        var fileIndexOfId = new Dictionary<int, int>(fileFields.Count);
        for (int i = 0; i < fileFields.Count; i++) fileIndexOfId[fileFields[i].Id] = i;
        var manifestIndexOfId = new Dictionary<int, int>(order.Count);
        for (int i = 0; i < order.Count; i++) manifestIndexOfId[order[i].Id] = i;

        bool same = order.Count == fileFields.Count;
        for (int i = 0; same && i < order.Count; i++)
        {
            bool manifestRoot = !manifestIndexOfId.TryGetValue(order[i].ParentId, out int mParent);
            bool fileRoot = !fileIndexOfId.TryGetValue(fileFields[i].ParentId, out int fParent);
            same = string.Equals(order[i].Name, fileFields[i].Name, StringComparison.Ordinal)
                && manifestRoot == fileRoot && (manifestRoot || mParent == fParent);
        }
        if (!same)
            throw new InvalidOperationException(
                $"The written file's fields [{string.Join(", ", fileFields.Select(f => f.Name))}] do not match the "
                + $"manifest's fields [{string.Join(", ", order.Select(f => f.Name))}].");
        return order.Select(f => f.Id).ToArray();
    }

    /// <summary>
    /// Lists <paramref name="fileFields"/> (a written file's schema, in its order) in <paramref name="dataFile"/>'s
    /// <c>fields</c>/<c>column_indices</c> as lance-rs expects for <paramref name="version"/>: in 2.0 every field
    /// with its own column; in 2.1+ only the leaves, numbered in order, since there a struct or list parent has no
    /// column and lance-rs refuses a data file that gives it one.
    /// </summary>
    internal static void AddFields(DataFile dataFile, IReadOnlyList<LanceField> fileFields, LanceVersion version)
    {
        bool leavesOnly = version.IsV2_1 || version.IsV2_2;
        var parents = new HashSet<int>(fileFields.Select(f => f.ParentId));
        int column = 0;
        foreach (var f in fileFields)
        {
            if (leavesOnly && parents.Contains(f.Id)) continue;
            dataFile.Fields.Add(f.Id);
            dataFile.ColumnIndices.Add(column++);
        }
    }
}
