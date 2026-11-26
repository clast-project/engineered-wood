// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

// iceberg-go interop driver for the EngineeredWood Iceberg tests.
//
// It obeys the contract of test/Shared/InteropDriver.cs, minus the Python:
//
//	iceberg-go-driver <command> <args-file> <result-file>
//
// The args file holds one JSON object. The driver writes exactly one JSON object to the result file
// and exits 0, reporting a failure as {"ok": false, "error": ...} so the C# side asserts on the
// message instead of on a crash. `iceberg-go-driver version` prints {"v": ...} to stdout instead,
// and is the availability probe.
//
// Catalog: every table lives in a SQL catalog backed by sqlite (pure Go, through sqliteshim) at
// <warehouse>/catalog.db, namespace "ew". The Hadoop catalog cannot be used: in 0.7.0 and
// 0.8.0-rc0 its NewCatalog parses a "C:/..." warehouse as URL scheme "c", and turns
// "file:///C:/..." into "C:\C:\..." (see the spike on #953). EW tests therefore address tables by
// metadata-file location, which every command accepts.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/uptrace/bun/driver/sqliteshim"
)

const namespace = "ew"

var ctx = context.Background()

type result map[string]any

func main() {
	if len(os.Args) == 2 && os.Args[1] == "version" {
		out, _ := json.Marshal(map[string]string{"v": icebergGoVersion(), "go": runtime.Version()})
		fmt.Println(string(out))
		return
	}
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: iceberg-go-driver <command> <args-file> <result-file> | version")
		os.Exit(2)
	}

	command, argsFile, resultFile := os.Args[1], os.Args[2], os.Args[3]
	res := run(command, argsFile)
	data, err := json.Marshal(res)
	if err != nil {
		data, _ = json.Marshal(result{"ok": false, "error": "marshal result: " + err.Error()})
	}
	if err := os.WriteFile(resultFile, data, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write result:", err)
		os.Exit(1)
	}
}

func run(command, argsFile string) (res result) {
	defer func() {
		if r := recover(); r != nil {
			res = result{"ok": false, "error": fmt.Sprintf("panic: %v\n%s", r, debug.Stack())}
		}
	}()

	raw, err := os.ReadFile(argsFile)
	if err != nil {
		return fail(err)
	}
	var args map[string]any
	if err := json.Unmarshal(raw, &args); err != nil {
		return fail(fmt.Errorf("parse args: %w", err))
	}

	var out result
	switch command {
	case "scenario":
		out, err = runScenario(args)
	case "scan":
		out, err = scan(args)
	case "describe":
		out, err = describe(args)
	case "append":
		out, err = appendRows(args)
	case "delete":
		out, err = deleteRows(args)
	default:
		err = fmt.Errorf("unknown command %q", command)
	}
	if err != nil {
		return fail(err)
	}
	out["ok"] = true
	return out
}

func fail(err error) result { return result{"ok": false, "error": err.Error()} }

// icebergGoVersion is the iceberg-go module version this binary was built against, from go.mod.
func icebergGoVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == "github.com/apache/iceberg-go" {
				return dep.Version
			}
		}
	}
	return "unknown"
}

// --- arguments -------------------------------------------------------------------------------

func str(args map[string]any, key string) (string, error) {
	v, ok := args[key].(string)
	if !ok || v == "" {
		return "", fmt.Errorf("missing string argument %q", key)
	}
	return v, nil
}

func boolArg(args map[string]any, key string) bool {
	v, _ := args[key].(bool)
	return v
}

// fileURI turns a local path into the file:///C:/... form iceberg-go's local IO accepts on Windows.
// Anything that already has a scheme is returned unchanged.
func fileURI(path string) (string, error) {
	if strings.Contains(path, "://") {
		return path, nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	slashed := filepath.ToSlash(abs)
	if !strings.HasPrefix(slashed, "/") {
		slashed = "/" + slashed
	}
	return (&url.URL{Scheme: "file", Path: slashed}).String(), nil
}

// --- catalog ---------------------------------------------------------------------------------

func openCatalog(warehouse string) (*sqlcat.Catalog, error) {
	if err := os.MkdirAll(warehouse, 0o755); err != nil {
		return nil, err
	}
	whURI, err := fileURI(warehouse)
	if err != nil {
		return nil, err
	}
	cat, err := catalog.Load(ctx, "ew", iceberg.Properties{
		"type":                "sql",
		sqlcat.DriverKey:      sqliteshim.ShimName,
		sqlcat.DialectKey:     "sqlite",
		"uri":                 "file:" + filepath.Join(warehouse, "catalog.db"),
		"warehouse":           whURI,
		"init_catalog_tables": "true",
	})
	if err != nil {
		return nil, fmt.Errorf("load sql catalog: %w", err)
	}
	sc, ok := cat.(*sqlcat.Catalog)
	if !ok {
		return nil, fmt.Errorf("catalog is %T, not *sql.Catalog", cat)
	}
	exists, err := sc.CheckNamespaceExists(ctx, table.Identifier{namespace})
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := sc.CreateNamespace(ctx, table.Identifier{namespace}, nil); err != nil {
			return nil, fmt.Errorf("create namespace: %w", err)
		}
	}
	return sc, nil
}

// register adopts an existing table, addressed by its metadata file, into the sqlite catalog so
// iceberg-go can commit to it through its real SQL-catalog code path. The SQL catalog has no
// RegisterTable, so the row goes in directly; it is the same row CreateTable writes.
func register(warehouse, name, metadataLocation string) (*table.Table, *sqlcat.Catalog, error) {
	cat, err := openCatalog(warehouse)
	if err != nil {
		return nil, nil, err
	}
	db, err := sql.Open(sqliteshim.ShimName, "file:"+filepath.Join(warehouse, "catalog.db"))
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, iceberg_type, metadata_location)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (catalog_name, table_namespace, table_name)
		 DO UPDATE SET metadata_location = excluded.metadata_location, previous_metadata_location = NULL`,
		"ew", namespace, name, sqlcat.TableType, metadataLocation); err != nil {
		return nil, nil, fmt.Errorf("register table: %w", err)
	}
	tbl, err := cat.LoadTable(ctx, table.Identifier{namespace, name})
	if err != nil {
		return nil, nil, fmt.Errorf("load registered table: %w", err)
	}
	return tbl, cat, nil
}

// loadByLocation reads a table from its metadata file without any catalog: the read-only path,
// usable on tables iceberg-go did not write.
func loadByLocation(args map[string]any) (*table.Table, error) {
	loc, err := str(args, "metadata_location")
	if err != nil {
		return nil, err
	}
	if loc, err = fileURI(loc); err != nil {
		return nil, err
	}
	return table.NewFromLocation(ctx, table.Identifier{namespace, "loaded"}, loc,
		iceio.LoadFSFunc(nil, loc), nil)
}

// --- scenarios -------------------------------------------------------------------------------

// The scenarios are the ones the #953 spike validated against Spark 4.1 + Iceberg 1.12. Each
// writes one table, ew.<scenario>, and records what it did in "steps".
//
// Schema: id long required (1), name string (2), ts timestamptz_ns (3) - or timestamptz (micros)
// for v2 tables, which have no nanosecond type. Row i has name "n<i>" and ts = 1.7e18 ns + i days
// + 123 ns, so the nanosecond part survives only a nanosecond-preserving reader.

type scenarioState struct {
	cat   *sqlcat.Catalog
	tbl   *table.Table
	nanos bool
	steps []string
}

func (s *scenarioState) step(what string, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	s.steps = append(s.steps, what)
	return nil
}

func runScenario(args map[string]any) (result, error) {
	warehouse, err := str(args, "warehouse")
	if err != nil {
		return nil, err
	}
	name, err := str(args, "scenario")
	if err != nil {
		return nil, err
	}
	cat, err := openCatalog(warehouse)
	if err != nil {
		return nil, err
	}
	s := &scenarioState{cat: cat, nanos: true}

	switch name {
	case "v3_dv":
		err = scenarioV3DV(s)
	case "v3_part":
		err = scenarioV3Part(s)
	case "v2_upgrade":
		err = scenarioV2Upgrade(s)
	case "v3_cow":
		err = scenarioV3CoW(s)
	case "v3_eqdel":
		err = scenarioV3EqDel(s)
	default:
		err = fmt.Errorf("unknown scenario %q", name)
	}
	if err != nil {
		return nil, err
	}
	return result{
		"metadata_location": s.tbl.MetadataLocation(),
		"table_location":    s.tbl.Location(),
		"steps":             s.steps,
	}, nil
}

func (s *scenarioState) schema() *iceberg.Schema {
	ts := iceberg.PrimitiveTypes.TimestampTz
	if s.nanos {
		ts = iceberg.PrimitiveTypes.TimestampTzNs
	}
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "ts", Type: ts},
	)
}

func (s *scenarioState) create(name, formatVersion, mode string, spec *iceberg.PartitionSpec) error {
	props := iceberg.Properties{"format-version": formatVersion}
	if mode != "" {
		props["write.delete.mode"] = mode
		props["write.update.mode"] = mode
		props["write.merge.mode"] = mode
	}
	opts := []catalog.CreateTableOpt{catalog.WithProperties(props)}
	if spec != nil {
		opts = append(opts, catalog.WithPartitionSpec(spec))
	}
	tbl, err := s.cat.CreateTable(ctx, table.Identifier{namespace, name}, s.schema(), opts...)
	s.tbl = tbl
	return s.step(fmt.Sprintf("create v%s %s", formatVersion, modeName(mode)), err)
}

func modeName(mode string) string {
	if mode == "" {
		return "copy-on-write"
	}
	return mode
}

func (s *scenarioState) batch(from, n int64) arrow.Table {
	unit := arrow.Microsecond
	if s.nanos {
		unit = arrow.Nanosecond
	}
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: unit, TimeZone: "UTC"}, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	for i := from; i < from+n; i++ {
		b.Field(0).(*array.Int64Builder).Append(i)
		b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("n%d", i))
		ns := int64(1_700_000_000_000_000_000) + i*86_400_000_000_000 + 123
		if !s.nanos {
			ns /= 1000
		}
		b.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(ns))
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	return array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
}

func (s *scenarioState) append(from, n int64) error {
	t := s.batch(from, n)
	defer t.Release()
	tbl, err := s.tbl.AppendTable(ctx, t, n, nil)
	if err == nil {
		s.tbl = tbl
	}
	return s.step(fmt.Sprintf("append ids %d..%d", from, from+n-1), err)
}

func (s *scenarioState) delete(what string, filter iceberg.BooleanExpression) error {
	tbl, err := s.tbl.Delete(ctx, filter, nil)
	if err == nil {
		s.tbl = tbl
	}
	return s.step("delete "+what, err)
}

func (s *scenarioState) commit(what string, tx *table.Transaction) error {
	tbl, err := tx.Commit(ctx)
	if err == nil {
		s.tbl = tbl
	}
	return s.step(what, err)
}

func id() iceberg.UnboundTerm { return iceberg.Reference("id") }

// v3_dv: two merge-on-read deletes, the second merging into the first one's DV, then a column
// added with an initial default of 42.
func scenarioV3DV(s *scenarioState) error {
	return firstErr(
		func() error { return s.create("v3_dv", "3", "merge-on-read", nil) },
		func() error { return s.append(0, 10) },
		func() error { return s.delete("id < 3", iceberg.LessThan(id(), int64(3))) },
		func() error { return s.delete("id = 7", iceberg.EqualTo(id(), int64(7))) },
		func() error {
			tx := s.tbl.NewTransaction()
			if err := tx.UpdateSchema(true, false).
				AddColumn([]string{"score"}, iceberg.PrimitiveTypes.Int32, "", false, iceberg.NewLiteral(int32(42))).
				Commit(); err != nil {
				return fmt.Errorf("add column score: %w", err)
			}
			return s.commit("add column score int default 42", tx)
		},
	)
}

// v3_part: bucket[4](id) and day(ts), then a merge-on-read delete across partitions.
func scenarioV3Part(s *scenarioState) error {
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_bucket", Transform: iceberg.BucketTransform{NumBuckets: 4}},
		iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1001, Name: "ts_day", Transform: iceberg.DayTransform{}},
	)
	return firstErr(
		func() error { return s.create("v3_part", "3", "merge-on-read", &spec) },
		func() error { return s.append(0, 8) },
		func() error { return s.delete("id in (1, 5)", iceberg.IsIn(id(), int64(1), int64(5))) },
	)
}

// v2_upgrade: a v2 merge-on-read delete (a position-delete file), the upgrade to v3, then an
// append and a delete whose DV must absorb the v2 position delete. iceberg-go 0.7.0 failed this:
// the deleted id 2 came back (apache/iceberg-go#2669, fixed in 0.8.0-rc0).
func scenarioV2Upgrade(s *scenarioState) error {
	s.nanos = false
	return firstErr(
		func() error { return s.create("v2_upgrade", "2", "merge-on-read", nil) },
		func() error { return s.append(0, 6) },
		func() error { return s.delete("id = 2 (v2 position delete)", iceberg.EqualTo(id(), int64(2))) },
		func() error {
			tx := s.tbl.NewTransaction()
			if err := tx.UpgradeFormatVersion(3); err != nil {
				return fmt.Errorf("upgrade to v3: %w", err)
			}
			return s.commit("upgrade to v3", tx)
		},
		func() error { return s.append(100, 3) },
		func() error { return s.delete("id = 3 (v3 DV)", iceberg.EqualTo(id(), int64(3))) },
	)
}

// v3_cow: a copy-on-write delete; the rewritten file must carry each surviving row's _row_id.
func scenarioV3CoW(s *scenarioState) error {
	return firstErr(
		func() error { return s.create("v3_cow", "3", "", nil) },
		func() error { return s.append(0, 5) },
		func() error { return s.delete("id = 1 (copy-on-write)", iceberg.EqualTo(id(), int64(1))) },
	)
}

// v3_eqdel: an equality delete on id.
func scenarioV3EqDel(s *scenarioState) error {
	return firstErr(
		func() error { return s.create("v3_eqdel", "3", "merge-on-read", nil) },
		func() error { return s.append(0, 5) },
		func() error {
			del := s.batch(3, 1)
			defer del.Release()
			idSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
			tr := array.NewTableReader(del, 10)
			defer tr.Release()
			records := func(yield func(arrow.RecordBatch, error) bool) {
				for tr.Next() {
					rec := tr.RecordBatch()
					if !yield(array.NewRecordBatch(idSchema, []arrow.Array{rec.Column(0)}, rec.NumRows()), nil) {
						return
					}
				}
			}
			tx := s.tbl.NewTransaction()
			if err := tx.CommitEqualityDeletes(ctx, []int{1}, records, nil); err != nil {
				return fmt.Errorf("write equality delete: %w", err)
			}
			return s.commit("equality delete id = 3", tx)
		},
	)
}

func firstErr(steps ...func() error) error {
	for _, f := range steps {
		if err := f(); err != nil {
			return err
		}
	}
	return nil
}

// --- reads -----------------------------------------------------------------------------------

// scan reads a table's current rows. With "row_lineage": true it adds _row_id and
// _last_updated_sequence_number. Values are JSON-native where the type allows (integers, strings,
// booleans, floats); timestamps are integers in the column's own unit, so nanoseconds survive.
func scan(args map[string]any) (result, error) {
	tbl, err := loadByLocation(args)
	if err != nil {
		return nil, err
	}
	var opts []table.ScanOption
	if boolArg(args, "row_lineage") {
		opts = append(opts, table.WithRowLineage())
	}
	at, err := tbl.Scan(opts...).ToArrowTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	defer at.Release()

	columns := []map[string]any{}
	for _, f := range at.Schema().Fields() {
		columns = append(columns, map[string]any{"name": f.Name, "type": f.Type.String()})
	}
	rows := [][]any{}
	tr := array.NewTableReader(at, 4096)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			row := make([]any, rec.NumCols())
			for c := 0; c < int(rec.NumCols()); c++ {
				row[c] = jsonValue(rec.Column(c), r)
			}
			rows = append(rows, row)
		}
	}
	if err := tr.Err(); err != nil {
		return nil, err
	}
	return result{"columns": columns, "rows": rows}, nil
}

func jsonValue(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}
	switch a := col.(type) {
	case *array.Int64:
		return a.Value(i)
	case *array.Int32:
		return a.Value(i)
	case *array.Int16:
		return a.Value(i)
	case *array.Int8:
		return a.Value(i)
	case *array.Boolean:
		return a.Value(i)
	case *array.Float64:
		return a.Value(i)
	case *array.Float32:
		return a.Value(i)
	case *array.String:
		return a.Value(i)
	case *array.LargeString:
		return a.Value(i)
	case *array.Timestamp:
		return int64(a.Value(i))
	case *array.Date32:
		return int32(a.Value(i))
	default:
		return col.ValueStr(i)
	}
}

// describe reports iceberg-go's view of a table's metadata, the current snapshot's manifests and
// every entry in them (DELETED entries included, so a test can check what a commit retired), and
// the scan plan: which delete files and DVs iceberg-go applies to each data file.
func describe(args map[string]any) (result, error) {
	tbl, err := loadByLocation(args)
	if err != nil {
		return nil, err
	}
	md := tbl.Metadata()
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}

	out := result{
		"metadata_location":    tbl.MetadataLocation(),
		"format_version":       md.FormatVersion(),
		"table_uuid":           md.TableUUID().String(),
		"location":             md.Location(),
		"last_sequence_number": md.LastSequenceNumber(),
		"current_schema_id":    md.CurrentSchema().ID,
		"default_spec_id":      md.DefaultPartitionSpec(),
		"last_column_id":       md.LastColumnID(),
	}
	if md.FormatVersion() >= 3 {
		out["next_row_id"] = md.NextRowID()
	}

	snapshots := []map[string]any{}
	for _, s := range md.Snapshots() {
		snapshots = append(snapshots, snapshotJSON(s))
	}
	out["snapshots"] = snapshots

	cur := md.CurrentSnapshot()
	if cur == nil {
		out["current_snapshot_id"] = nil
		out["manifests"] = []any{}
		out["tasks"] = []any{}
		return out, nil
	}
	out["current_snapshot_id"] = cur.SnapshotID

	manifests, err := cur.Manifests(ctx, fs)
	if err != nil {
		return nil, fmt.Errorf("read manifest list: %w", err)
	}
	ms := []map[string]any{}
	for _, m := range manifests {
		entries := []map[string]any{}
		for e, err := range m.Entries(ctx, fs, false) {
			if err != nil {
				return nil, fmt.Errorf("read manifest %s: %w", m.FilePath(), err)
			}
			entries = append(entries, entryJSON(e))
		}
		ms = append(ms, map[string]any{
			"path":                m.FilePath(),
			"length":              m.Length(),
			"content":             contentName(int(m.ManifestContent())),
			"partition_spec_id":   m.PartitionSpecID(),
			"sequence_number":     m.SequenceNumber(),
			"min_sequence_number": m.MinSequenceNumber(),
			"added_snapshot_id":   m.SnapshotID(),
			"added_files":         m.AddedDataFiles(),
			"existing_files":      m.ExistingDataFiles(),
			"deleted_files":       m.DeletedDataFiles(),
			"added_rows":          m.AddedRows(),
			"existing_rows":       m.ExistingRows(),
			"deleted_rows":        m.DeletedRows(),
			"first_row_id":        m.FirstRowID(),
			"entries":             entries,
		})
	}
	out["manifests"] = ms

	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("plan files: %w", err)
	}
	ts := []map[string]any{}
	for _, t := range tasks {
		ts = append(ts, map[string]any{
			"file":             t.File.FilePath(),
			"position_deletes": paths(t.DeleteFiles),
			"equality_deletes": paths(t.EqualityDeleteFiles),
			"deletion_vectors": paths(t.DeletionVectorFiles),
		})
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i]["file"].(string) < ts[j]["file"].(string) })
	out["tasks"] = ts
	return out, nil
}

func snapshotJSON(s table.Snapshot) map[string]any {
	m := map[string]any{
		"snapshot_id":     s.SnapshotID,
		"parent_id":       s.ParentSnapshotID,
		"sequence_number": s.SequenceNumber,
		"timestamp_ms":    s.TimestampMs,
		"manifest_list":   s.ManifestList,
		"schema_id":       s.SchemaID,
		"first_row_id":    s.FirstRowID,
		"added_rows":      s.AddedRows,
	}
	if s.Summary != nil {
		summary := map[string]string{"operation": string(s.Summary.Operation)}
		for k, v := range s.Summary.Properties {
			summary[k] = v
		}
		m["summary"] = summary
	}
	return m
}

func entryJSON(e iceberg.ManifestEntry) map[string]any {
	df := e.DataFile()
	partition := map[string]any{}
	for k, v := range df.Partition() {
		partition[fmt.Sprint(k)] = fmt.Sprint(v)
	}
	return map[string]any{
		"status":                statusName(int(e.Status())),
		"snapshot_id":           e.SnapshotID(),
		"data_sequence_number":  e.DataSequenceNumber(),
		"file_sequence_number":  e.FileSequenceNumber(),
		"content":               fileContentName(int(df.ContentType())),
		"file_path":             df.FilePath(),
		"file_format":           string(df.FileFormat()),
		"spec_id":               df.SpecID(),
		"record_count":          df.Count(),
		"file_size_in_bytes":    df.FileSizeBytes(),
		"first_row_id":          df.FirstRowID(),
		"referenced_data_file":  df.ReferencedDataFile(),
		"content_offset":        df.ContentOffset(),
		"content_size_in_bytes": df.ContentSizeInBytes(),
		"equality_ids":          df.EqualityFieldIDs(),
		"partition":             partition,
	}
}

func paths(files []iceberg.DataFile) []string {
	out := []string{}
	for _, f := range files {
		out = append(out, f.FilePath())
	}
	sort.Strings(out)
	return out
}

func statusName(s int) string {
	return [...]string{"EXISTING", "ADDED", "DELETED"}[s]
}

func contentName(c int) string {
	return [...]string{"DATA", "DELETES"}[c]
}

func fileContentName(c int) string {
	return [...]string{"DATA", "POSITION_DELETES", "EQUALITY_DELETES"}[c]
}

// --- writes to an existing table --------------------------------------------------------------

// appendRows registers the table addressed by metadata_location under the name "table" (default:
// "adopted") and appends "rows", a list of JSON objects keyed by column name, converted with the
// table's own schema. Returns the new metadata location.
func appendRows(args map[string]any) (result, error) {
	tbl, err := adopt(args)
	if err != nil {
		return nil, err
	}
	rowsJSON, err := json.Marshal(args["rows"])
	if err != nil {
		return nil, err
	}
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	if err != nil {
		return nil, err
	}
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(string(rowsJSON)))
	if err != nil {
		return nil, fmt.Errorf("rows -> arrow: %w", err)
	}
	defer rec.Release()
	at := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	defer at.Release()
	next, err := tbl.AppendTable(ctx, at, max(rec.NumRows(), 1), nil)
	if err != nil {
		return nil, fmt.Errorf("append: %w", err)
	}
	return result{"metadata_location": next.MetadataLocation()}, nil
}

// deleteRows registers the table like appendRows and deletes the rows where "column" equals any of
// "values" (integers). The table's write.delete.mode decides between DVs and a rewrite.
func deleteRows(args map[string]any) (result, error) {
	tbl, err := adopt(args)
	if err != nil {
		return nil, err
	}
	column, err := str(args, "column")
	if err != nil {
		return nil, err
	}
	raw, _ := args["values"].([]any)
	if len(raw) == 0 {
		return nil, errors.New(`missing "values"`)
	}
	values := make([]int64, len(raw))
	for i, v := range raw {
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("value %v is not a number", v)
		}
		values[i] = int64(f)
	}
	next, err := tbl.Delete(ctx, iceberg.IsIn(iceberg.Reference(column), values...), nil)
	if err != nil {
		return nil, fmt.Errorf("delete: %w", err)
	}
	return result{"metadata_location": next.MetadataLocation()}, nil
}

func adopt(args map[string]any) (*table.Table, error) {
	warehouse, err := str(args, "warehouse")
	if err != nil {
		return nil, err
	}
	loc, err := str(args, "metadata_location")
	if err != nil {
		return nil, err
	}
	if loc, err = fileURI(loc); err != nil {
		return nil, err
	}
	name, _ := args["table"].(string)
	if name == "" {
		name = "adopted"
	}
	tbl, _, err := register(warehouse, name, loc)
	return tbl, err
}
