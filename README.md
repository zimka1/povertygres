# Povertygres – checklist

A toy SQL database engine written in Rust, inspired by PostgreSQL.  
Goal: implement core PostgreSQL architecture and algorithms.

---

## Current status

* [x] In-memory table storage (initial prototype)
* [x] Persistent catalog (`catalog.json`) with table definitions
* [x] Heap-file storage (one `.tbl` file per table)

  * [x] Page layout with fixed-size pages (8KB)
  * [x] `PageHeader`, `ItemId`, `TupleHeader`, `NullBitmap`
  * [x] Row serialization (`insert_tuple`)
  * [x] Row deserialization (`get_tuple`)
  * [x] Automatic page extension when out of space (`append_page`)
  * [x] Row deletion support (`delete_at` marks tuple as removed)
  * [x] Row update support (`update_row` re-inserts modified tuple)
  * [x] `scan_all_with_pos` to return `(page_no, slot_no, row)` for updates/deletes

* [x] Table constraints
  * [x] `PRIMARY KEY` (uniqueness + implicit `NOT NULL`)
  * [x] `NOT NULL` columns
  * [x] `DEFAULT` values
  * [x] `FOREIGN KEY` (validated on `INSERT`/`UPDATE`/`DELETE`, no cascade yet)

* [x] `CREATE TABLE` support (writes catalog + creates heap file)
* [x] `INSERT INTO` with/without column list (auto-fill missing columns with `NULL`, writes row into heap file)
* [x] `SELECT` with specific columns and `SELECT *` (reads rows from heap files)
* [x] `Value` types: `INT`, `TEXT`, `BOOL`, `NULL`
* [x] Pretty table output
* [x] Basic `WHERE` clause support

  * [x] Comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
  * [x] Logical operators: `AND`, `OR`, `NOT` (with short-circuit evaluation)
  * [x] Strict type checking (no implicit casts)
  * [x] Error handling for unknown columns, type mismatch, invalid operations

* [x] `DELETE FROM ... WHERE ...` support with row count return (heap-backed)
* [x] `UPDATE ... SET ... WHERE ...` support (heap-backed)
* [x] `JOIN` support

  * [x] `INNER JOIN` with `ON` conditions
  * [x] `LEFT JOIN` with `NULL` fill for unmatched rows

* [x] Indexes
  * [x] `CREATE INDEX` (single and composite keys)
  * [x] `BTreeIndex` structure backed by `BTreeMap`
  * [x] Index maintenance on `INSERT`, `UPDATE`, `DELETE`
  * [x] Index-based lookup for `SELECT`:
    * [x] Equality lookups (`col = value`, composite `col1 = v1 AND col2 = v2`)
    * [x] Range scans (`<`, `<=`, `>`, `>=`)
  * [x] Fallback to full table scan when no usable index is found

* [x] MVCC (multi-version concurrency control)
  * [x] `xmin` / `xmax` in `TupleHeader`
  * [x] `TransactionManager` assigns XIDs and tracks commit/rollback
  * [x] Visibility rules (`is_visible(xid, tm)`)
  * [x] `INSERT`: new tuple gets `xmin = xid`, visible only after commit
  * [x] `DELETE`: sets `xmax = xid` instead of physical removal
  * [x] `UPDATE`: old tuple gets `xmax = xid`, new version inserted with `xmin = xid`
  * [x] Rollback discards uncommitted versions
  * [x] Joins and scans return only visible versions (old + new until vacuum)
  * [x] `VACUUM` support:
    * [x] `TupleHeader::is_dead(tm)` detects tuples with committed `xmax`
    * [x] `HeapFile::vacuum` scans pages, reclaims slots of dead tuples
    * [x] Integrated `VACUUM table` command in executor
    * [x] Frees space for new inserts
    * [x] **Page compaction**: live tuples copied into fresh page layout, 
          dead tuples and gaps removed, ensuring contiguous free space
    * [x] **Index cleanup**: dangling entries removed from all BTree indexes
  * [x] **Snapshots & Isolation**
    * [x] `Snapshot` struct records `xmax` and active XIDs
    * [x] Each `BEGIN` captures a snapshot from `TransactionManager`
    * [x] `TupleHeader::is_visible(xid, snapshot, tm)` enforces snapshot rules
    * [x] **READ COMMITTED**: every statement uses a fresh snapshot, sees only committed rows at execution time
    * [x] **REPEATABLE READ**: all statements in a transaction share the same snapshot, guaranteeing stable view for the whole transaction

---

## Planned architecture

### Planner (AST → Logical Plan)
- [ ] Define `LogicalPlan` (projection, selection, scan)
- [ ] Convert `Query` to `LogicalPlan`

### Expression system
- [ ] `Expr` tree (`BinaryOp`, `Column`, `Literal`)
- [ ] Expression evaluation on rows

### Executor (PhysicalPlan → Rows)
- [ ] Define `PhysicalPlan`
- [ ] Sequential scan
- [ ] Filter execution
- [ ] Projection operator

---

## Storage Engine
* [ ] Storage trait (`Storage` abstraction over in-memory / heap files)
* [ ] Better row identifier (`RID { page_no, slot_no }`)

---

## Query Optimizer
- [ ] Cost model
- [ ] Plan rewriting rules
- [ ] Index usage decision
- [ ] Join ordering strategies

---

## Concurrency & Transactions
- [ ] Simple locks
- [ ] WAL (write-ahead logging)

---

## Testing & Tooling
- [ ] Integration tests (SQL scripts)
- [ ] Unit tests (planner, executor, expressions)
- [ ] Benchmarks
- [ ] CLI improvements (command history, syntax help)

---

## Interface
- [ ] SQL REPL with `rustyline`
- [ ] HTTP API for external access
- [ ] Web frontend (playground-style)

---

## Documentation
- [ ] Getting started guide
- [ ] Supported SQL syntax
- [ ] Internal architecture overview
- [ ] Dev guide (how to add new features)

---
