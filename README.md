# Povertygres – checklist

A toy SQL database engine written in Rust, inspired by PostgreSQL.  
Goal: implement core PostgreSQL architecture and algorithms.

---

## Current status

- [x] In-memory table storage
- [x] `CREATE TABLE` support
- [x] `INSERT INTO` with/without column list (auto-fill missing columns with `NULL`)
- [x] `SELECT` with specific columns and `SELECT *`
- [x] `Value` types: `INT`, `TEXT`, `BOOL`, `NULL`
- [x] Pretty table output
- [x] Basic `WHERE` clause support
  - [x] Comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
  - [x] Logical operators: `AND`, `OR`, `NOT` (with short-circuit evaluation)
  - [x] Strict type checking (no implicit casts)
  - [x] Error handling for unknown columns, type mismatch, invalid operations
---

## Planned architecture

### Parsing (SQL → AST)
- [ ] Extend parser with `UPDATE`, `DELETE`, `JOIN`

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
- [ ] `Storage` trait (abstraction layer)
- [ ] In-memory implementation
- [ ] File-based storage (1 file per table)
- [ ] Page layout (fixed-size pages, e.g. 4KB)
- [ ] Row serialization/deserialization

---

## Query Optimizer
- [ ] Cost model
- [ ] Plan rewriting rules
- [ ] Index usage decision
- [ ] Join ordering strategies

---

## Indexes
- [ ] `CREATE INDEX` syntax
- [ ] B-Tree index (single column)
- [ ] Index scan operator

---

## Concurrency & Transactions
- [ ] MVCC (multi-version concurrency control)
- [ ] Snapshot isolation
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
