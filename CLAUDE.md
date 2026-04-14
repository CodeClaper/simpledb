# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

- `make` – Builds the project (compiles all subdirectories).
- `make DEBUG=1` – Builds with debug symbols and no optimizations.
- `make clean` – Cleans build artifacts.

The build system uses a top-level Makefile that delegates to subdirectories (`src`, `test/gtest`). The main executables are `src/simpledb` (database server) and `src/simpledb_cli` (command-line client).

## Test Commands

- `make check` – Runs all test suites (gtest, pytest, minunit).
- `make check-gtest` – Runs C++ unit tests (Google Test).
- `make check-pytest` – Runs Python integration tests.
- `make check-minunit` – Runs C unit tests (minunit).
- `pytest -v` – Run pytest with verbose output (can be used with a specific test file).
- `cd test/minunit && ./test` – Run minunit tests directly.

To run a single Google Test, first build the gtest binary (`make -C test/gtest`), then run `./test/gtest/gtest --gtest_filter=TestSuite.TestName`.

Test suites are located in `test/`:
- `test/gtest/` – Google Test C++ tests.
- `test/pytest/` – Python integration tests (require a running simpledb server; start the server with default config before running pytest).
- `test/minunit/` – Lightweight C unit tests.

## Running the Server and Client

1. **Start the server**: `./src/simpledb` (uses default port 4083 from config).
   - The server reads configuration from `config/simpledb.cnf`.
   - Data directory is configured as `dir` under `[data]` (default `~/data/`).
   - Log directory is configured under `[log]`.

2. **Run the CLI client**: `./src/simpledb_cli -h <host> -p <port>` (default host 127.0.0.1, port 4080).
   - The client connects to the server via TCP socket.
   - Interactive shell with readline support.
   - Note: the server's default port is 4083 (configured in `config/simpledb.cnf`), so you may need to specify `-p 4083` or adjust the configuration.

## Configuration

The main configuration file is `config/simpledb.cnf`. Key settings:
- `[data] dir` – Database file storage path.
- `[base] port` – Server listening port.
- `[log] level` – Log verbosity (TRACE, DEBUG, INFO, SUCCESS, WARN, ERROR).
- `[transaction] trans_isolation_level` – READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE.
- `[auth] account` / `password` – Authentication credentials.

## Architecture Overview

SimpleDb is an object‑oriented relational database engine written in C. It follows a modular architecture:

### Core Modules

- **backend/** – Server process management, session handling, request processing.
- **memory/** – Memory context system (`MemoryContext`), shared‑memory manager, allocator wrappers.
- **storage/** – File descriptors, buffer manager (`bufmgr`), background writer (`bgwriter`).
- **trans/** – Transaction management (MVCC), isolation levels, visibility checks.
- **lock/** – Table‑level and row‑level locking, deadlock detection, spinlocks.
- **parser/** – SQL lexer (Lex) and parser (Yacc), statement tree generation.
- **heap/** – Heap‑organized table storage.
- **index/** – Index structures (B‑tree, binary search).
- **utils/** – Atomic operations, hash tables, queues, timers, bit utilities.
- **common/** – Common definitions, error handling, configuration loading.
- **sys/** – System tables and system state tracking.

### Key Data Structures

- `MemoryContext` – Hierarchical memory regions used for lifetime management.
- `Row` – Represents a table row with MVCC metadata (created/expired transaction IDs).
- `TransEntry` – Transaction descriptor.
- `TableLock` / `RowLock` – Lock structures.
- `BufMgr` – Buffer pool manager.

### Initialization Flow (see `src/db.c`)

1. Load configuration (`load_conf`).
2. Initialize memory contexts (`MemoryContextInit`).
3. Set up shared memory (`init_shmem`).
4. Initialize transaction subsystem (`InitTrans`).
5. Start buffer manager (`InitBufMgr`).
6. Start background writer (`StartBgWriter`).
7. Listen for client connections and fork backend processes.

### Client‑Server Protocol

- The server listens on a TCP port, forks a child process per connection.
- The client sends SQL statements as plain text; server replies with result sets terminated by “OVER”.

## Development Tips

- Use `debug.sh` to build with debug symbols, kill any running server, clear the data directory, and start GDB.
- The data directory is `~/data/` by default; ensure it exists and is writable.
- Logs are written to `~/logs/simpledb/` (configurable).
- When adding new SQL syntax, update `sql/sql2.y` (parser) and `sql/scn2.l` (lexer), then regenerate the C files (requires flex & bison).
- The codebase uses a custom allocator (`dalloc`/`dfree`) that respects memory contexts; prefer these over `malloc`/`free` for database‑managed memory.
- Commit messages often use a prefix (`fix:`, `raf:`, etc.) to indicate the type of change.
- There is a benchmark tool in `src/bench/`; build with `make -C src/bench` and run `./src/bench/bench`.
