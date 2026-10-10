# Sirannon builds its own SQLite to count the rows that each statement reads

A host that puts many tenants on one machine needs to know how much of the machine each tenant used. For a shared SQLite database, the established measure of that use is rows read and rows written, which Cloudflare D1, Durable Objects, and Turso all bill by. Stock SQLite has no rows-read counter, so Cloudflare and Turso each build their own SQLite to get one. Sirannon does the same by publishing `@delali/sirannon-sqlite`, which contains better-sqlite3 13's JavaScript unchanged and a native build of better-sqlite3 compiled against a SQLite with Cloudflare's row-count patch applied. The better-sqlite3 driver loads that package when the host passes `rowCounts: true`.

## Why this over the alternatives

- **CPU time per statement.** Sirannon can measure it without native code, since Node's `process.threadCpuUsage()` returns the CPU time of the current thread. However, the same work takes more CPU time when other processes are busy on the machine. Measured with `node:sqlite` on an M3 Pro, a scan of 200,000 rows took 5.1 ms of CPU time with the machine idle, 6.7 ms beside 11 CPU-bound processes, and 13.2 ms beside 6 memory-bound processes. One tenant's bill can therefore depend on what the other tenants are doing.
- **SQLite's virtual machine step counter.** Stock SQLite keeps this counter, which read 600,013 for the same scan on every run, idle or loaded. We found no host that bills by it, so a tenant has nothing to compare it with.
- **A loadable extension on its own.** SQLite gives an extension hooks for storage, functions, virtual tables, and statement events, but it calls none of them when a cursor moves to a row. Stock SQLite counts each step to the next row, but it counts no seek, so an extension will report zero rows read for a primary-key lookup.
- **Building better-sqlite3 on the host.** A host that compiles better-sqlite3 against a patched SQLite needs a compiler wherever it installs Sirannon.
- **Loading the native file through `nativeBinding`.** With better-sqlite3's `nativeBinding` option, the host's own better-sqlite3 JavaScript loads the patched native file. When the host upgrades better-sqlite3, that JavaScript can call a function that the native file lacks.

## Decision

- Sirannon counts rows read and rows written by the rules in Cloudflare's patch, which match the rules that D1 publishes. Turso counts an exact `count(*)` in pages, so Sirannon makes no claim to match Turso.
- Sirannon changes the patch in two places. The patched counter skips the work of any trigger whose name begins with `_sirannon_trg_`, so that a tenant's counts stay the same whether change capture is on or off. Sirannon also widens both counters from 32 to 64 bits and reads them through a 64-bit call modelled on SQLite's `sqlite3_status64`. The 200,000-row scan above works out to 39 million rows read per second, at which rate a statement passes the 32-bit maximum of 4,294,967,295 in about 110 seconds. Sirannon sets no limit on how long a read can take, so a statement can reach that maximum. libSQL, the source of Cloudflare's patch, caps its 32-bit counters at that maximum and calls the cap a temporary fix, since widening the counters was more work than its maintainers wanted to take on at the time.
- The driver returns the counts with each statement's own result. When Sirannon rolls back a grouped write and executes it again, it therefore reports only the attempt that took effect. For a failed statement, Sirannon reports the work that SQLite did before the failure.
- Sirannon reports the counts through `onQueryComplete`, including each read that a live query makes, marked `source: 'live'`. Sirannon counts nothing for change delivery, device sync, backups, or replication.
- A driver configured to count fails at open with `ROW_COUNTS_UNSUPPORTED` when it cannot load the counting build, so that a billing host receives either a count for every statement or an error at open.
- Sirannon builds the native file for the same 8 targets as the streaming extension, with the same zig toolchain. Before each release, CI loads the file and checks its counts on each target's own operating system and architecture.

## Cost

Sirannon maintains a patch on SQLite, reapplies it for each SQLite release from Cloudflare's current copy, and rebuilds for each better-sqlite3 version that it adopts. On Windows, zig refuses the flag that lets the native file resolve Node's functions from the running process, so the build script reruns zig's own link command with that flag added. The `node:sqlite`, Bun, wa-sqlite, and Expo drivers report no row counts.

## References

- Cloudflare's row-count patch: <https://github.com/cloudflare/workerd/blob/main/patches/sqlite/0001-row-counts-plain.patch>
- libSQL's counters: <https://github.com/tursodatabase/libsql/blob/main/libsql-sqlite3/src/vdbe.c>
- libSQL's temporary cap on its 32-bit counters: <https://github.com/tursodatabase/libsql/pull/1525>
- D1's counting rules: <https://developers.cloudflare.com/d1/platform/pricing/>
- better-sqlite3's custom builds: <https://github.com/WiseLibs/better-sqlite3/blob/master/docs/compilation.md>
- SQLite's statement counters: <https://www.sqlite.org/c3ref/c_stmtstatus_counter.html>
