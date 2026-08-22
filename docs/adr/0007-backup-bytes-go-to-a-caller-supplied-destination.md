# Backup bytes go to a caller-supplied destination

A backup destination must be something the caller supplies, and it must work in both directions. The caller gives Sirannon three operations: write a piece, read a piece back, and list the pieces that exist. Sirannon carries no client for any storage service, so a caller can connect object storage, another machine, or anything else that moves bytes, and the same interface serves backup and restore. An interface that only wrote would make 'the destination can be anything' true for backup and false for restore, which is where it matters most.

Today the driver contract admits only a filesystem path, and `BackupManager.backup` in the TypeScript implementation resolves whatever it receives into an absolute local path before SQLite sees it, so the capability SQLite offers is discarded by Sirannon's own layer.

## Why this over the alternatives

- **Building storage clients into Sirannon** would pick winners among providers, drag their dependencies into every install, and still miss whichever destination the next caller needs. ClickHouse builds the destination into the engine, and PostgreSQL keeps it out of core and leaves it to outside tools, which is the side Sirannon takes because it ships as a library.
- **Writing a local file and telling the caller to move it** needs free disk equal to the database on every backup, which fails the sizing rule this work set, and it leaves the caller watching a directory to learn that anything happened.
- **Whole-database serialisation into one buffer** holds the entire copy in memory and is ruled out at the sizes this work targets.

## Decision

These rules bind every implementation, because they follow from how SQLite behaves rather than from any one runtime.

- A streamed full copy reaches its destination through a custom VFS registered with SQLite, and the destination is named on the copy through a `vfs=` URI parameter. Change pieces need no VFS, because an implementation reads the write-ahead log file directly.
- The destination takes fixed-size pieces and accepts them in any order, because ordering the bytes is impossible rather than merely expensive. SQLite writes page one last, at 99.79% of the way through a 6.4 GB copy, so a destination that could only append forwards would have to hold the whole database first, measured at 6,387 MB for a 6.4 GB source and growing with every size. Fixed-size pieces collapse that to 16 MB held in two open pieces, and the figure is the same at 100 MB and at 6.4 GB. Object storage already accepts multipart uploads out of order, so the interface asks for what those targets already give.
- The destination fills the gap SQLite leaves at offset 1,073,741,824 with zeros. SQLite never writes that page, so a destination that joined only the bytes it received would produce a file 4096 bytes short with every later byte misplaced. The gap was confirmed at 1.6 GB and 6.4 GB and is absent below 1 GiB.
- The destination holds more than one name, since a chain stores its full copy, each change piece, and its own log under a name of its own. The journal SQLite opens beside a copy stays with the implementation that writes it and reaches no destination.
- Every run reports completion and progress. Completion carries the database id, the source path, a run identifier, whether the run was full or change-only and what it builds on, start, end, and duration, bytes and pages written, the chain position, the destination detail the caller supplied, success or failure with the error, and the restart count. Progress carries the counters at step resolution, so a caller streaming to a remote destination can drive its own reporting. A content fingerprint is included and can be turned off, because computing it costs a full read of what was written.
- The interface is identical on every runtime, and each runtime declares which capabilities it supports, so a caller learns at capability-check time, rather than at failure time, where it can hand over whole databases only.
- Where an implementation cannot register a VFS, the staged fallback writes a temporary local file and streams it out from disk, and it declares that it needs local disk equal to the backup. Pretending to stream while staging would lie about the disk requirement.

## What the TypeScript implementation must do

This section binds the reference implementation alone. An implementation in a language that links SQLite directly registers its VFS through the C API in the same process and needs none of it, so a second implementation should read this as a tax that JavaScript pays rather than as part of the design. JavaScript cannot hand SQLite the C function pointers a VFS is made of, which is the whole reason for what follows.

- The VFS ships as a loadable SQLite extension that Sirannon owns, written in C. Experiments recorded in `docs/research/backup-interoperability.md` proved the route end to end on both Node drivers, with a 100 MB copy delivered and no local file created, and later runs carried it to 6.4 GB.
- The extension returns `SQLITE_OK_LOAD_PERMANENTLY`, because returning plain success lets SQLite unload the module when the loading connection closes, leaving the registered destination pointer dangling and the process crashing on the next use.
- The compiled extension is published as one npm package per platform, listed as optional dependencies of the main package with platform rules, the pattern esbuild uses. Installing Sirannon on a Mac fetches the Mac binary and nothing else, with no extra command and no compiler on the user's machine. CI cross-compiles the Linux and Windows targets with Zig's C compiler and builds the macOS targets on macOS runners.
- Naming the destination through a `vfs=` URI works without configuration on Node's built-in SQLite. On better-sqlite3 it requires `SQLITE_USE_URI=1` in the environment before the module loads, and Sirannon cannot set that on the caller's behalf, because better-sqlite3 reads the variable once at load time and ignores a later change. Running both orders against version 13.0.2 confirmed it: without the variable the statement fails with `SQLITE_CANTOPEN` and the URI is treated as a filename, and with it the same statement fails with `no such vfs`, which proves the parameter reaches lookup. So the operator sets that variable to stream on better-sqlite3, Sirannon reports streaming as unavailable where it is absent, and the documentation states that the variable turns on URI parsing for every filename in the process, including any path containing a question mark.
- Extension loading in Sirannon is broken on both Node drivers today, because the SQL `load_extension` route is refused as unauthorised, and the only test asserts a failure message that a missing file and a refused API produce alike. The fix, switching to each driver's own loading method and adding a test that loads a real extension and asserts it worked, is a defect fix in this change set and precedes everything above.

## Cost

The TypeScript implementation takes on native binaries: one C file, six compiled artefacts per release, and a platform matrix in CI, none of which the project has today. Each platform left out of the matrix silently downgrades to the staged fallback, so the matrix is a standing maintenance duty. Streaming on better-sqlite3 depends on an environment variable the operator sets, which makes the fastest driver the one with the extra deployment step.

Streaming itself costs no measurable throughput: a 6.4 GB copy ran at 675 MB/s through the destination against 534 MB/s to a local file, with process memory under 89 MB at every size tested.

## References

- SQLite VFS interface: <https://sqlite.org/vfs.html>
- URI filenames and the `vfs=` parameter: <https://sqlite.org/uri.html>
- esbuild's per-platform optional packages: <https://esbuild.github.io/getting-started/#simultaneous-platforms>
- Zig as a cross-compiling C toolchain: <https://zig.news/kristoff/building-sqlite-with-cgo-for-every-os-4cic>
- Experiments proving the VFS route and its scale behaviour: `docs/research/backup-interoperability.md`
