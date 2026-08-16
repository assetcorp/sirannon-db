# Backup bytes go to a caller-supplied destination

A backup destination must be something the caller supplies, and it must work in both directions. The caller gives Sirannon three operations: write a piece, read a piece back, and list the pieces that exist. Sirannon carries no client for any storage service, so a caller can connect object storage, another machine, or anything else that moves bytes, and the same interface serves backup and restore. An interface that only wrote would make 'the destination can be anything' true for backup and false for restore, which is where it matters most.

Today the driver contract admits only a filesystem path, and `BackupManager.backup` resolves whatever it receives into an absolute local path before SQLite sees it, so the capability SQLite offers is discarded by Sirannon's own layer.

## Why this over the alternatives

- **Building storage clients into Sirannon** would pick winners among providers, drag their dependencies into every install, and still miss whichever destination the next caller needs. ClickHouse builds the destination into the engine, and PostgreSQL keeps it out of core and leaves it to outside tools, which is the side Sirannon takes because it ships as a library.
- **Writing a local file and telling the caller to move it** needs free disk equal to the database on every backup, which fails the sizing rule this work set, and it leaves the caller watching a directory to learn that anything happened.
- **Whole-database serialisation into one buffer**, which both Node drivers offer, holds the entire copy in memory and is ruled out at the sizes this work targets.

## Decision

- The streamed full copy runs through a loadable SQLite extension that Sirannon owns: roughly two hundred lines of C implementing a VFS, named on the destination through a `vfs=` URI parameter. Experiments recorded in `docs/research/backup-interoperability.md` proved this end to end on both Node drivers, with a 100 MB copy delivered through the custom VFS and no local file created. Change pieces need no extension, because Sirannon reads the log file directly.
- The destination VFS receives writes that are mostly forward with a measured 1.7% landing at earlier offsets, the lowest at offset zero, so the interface presents pieces to the caller only after Sirannon has put their bytes in order. An append-only upload target must never see an out-of-order write.
- The compiled extension is published as one npm package per platform, listed as optional dependencies of the main package with platform rules, the pattern esbuild uses. Installing Sirannon on a Mac fetches the Mac binary and nothing else, with no extra command and no compiler on the user's machine. CI cross-compiles the Linux and Windows targets with Zig's C compiler and builds the macOS targets on macOS runners.
- On a platform with no published binary, the install still succeeds, Sirannon declares streaming unavailable in its capability report, and the staged fallback writes a temporary local file and streams it out from disk. The fallback declares that it needs local disk equal to the backup, because pretending to stream while staging would lie about the disk requirement.
- Extension loading in Sirannon is broken on both Node drivers today, because the SQL `load_extension` route is refused as unauthorised, and the only test asserts a failure message that a missing file and a refused API produce alike. The fix, switching to each driver's own loading method and adding a test that loads a real extension and asserts it worked, is a defect fix in this change set and precedes everything above.
- Every run reports completion and progress. Completion carries the database id, the source path, a run identifier, whether the run was full or change-only and what it builds on, start, end, and duration, bytes and pages written, the chain position, the destination detail the caller supplied, success or failure with the error, and the restart count. Progress carries the counters at step resolution, so a caller streaming to a remote destination can drive its own reporting. A content fingerprint is included and can be turned off, because computing it costs a full read of what was written.
- The interface is identical on every runtime, and each runtime declares which capabilities it supports, so a browser or Expo caller learns at capability-check time, rather than at failure time, that it can hand over whole databases only.

## Cost

Sirannon takes on native binaries: one C file, six compiled artefacts per release, and a platform matrix in CI, none of which the project has today. Each platform left out of the matrix silently downgrades to the staged fallback, so the matrix is a standing maintenance duty.

## References

- SQLite VFS interface: <https://sqlite.org/vfs.html>
- URI filenames and the `vfs=` parameter: <https://sqlite.org/uri.html>
- esbuild's per-platform optional packages: <https://esbuild.github.io/getting-started/#simultaneous-platforms>
- Zig as a cross-compiling C toolchain: <https://zig.news/kristoff/building-sqlite-with-cgo-for-every-os-4cic>
- Experiments proving the VFS route on both drivers: `docs/research/backup-interoperability.md`
