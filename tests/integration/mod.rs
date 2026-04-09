//! Integration tests — fast, focused cases per command.
//!
//! These run via `just test` and must complete in <2 seconds total.
//! Each test hits the real server (and eventually real FDB) with a
//! handful of representative cases: happy path, error path, one edge case.

// Test modules will be added as commands are implemented:
// mod strings;    // M4
// mod hashes;     // M6
// mod sets;       // M7
// mod sorted_sets; // M8
// mod lists;      // M9
// mod keys;       // M5
// mod transactions; // M10
// mod pubsub;     // M11
// mod server;     // M2
