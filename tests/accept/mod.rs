//! Acceptance tests — exhaustive randomized testing.
//!
//! These run via `just accept` and target <15 seconds total. They use
//! property-based testing (proptest) and randomized command sequences
//! to find bugs that focused integration tests miss.

// Test modules will be added as commands are implemented:
// mod strings;      // M4
// mod hashes;       // M6
// mod sets;         // M7
// mod sorted_sets;  // M8
// mod lists;        // M9
// mod keys;         // M5
// mod transactions; // M10
// mod mixed;        // cross-type randomized interleaving
