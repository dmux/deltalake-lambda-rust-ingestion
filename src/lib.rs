// Library crate — exposes all modules so integration tests in tests/ can
// import them directly without going through the binary entry point.
pub mod adapters;
pub mod application;
pub mod domain;
pub mod error;
pub mod models;
