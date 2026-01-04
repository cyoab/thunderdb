//! Summary: Failpoint injection for crash safety testing.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides a failpoint system for testing database durability
//! and crash recovery. Failpoints can be injected at critical points in
//! the write path to simulate crashes and verify recovery.
//!
//! # Usage
//!
//! Failpoints are only active when the `failpoint` feature is enabled.
//! In production builds, all failpoint checks compile to no-ops.
//!
//! ```ignore
//! use thunderdb::failpoint::{failpoint, FailpointRegistry};
//!
//! // Register a failpoint
//! FailpointRegistry::global().register("before_meta_write", FailAction::Panic);
//!
//! // In critical code path
//! failpoint!("before_meta_write");
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// Global failpoint registry singleton.
static REGISTRY: OnceLock<FailpointRegistry> = OnceLock::new();

/// Actions that can be triggered when a failpoint is hit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailAction {
    /// Do nothing (failpoint disabled).
    None,
    /// Panic immediately (simulates crash).
    Panic,
    /// Return an I/O error.
    IoError,
    /// Sleep for a duration before continuing.
    Sleep(std::time::Duration),
    /// Execute a callback function.
    Callback,
    /// Panic after N hits.
    PanicAfterN(u64),
    /// Return error after N hits.
    ErrorAfterN(u64),
    /// Panic with specified probability (0-100).
    PanicWithProbability(u8),
    /// Exit process immediately (true kill -9 simulation).
    Exit(i32),
}

impl Default for FailAction {
    fn default() -> Self {
        FailAction::None
    }
}

/// Configuration for a single failpoint.
pub struct Failpoint {
    /// Name of the failpoint.
    pub name: &'static str,
    /// Action to take when hit.
    action: Mutex<FailAction>,
    /// Number of times this failpoint has been hit.
    hit_count: AtomicU64,
    /// Whether this failpoint is enabled.
    enabled: AtomicBool,
    /// Optional callback for Callback action.
    callback: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl std::fmt::Debug for Failpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Failpoint")
            .field("name", &self.name)
            .field("action", &self.action)
            .field("hit_count", &self.hit_count)
            .field("enabled", &self.enabled)
            .field("callback", &"<callback>")
            .finish()
    }
}

impl Failpoint {
    /// Creates a new failpoint with the given name.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            action: Mutex::new(FailAction::None),
            hit_count: AtomicU64::new(0),
            enabled: AtomicBool::new(false),
            callback: Mutex::new(None),
        }
    }

    /// Sets the action for this failpoint.
    pub fn set_action(&self, action: FailAction) {
        let enabled = !matches!(action, FailAction::None);
        *self.action.lock().unwrap() = action;
        self.enabled.store(enabled, Ordering::SeqCst);
    }

    /// Sets a callback function for the Callback action.
    pub fn set_callback<F: Fn() + Send + Sync + 'static>(&self, callback: F) {
        *self.callback.lock().unwrap() = Some(Box::new(callback));
    }

    /// Checks if this failpoint is enabled.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Returns the current hit count.
    pub fn hit_count(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    /// Resets the hit count to zero.
    pub fn reset(&self) {
        self.hit_count.store(0, Ordering::SeqCst);
        self.enabled.store(false, Ordering::SeqCst);
        *self.action.lock().unwrap() = FailAction::None;
    }

    /// Triggers the failpoint, executing its configured action.
    ///
    /// Returns `true` if an action was executed, `false` if disabled.
    pub fn trigger(&self) -> Result<bool, std::io::Error> {
        if !self.is_enabled() {
            return Ok(false);
        }

        let count = self.hit_count.fetch_add(1, Ordering::SeqCst) + 1;
        let action = self.action.lock().unwrap().clone();

        match action {
            FailAction::None => Ok(false),
            FailAction::Panic => {
                panic!("Failpoint '{}' triggered panic (hit #{})", self.name, count);
            }
            FailAction::IoError => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failpoint '{}' triggered I/O error", self.name),
            )),
            FailAction::Sleep(duration) => {
                std::thread::sleep(duration);
                Ok(true)
            }
            FailAction::Callback => {
                if let Some(ref cb) = *self.callback.lock().unwrap() {
                    cb();
                }
                Ok(true)
            }
            FailAction::PanicAfterN(n) => {
                if count >= n {
                    panic!(
                        "Failpoint '{}' triggered panic after {} hits",
                        self.name, count
                    );
                }
                Ok(true)
            }
            FailAction::ErrorAfterN(n) => {
                if count >= n {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failpoint '{}' triggered error after {} hits", self.name, n),
                    ));
                }
                Ok(true)
            }
            FailAction::PanicWithProbability(pct) => {
                let roll: u8 = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .subsec_nanos()
                    % 100) as u8;
                if roll < pct {
                    panic!(
                        "Failpoint '{}' triggered probabilistic panic ({}%)",
                        self.name, pct
                    );
                }
                Ok(true)
            }
            FailAction::Exit(code) => {
                // Flush stderr to ensure any error messages are written
                eprintln!("Failpoint '{}' triggering process exit({})", self.name, code);
                std::process::exit(code);
            }
        }
    }
}

/// Registry of all failpoints in the system.
pub struct FailpointRegistry {
    failpoints: Mutex<HashMap<&'static str, Failpoint>>,
}

impl FailpointRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            failpoints: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the global failpoint registry.
    pub fn global() -> &'static Self {
        REGISTRY.get_or_init(FailpointRegistry::new)
    }

    /// Registers a failpoint with a specific action.
    pub fn register(&self, name: &'static str, action: FailAction) {
        let mut fps = self.failpoints.lock().unwrap();
        let fp = fps
            .entry(name)
            .or_insert_with(|| Failpoint::new(name));
        fp.set_action(action);
    }

    /// Registers a failpoint with a callback.
    pub fn register_callback<F: Fn() + Send + Sync + 'static>(
        &self,
        name: &'static str,
        callback: F,
    ) {
        let mut fps = self.failpoints.lock().unwrap();
        let fp = fps
            .entry(name)
            .or_insert_with(|| Failpoint::new(name));
        fp.set_action(FailAction::Callback);
        fp.set_callback(callback);
    }

    /// Disables a failpoint.
    pub fn disable(&self, name: &str) {
        let fps = self.failpoints.lock().unwrap();
        if let Some(fp) = fps.get(name) {
            fp.set_action(FailAction::None);
        }
    }

    /// Disables all failpoints.
    pub fn disable_all(&self) {
        let fps = self.failpoints.lock().unwrap();
        for fp in fps.values() {
            fp.set_action(FailAction::None);
        }
    }

    /// Resets all failpoints (disables and clears hit counts).
    pub fn reset_all(&self) {
        let fps = self.failpoints.lock().unwrap();
        for fp in fps.values() {
            fp.reset();
        }
    }

    /// Triggers a failpoint by name.
    ///
    /// Returns Ok(false) if the failpoint doesn't exist or is disabled.
    pub fn trigger(&self, name: &str) -> Result<bool, std::io::Error> {
        let fps = self.failpoints.lock().unwrap();
        if let Some(fp) = fps.get(name) {
            fp.trigger()
        } else {
            Ok(false)
        }
    }

    /// Gets the hit count for a failpoint.
    pub fn hit_count(&self, name: &str) -> u64 {
        let fps = self.failpoints.lock().unwrap();
        fps.get(name).map(|fp| fp.hit_count()).unwrap_or(0)
    }

    /// Checks if a failpoint is enabled.
    pub fn is_enabled(&self, name: &str) -> bool {
        let fps = self.failpoints.lock().unwrap();
        fps.get(name).is_some_and(|fp| fp.is_enabled())
    }

    /// Lists all registered failpoint names.
    pub fn list(&self) -> Vec<&'static str> {
        let fps = self.failpoints.lock().unwrap();
        fps.keys().copied().collect()
    }
}

impl Default for FailpointRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Failpoint Names (Constants)
// ============================================================================

/// Failpoint locations in the write path.
pub mod locations {
    // persist_tree path
    pub const BEFORE_DATA_WRITE: &str = "before_data_write";
    pub const AFTER_DATA_WRITE: &str = "after_data_write";
    pub const BEFORE_OVERFLOW_WRITE: &str = "before_overflow_write";
    pub const AFTER_OVERFLOW_WRITE: &str = "after_overflow_write";
    pub const BEFORE_META_WRITE: &str = "before_meta_write";
    pub const AFTER_META_WRITE: &str = "after_meta_write";
    pub const BEFORE_FSYNC: &str = "before_fsync";
    pub const AFTER_FSYNC: &str = "after_fsync";

    // persist_incremental path
    pub const INCR_BEFORE_META_WRITE: &str = "incr_before_meta_write";
    pub const INCR_AFTER_META_WRITE: &str = "incr_after_meta_write";
    pub const INCR_BEFORE_ENTRY_WRITE: &str = "incr_before_entry_write";
    pub const INCR_AFTER_ENTRY_WRITE: &str = "incr_after_entry_write";
    pub const INCR_BEFORE_FSYNC: &str = "incr_before_fsync";
    pub const INCR_AFTER_FSYNC: &str = "incr_after_fsync";

    // Meta page specific
    pub const BEFORE_TXID_INCREMENT: &str = "before_txid_increment";
    pub const AFTER_TXID_INCREMENT: &str = "after_txid_increment";
    pub const BEFORE_ROOT_UPDATE: &str = "before_root_update";
    pub const AFTER_ROOT_UPDATE: &str = "after_root_update";

    // WAL specific
    pub const WAL_BEFORE_WRITE: &str = "wal_before_write";
    pub const WAL_AFTER_WRITE: &str = "wal_after_write";
    pub const WAL_BEFORE_SYNC: &str = "wal_before_sync";
    pub const WAL_AFTER_SYNC: &str = "wal_after_sync";

    /// All failpoint location names.
    pub const ALL: &[&str] = &[
        BEFORE_DATA_WRITE,
        AFTER_DATA_WRITE,
        BEFORE_OVERFLOW_WRITE,
        AFTER_OVERFLOW_WRITE,
        BEFORE_META_WRITE,
        AFTER_META_WRITE,
        BEFORE_FSYNC,
        AFTER_FSYNC,
        INCR_BEFORE_META_WRITE,
        INCR_AFTER_META_WRITE,
        INCR_BEFORE_ENTRY_WRITE,
        INCR_AFTER_ENTRY_WRITE,
        INCR_BEFORE_FSYNC,
        INCR_AFTER_FSYNC,
        BEFORE_TXID_INCREMENT,
        AFTER_TXID_INCREMENT,
        BEFORE_ROOT_UPDATE,
        AFTER_ROOT_UPDATE,
        WAL_BEFORE_WRITE,
        WAL_AFTER_WRITE,
        WAL_BEFORE_SYNC,
        WAL_AFTER_SYNC,
    ];
}

// ============================================================================
// Failpoint Macro
// ============================================================================

/// Triggers a failpoint by name.
///
/// When the `failpoint` feature is enabled, this macro checks the global
/// failpoint registry and executes the configured action. In release builds
/// without the feature, this compiles to nothing.
///
/// # Examples
///
/// ```ignore
/// failpoint!("before_meta_write");
/// failpoint!("custom_point", |action| { ... });
/// ```
#[cfg(feature = "failpoint")]
#[macro_export]
macro_rules! failpoint {
    ($name:expr) => {{
        if let Err(e) = $crate::failpoint::FailpointRegistry::global().trigger($name) {
            return Err($crate::error::Error::FileSync {
                context: concat!("failpoint: ", $name),
                source: e,
            });
        }
    }};
    ($name:expr, $err_type:expr) => {{
        if let Err(_e) = $crate::failpoint::FailpointRegistry::global().trigger($name) {
            return Err($err_type);
        }
    }};
}

/// No-op version of failpoint macro when feature is disabled.
#[cfg(not(feature = "failpoint"))]
#[macro_export]
macro_rules! failpoint {
    ($name:expr) => {};
    ($name:expr, $err_type:expr) => {};
}

// ============================================================================
// Testing Utilities
// ============================================================================

/// Scoped failpoint guard that disables the failpoint on drop.
pub struct FailpointGuard {
    name: &'static str,
}

impl FailpointGuard {
    /// Creates a new guard that will disable the failpoint on drop.
    pub fn new(name: &'static str, action: FailAction) -> Self {
        FailpointRegistry::global().register(name, action);
        Self { name }
    }
}

impl Drop for FailpointGuard {
    fn drop(&mut self) {
        FailpointRegistry::global().disable(self.name);
    }
}

/// Builder for configuring failpoints in tests.
pub struct FailpointBuilder {
    configs: Vec<(&'static str, FailAction)>,
}

impl FailpointBuilder {
    /// Creates a new failpoint builder.
    pub fn new() -> Self {
        Self {
            configs: Vec::new(),
        }
    }

    /// Adds a panic failpoint.
    pub fn panic_at(mut self, name: &'static str) -> Self {
        self.configs.push((name, FailAction::Panic));
        self
    }

    /// Adds an exit failpoint (true process termination).
    pub fn exit_at(mut self, name: &'static str, code: i32) -> Self {
        self.configs.push((name, FailAction::Exit(code)));
        self
    }

    /// Adds an error failpoint.
    pub fn error_at(mut self, name: &'static str) -> Self {
        self.configs.push((name, FailAction::IoError));
        self
    }

    /// Adds a conditional panic failpoint.
    pub fn panic_after_n(mut self, name: &'static str, n: u64) -> Self {
        self.configs.push((name, FailAction::PanicAfterN(n)));
        self
    }

    /// Adds a probabilistic panic failpoint.
    pub fn panic_with_probability(mut self, name: &'static str, pct: u8) -> Self {
        self.configs.push((name, FailAction::PanicWithProbability(pct)));
        self
    }

    /// Applies all configured failpoints.
    pub fn apply(self) {
        let registry = FailpointRegistry::global();
        for (name, action) in self.configs {
            registry.register(name, action);
        }
    }

    /// Applies failpoints and returns a guard that cleans up on drop.
    pub fn apply_scoped(self) -> FailpointScopeGuard {
        let names: Vec<_> = self.configs.iter().map(|(n, _)| *n).collect();
        self.apply();
        FailpointScopeGuard { names }
    }
}

impl Default for FailpointBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Guard that disables multiple failpoints on drop.
pub struct FailpointScopeGuard {
    names: Vec<&'static str>,
}

impl Drop for FailpointScopeGuard {
    fn drop(&mut self) {
        let registry = FailpointRegistry::global();
        for name in &self.names {
            registry.disable(name);
        }
    }
}

// ============================================================================
// Random Failpoint Selector (for fuzzing)
// ============================================================================

/// Selects random failpoints for chaos testing.
pub struct RandomFailpointSelector {
    seed: u64,
    enabled_locations: Vec<&'static str>,
}

impl RandomFailpointSelector {
    /// Creates a new random selector with the given seed.
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            enabled_locations: locations::ALL.to_vec(),
        }
    }

    /// Creates a selector from environment variable or random seed.
    pub fn from_env_or_random() -> Self {
        let seed = std::env::var("THUNDERDB_CRASH_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });
        Self::new(seed)
    }

    /// Restricts which locations can be selected.
    pub fn only_locations(mut self, locations: &[&'static str]) -> Self {
        self.enabled_locations = locations.to_vec();
        self
    }

    /// Simple LCG random number generator.
    fn next_random(&mut self) -> u64 {
        // LCG parameters from Numerical Recipes
        self.seed = self.seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.seed
    }

    /// Selects a random failpoint location.
    pub fn select_location(&mut self) -> &'static str {
        let idx = (self.next_random() as usize) % self.enabled_locations.len();
        self.enabled_locations[idx]
    }

    /// Registers a random failpoint with the given action.
    pub fn register_random(&mut self, action: FailAction) -> &'static str {
        let location = self.select_location();
        FailpointRegistry::global().register(location, action);
        location
    }

    /// Returns the current seed (for reproducibility).
    pub fn seed(&self) -> u64 {
        self.seed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failpoint_disabled_by_default() {
        let fp = Failpoint::new("test");
        assert!(!fp.is_enabled());
        assert!(fp.trigger().unwrap() == false);
    }

    #[test]
    fn test_failpoint_io_error() {
        let fp = Failpoint::new("test_error");
        fp.set_action(FailAction::IoError);
        assert!(fp.is_enabled());
        assert!(fp.trigger().is_err());
    }

    #[test]
    fn test_failpoint_hit_count() {
        let fp = Failpoint::new("test_count");
        fp.set_action(FailAction::Sleep(std::time::Duration::from_nanos(1)));

        for _ in 0..5 {
            let _ = fp.trigger();
        }

        assert_eq!(fp.hit_count(), 5);
    }

    #[test]
    fn test_failpoint_error_after_n() {
        let fp = Failpoint::new("test_after_n");
        fp.set_action(FailAction::ErrorAfterN(3));

        assert!(fp.trigger().is_ok()); // hit 1
        assert!(fp.trigger().is_ok()); // hit 2
        assert!(fp.trigger().is_err()); // hit 3 - error
    }

    #[test]
    fn test_registry_global() {
        let registry = FailpointRegistry::global();
        registry.register("global_test", FailAction::IoError);
        assert!(registry.is_enabled("global_test"));
        registry.disable("global_test");
        assert!(!registry.is_enabled("global_test"));
    }

    #[test]
    fn test_failpoint_callback() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();

        let fp = Failpoint::new("test_callback");
        fp.set_action(FailAction::Callback);
        fp.set_callback(move || {
            called_clone.store(true, Ordering::SeqCst);
        });

        let _ = fp.trigger();
        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn test_failpoint_builder() {
        let _guard = FailpointBuilder::new()
            .error_at("builder_test1")
            .panic_after_n("builder_test2", 10)
            .apply_scoped();

        let registry = FailpointRegistry::global();
        assert!(registry.is_enabled("builder_test1"));
        assert!(registry.is_enabled("builder_test2"));
    }

    #[test]
    fn test_random_selector() {
        let mut selector = RandomFailpointSelector::new(12345);

        let loc1 = selector.select_location();
        let loc2 = selector.select_location();

        // With same seed, should get same sequence
        let mut selector2 = RandomFailpointSelector::new(12345);
        assert_eq!(loc1, selector2.select_location());
        assert_eq!(loc2, selector2.select_location());
    }

    #[test]
    fn test_all_locations_defined() {
        // Verify all location constants are in the ALL array
        assert!(locations::ALL.contains(&locations::BEFORE_DATA_WRITE));
        assert!(locations::ALL.contains(&locations::AFTER_FSYNC));
        assert!(locations::ALL.contains(&locations::WAL_AFTER_SYNC));
    }
}
