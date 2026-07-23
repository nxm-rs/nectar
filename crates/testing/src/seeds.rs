//! Seed-corpus replay walker: one loop shared by every stable seed-replay
//! test, so a committed fuzz seed always carries an intent assertion.

use std::fs;
use std::path::{Path, PathBuf};

type Hook<'a> = Box<dyn FnMut(&str, &[u8]) + 'a>;

struct Rule<'a> {
    prefix: &'static str,
    hook: Option<Hook<'a>>,
    matched: usize,
}

/// Replay walker over one committed seed corpus.
///
/// [`each`](Self::each) hooks run for every seed; [`on`](Self::on) rules
/// classify a seed by name prefix, first registered match wins;
/// [`covers`](Self::covers) classifies a prefix with no assertion beyond the
/// `each` hooks. [`run`](Self::run) fails the walk on a seed no rule
/// classifies and on a rule no seed matches, so the assertions and the
/// corpus cannot drift apart silently.
#[must_use = "the corpus is only replayed by run()"]
pub struct SeedReplay<'a> {
    dir: PathBuf,
    each: Vec<Hook<'a>>,
    rules: Vec<Rule<'a>>,
    floor: usize,
}

impl<'a> SeedReplay<'a> {
    /// Walker over `fuzz/seeds/<target>`, resolved from a workspace crate's
    /// `CARGO_MANIFEST_DIR`.
    pub fn corpus(manifest_dir: &str, target: &str) -> Self {
        Self::dir(
            Path::new(manifest_dir)
                .join("../../fuzz/seeds")
                .join(target),
        )
    }

    /// Walker over an explicit seed directory.
    pub fn dir(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            each: Vec::new(),
            rules: Vec::new(),
            floor: 1,
        }
    }

    /// Runs `hook` on every seed, before classification.
    pub fn each(mut self, hook: impl FnMut(&str, &[u8]) + 'a) -> Self {
        self.each.push(Box::new(hook));
        self
    }

    /// Classifies seeds whose name starts with `prefix` and runs `hook` on
    /// them. The first matching rule wins, so register the more specific
    /// prefix first.
    pub fn on(mut self, prefix: &'static str, hook: impl FnMut(&str, &[u8]) + 'a) -> Self {
        self.rules.push(Rule {
            prefix,
            hook: Some(Box::new(hook)),
            matched: 0,
        });
        self
    }

    /// Classifies `prefix` with no assertion beyond the `each` hooks.
    pub fn covers(mut self, prefix: &'static str) -> Self {
        self.rules.push(Rule {
            prefix,
            hook: None,
            matched: 0,
        });
        self
    }

    /// Requires at least `n` replayed seeds after the walk; defaults to 1.
    pub fn floor(mut self, n: usize) -> Self {
        self.floor = n;
        self
    }

    /// Walks the corpus in name order. Panics on a missing or unreadable
    /// directory, an unclassified seed name, a prefix matching no seed, or a
    /// floor violation. Returns the replayed count.
    pub fn run(mut self) -> usize {
        let entries = fs::read_dir(&self.dir)
            .unwrap_or_else(|e| panic!("seed dir {} must exist: {e}", self.dir.display()));
        let mut paths: Vec<PathBuf> = entries
            .map(|entry| entry.expect("seed dir entries must be readable").path())
            .collect();
        paths.sort();

        let mut replayed = 0usize;
        for path in paths {
            let name = path
                .file_name()
                .expect("a seed path carries a file name")
                .to_string_lossy()
                .into_owned();
            let data =
                fs::read(&path).unwrap_or_else(|e| panic!("seed {name} must be readable: {e}"));
            for hook in &mut self.each {
                hook(&name, &data);
            }
            // Machine seeds from the nightly corpus refresh are hex-named;
            // they run the target oracle above but carry no intent class.
            if is_machine_name(&name) {
                replayed += 1;
                continue;
            }
            let rule = self
                .rules
                .iter_mut()
                .find(|rule| name.starts_with(rule.prefix))
                .unwrap_or_else(|| {
                    panic!(
                        "seed {name} in {} matches no registered prefix",
                        self.dir.display()
                    )
                });
            rule.matched += 1;
            if let Some(hook) = rule.hook.as_mut() {
                hook(&name, &data);
            }
            replayed += 1;
        }

        for rule in &self.rules {
            assert!(
                rule.matched > 0,
                "prefix {:?} matched no seed in {}",
                rule.prefix,
                self.dir.display()
            );
        }
        assert!(
            replayed >= self.floor,
            "expected at least {} seeds in {}, found {replayed}",
            self.floor,
            self.dir.display()
        );
        replayed
    }
}

/// Whether `name` is a machine seed from the nightly corpus refresh: a bare
/// sha1 hex stem, no curated prefix.
fn is_machine_name(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or(name);
    stem.len() == 40
        && stem
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::path::PathBuf;

    use super::SeedReplay;

    /// A throwaway corpus directory, removed on drop.
    struct Corpus(PathBuf);

    impl Corpus {
        fn new(files: &[(&str, &[u8])]) -> Self {
            static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "nectar-seed-replay-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).expect("temp corpus dir is creatable");
            for (name, data) in files {
                std::fs::write(dir.join(name), data).expect("temp seed is writable");
            }
            Self(dir)
        }
    }

    impl Drop for Corpus {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn panic_message(result: std::thread::Result<usize>) -> String {
        let payload = result.expect_err("the walk must fail");
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
            .expect("panic payload is a string")
    }

    #[test]
    fn classifies_by_first_matching_prefix_and_counts() {
        let corpus = Corpus::new(&[
            ("valid-a.bin", b"a"),
            ("valid-soc-b.bin", b"b"),
            ("invalid-c.bin", b"c"),
        ]);
        let seen = Cell::new(0usize);
        let socs = Cell::new(0usize);
        let replayed = SeedReplay::dir(&corpus.0)
            .each(|_, _| seen.set(seen.get() + 1))
            .on("valid-soc-", |_, data| {
                socs.set(socs.get() + 1);
                assert_eq!(data, b"b");
            })
            .covers("valid-")
            .covers("invalid-")
            .floor(3)
            .run();
        assert_eq!((replayed, seen.get(), socs.get()), (3, 3, 1));
    }

    #[test]
    fn machine_named_seeds_replay_without_a_class() {
        let corpus = Corpus::new(&[
            ("valid-a.bin", b"a" as &[u8]),
            ("da39a3ee5e6b4b0d3255bfef95601890afd80709", b"m"),
        ]);
        let seen = Cell::new(0usize);
        let n = SeedReplay::dir(&corpus.0)
            .each(|_, _| seen.set(seen.get() + 1))
            .covers("valid-")
            .run();
        assert_eq!(n, 2, "the machine seed counts toward the replayed total");
        assert_eq!(seen.get(), 2, "the machine seed runs the oracle hook");
    }

    #[test]
    fn unclassified_seed_fails_the_walk() {
        let corpus = Corpus::new(&[("valid-a.bin", b"a"), ("stray-b.bin", b"b")]);
        let result = catch_unwind(AssertUnwindSafe(|| {
            SeedReplay::dir(&corpus.0).covers("valid-").run()
        }));
        assert!(panic_message(result).contains("stray-b.bin"));
    }

    #[test]
    fn stale_prefix_fails_the_walk() {
        let corpus = Corpus::new(&[("valid-a.bin", b"a")]);
        let result = catch_unwind(AssertUnwindSafe(|| {
            SeedReplay::dir(&corpus.0)
                .covers("valid-")
                .covers("crash-")
                .run()
        }));
        assert!(panic_message(result).contains("\"crash-\""));
    }

    #[test]
    fn floor_violation_fails_the_walk() {
        let corpus = Corpus::new(&[("valid-a.bin", b"a")]);
        let result = catch_unwind(AssertUnwindSafe(|| {
            SeedReplay::dir(&corpus.0).covers("valid-").floor(2).run()
        }));
        assert!(panic_message(result).contains("at least 2"));
    }

    #[test]
    fn missing_directory_fails_the_walk() {
        let result = catch_unwind(AssertUnwindSafe(|| {
            SeedReplay::dir("/nonexistent/seed/dir").run()
        }));
        assert!(panic_message(result).contains("must exist"));
    }
}
