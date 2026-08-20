//! The result table, the refusal contrast and the put-window guidance.

use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::time::Duration;

use nectar_postage_issuer::Window;
use nectar_testing::bench::RunMeta;

use crate::arms::{BODY, Outcome, Refusal};

#[derive(Debug, Clone, Copy)]
pub struct Row {
    pub arm: &'static str,
    pub latency: Duration,
    pub put_window: u16,
    pub bytes: usize,
    pub outcome: Outcome,
    /// This cell's byte rate over the plain split's at the same put window.
    pub of_plain: f64,
}

/// Little's law over the measured rate: the slots the inline decorator needs
/// to sustain `bytes_per_second` at `latency` a signature.
pub fn recommended_put_window(bytes_per_second: f64, latency: Duration) -> u16 {
    let rate = if bytes_per_second.is_finite() && bytes_per_second > 0.0 {
        bytes_per_second as u64
    } else {
        0
    };
    Window::for_throughput(rate, latency, NonZeroUsize::new(BODY).unwrap()).get()
}

fn header(meta: &RunMeta, bytes: usize, sign_window: u16) -> String {
    format!(
        "wrapped-split throughput\n\
         generated {}\nbranch {} at {}\nharness {}\n\
         corpus {bytes} B, body {BODY} B, sign window {sign_window}\n",
        meta.generated, meta.git_branch, meta.git_commit, meta.harness_version,
    )
}

fn table(rows: &[Row]) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "\n{:<8} {:>8} {:>10} {:>12} {:>9} {:>11} {:>9}",
        "arm", "sign ms", "put slots", "chunks/s", "MiB/s", "peak signs", "of plain"
    );
    for row in rows {
        let _ = writeln!(
            out,
            "{:<8} {:>8} {:>10} {:>12.0} {:>9.1} {:>11} {:>8.2}x",
            row.arm,
            row.latency.as_millis(),
            row.put_window,
            row.outcome.chunks_per_second(),
            row.outcome.bytes_per_second(row.bytes) / (1024.0 * 1024.0),
            row.outcome.peak_signs,
            row.of_plain,
        );
    }
    out
}

fn refusals(inline: Refusal, staged: Refusal) -> String {
    format!(
        "\nrefused allocation, three puts of one address against two bucket slots\n\
         inline: {} pairs delivered, shut afterwards {}\n\
         staged: {} pairs delivered, shut afterwards {}\n",
        inline.delivered, inline.shut, staged.delivered, staged.shut,
    )
}

fn guidance(plain_bytes_per_second: f64) -> String {
    let mut out = String::from("\nput-window width the inline decorator needs\n");
    for latency in [10, 50, 250].map(Duration::from_millis) {
        let _ = writeln!(
            out,
            "  {:>4} ms a signature: {:>5} slots to hold the plain split's rate",
            latency.as_millis(),
            recommended_put_window(plain_bytes_per_second, latency),
        );
    }
    out.push_str(
        "  these widths bound what the inline arm could reach, not what it does: it \
overlaps at most min(put window, sign pool) signatures\n\
         \x20 the staged decorator needs none of them: its put window sizes from store \
latency alone\n",
    );
    out
}

fn caveats() -> String {
    String::from(
        "\ncaveats\n\
         - The signer is a sleep, not ECDSA. The sweep measures how signer latency \
propagates, not signature cost.\n\
         - Sign jobs run one thread each, so the staged arm reports the concurrency a \
remote signer gives, not what a local key would.\n\
         - Without the `parallel` feature the inline decorator signs on the split's own \
thread, so it overlaps nothing and its peak reads 1. With `parallel` it signs on the rayon \
pool, so its peak is min(put window, pool width).\n\
         - Wall time on a shared host carries the usual noise. Only the structural \
multipliers here are meaningful; small deltas are not.\n",
    )
}

pub fn render(
    meta: &RunMeta,
    bytes: usize,
    sign_window: u16,
    rows: &[Row],
    plain_bytes_per_second: f64,
    refusal: (Refusal, Refusal),
) -> String {
    let mut out = header(meta, bytes, sign_window);
    out.push_str(&table(rows));
    out.push_str(&refusals(refusal.0, refusal.1));
    out.push_str(&guidance(plain_bytes_per_second));
    out.push_str(&caveats());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(arm: &'static str, latency: Duration) -> Row {
        Row {
            arm,
            latency,
            put_window: 16,
            bytes: 65_536,
            outcome: Outcome {
                elapsed: Duration::from_millis(10),
                delivered: 17,
                peak_signs: 4,
            },
            of_plain: 1.0,
        }
    }

    #[test]
    fn the_recommended_width_grows_with_signer_latency() {
        let rate = 4.0 * 1024.0 * 1024.0;
        let narrow = recommended_put_window(rate, Duration::from_millis(10));
        let wide = recommended_put_window(rate, Duration::from_millis(50));
        assert!(narrow >= 1 && wide > narrow, "{narrow} then {wide}");
        assert_eq!(recommended_put_window(0.0, Duration::from_millis(50)), 1);
        assert_eq!(recommended_put_window(f64::NAN, Duration::ZERO), 1);
    }

    #[test]
    fn the_table_renders_one_line_per_cell() {
        let rows = [
            row("plain", Duration::ZERO),
            row("stamped", Duration::from_millis(50)),
            row("staged", Duration::from_millis(50)),
        ];
        // The leading blank line, the column heads, then the cells.
        assert_eq!(table(&rows).lines().count(), 5);
    }

    #[test]
    fn the_document_carries_the_refusal_contrast_and_the_caveats() {
        let rendered = render(
            &RunMeta::current("test"),
            65_536,
            256,
            &[row("plain", Duration::ZERO)],
            4.0 * 1024.0 * 1024.0,
            (
                Refusal {
                    delivered: 3,
                    shut: false,
                },
                Refusal {
                    delivered: 2,
                    shut: true,
                },
            ),
        );
        assert!(rendered.contains("inline: 3 pairs delivered, shut afterwards false"));
        assert!(rendered.contains("staged: 2 pairs delivered, shut afterwards true"));
        assert!(rendered.contains("caveats"));
    }
}
