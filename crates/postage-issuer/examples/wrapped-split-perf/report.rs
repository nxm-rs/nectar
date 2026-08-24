//! The result table, the refusal note and the caveats.

use std::fmt::Write as _;
use std::time::Duration;

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

fn refusal(staged: Refusal) -> String {
    format!(
        "\nrefused allocation, three puts of one address against two bucket slots\n\
         staged: {} pairs delivered, shut afterwards {}\n",
        staged.delivered, staged.shut,
    )
}

fn caveats() -> String {
    String::from(
        "\ncaveats\n\
         - The signer is a sleep, not ECDSA. The sweep measures how signer latency \
propagates, not signature cost.\n\
         - Sign jobs run one thread each, so the staged arm reports the concurrency a \
remote signer gives, not what a local key would.\n\
         - Wall time on a shared host carries the usual noise. Only the structural \
multipliers here are meaningful; small deltas are not.\n",
    )
}

pub fn render(
    meta: &RunMeta,
    bytes: usize,
    sign_window: u16,
    rows: &[Row],
    staged: Refusal,
) -> String {
    let mut out = header(meta, bytes, sign_window);
    out.push_str(&table(rows));
    out.push_str(&refusal(staged));
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
    fn the_table_renders_one_line_per_cell() {
        let rows = [
            row("plain", Duration::ZERO),
            row("staged", Duration::from_millis(50)),
            row("staged", Duration::from_millis(10)),
        ];
        // The leading blank line, the column heads, then the cells.
        assert_eq!(table(&rows).lines().count(), 5);
    }

    #[test]
    fn the_document_carries_the_refusal_note_and_the_caveats() {
        let rendered = render(
            &RunMeta::current("test"),
            65_536,
            256,
            &[row("plain", Duration::ZERO)],
            Refusal {
                delivered: 2,
                shut: true,
            },
        );
        assert!(rendered.contains("staged: 2 pairs delivered, shut afterwards true"));
        assert!(rendered.contains("caveats"));
    }
}
