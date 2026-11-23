//! Reading the CSV files: rows, their `Time` column, and the feed's clock.
//!
//! The files are in arrival order, but a row's `Time` cannot be used as the arrival time directly. Early in every
//! trading day's file there are snapshot rows with a much later `Time` (8 Nov, file row 41: 09:12:16; a whole block
//! around row 31,000: 17:35), and later in the day there are rows that are hours late. Pacing by the raw `Time` would
//! wait for the first kind and send everything before it as one burst.
//!
//! The feed's clock is therefore the median `Time` of each block of rows, and the median of that over the
//! neighbouring blocks, which removes blocks that are off as a whole. For a feed in order, both leave the time
//! unchanged.

use std::collections::VecDeque;
use std::io::{BufRead, Result};

/// Rows per reading of the feed's clock (a quarter of a second at the open).
const BLOCK: usize = 1000;
/// The clock of a block is the median over this many blocks on both sides, so up to this many blocks in a row can be
/// off. On the seven DEBS files 4 is enough.
const NEIGHBOURS: usize = 10;
/// Rows of the first (last) block may be this much before (after) its clock, as it has no neighbour on that side.
const EDGE_MS: i64 = 60_000;

/// "HH:MM:SS" or "HH:MM:SS.mmm" → ms since midnight.
pub fn clock_ms(t: &[u8]) -> Option<i64> {
    let num = |b: &[u8]| -> Option<i64> {
        if b.is_empty() || !b.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(b).ok()?.parse().ok()
    };
    if t.len() < 8 || t[2] != b':' || t[5] != b':' {
        return None;
    }
    let (h, m, sec) = (num(&t[0..2])?, num(&t[3..5])?, num(&t[6..8])?);
    let ms = match t.get(8) {
        Some(b'.') => num(&t[9..])? * 10i64.pow(3u32.saturating_sub((t.len() - 9) as u32)),
        None => 0,
        _ => return None,
    };
    Some(((h * 60 + m) * 60 + sec) * 1000 + ms)
}

/// The `Time` column (4th field: ID,SecType,Date,Time,...) of a CSV row, in ms since midnight.
pub fn row_time(line: &[u8]) -> Option<i64> {
    let mut fields = line.split(|&b| b == b',');
    clock_ms(fields.nth(3)?)
}

/// The next data row without its line end; None at the end of the file.
pub fn read_row(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>> {
    loop {
        let mut line = Vec::with_capacity(128);
        if reader.read_until(b'\n', &mut line)? == 0 {
            return Ok(None);
        }
        // strip "\n" / "\r\n", like Java's readLine()
        while matches!(line.last(), Some(b'\n' | b'\r')) {
            line.pop();
        }
        // DEBS files: '#' comment lines (licence, column descriptions) and the "ID,SecType,..." header
        if line.iter().all(u8::is_ascii_whitespace) || line.starts_with(b"#") || line.starts_with(b"ID,") {
            continue;
        }
        return Ok(Some(line));
    }
}

type Row = (Vec<u8>, Option<i64>);

fn median(values: &mut [i64]) -> Option<i64> {
    values.sort_unstable();
    values.get(values.len() / 2).copied()
}

/// Gives every row, in file order, the time at which the feed delivered it (ms since midnight): never decreasing,
/// and the row's own `Time` unless that is off the feed's clock.
#[derive(Default)]
pub struct FeedClock {
    /// Median `Time` of each block read so far
    clocks: Vec<i64>,
    /// Blocks read ahead, not yet handed out
    ahead: VecDeque<Vec<Row>>,
    /// Index of the next block to hand out
    next: usize,
    end_of_file: bool,
    rows: std::vec::IntoIter<Row>,
    /// Bounds for the rows of the current block: the clocks of the blocks before and after it
    floor: i64,
    cap: i64,
    last: Option<i64>,
}

impl FeedClock {
    pub fn next_row(&mut self, reader: &mut impl BufRead) -> Result<Option<(Vec<u8>, i64)>> {
        loop {
            if let Some((line, time)) = self.rows.next() {
                let time = time.map_or(self.floor, |t| t.min(self.cap).max(self.floor));
                let time = self.last.map_or(time, |last| time.max(last));
                self.last = Some(time);
                return Ok(Some((line, time)));
            }
            if !self.next_block(reader)? {
                return Ok(None);
            }
        }
    }

    fn next_block(&mut self, reader: &mut impl BufRead) -> Result<bool> {
        let i = self.next;
        while !self.end_of_file && self.clocks.len() <= i + 1 + NEIGHBOURS {
            self.read_block(reader)?;
        }
        let Some(rows) = self.ahead.pop_front() else { return Ok(false) };
        self.floor = if i == 0 { self.clock(0) - EDGE_MS } else { self.clock(i - 1) };
        self.cap = if i + 1 < self.clocks.len() { self.clock(i + 1) } else { self.clock(i) + EDGE_MS };
        self.rows = rows.into_iter();
        self.next += 1;
        Ok(true)
    }

    fn read_block(&mut self, reader: &mut impl BufRead) -> Result<()> {
        let mut rows = Vec::with_capacity(BLOCK);
        while rows.len() < BLOCK {
            let Some(line) = read_row(reader)? else {
                self.end_of_file = true;
                break;
            };
            let time = row_time(&line);
            rows.push((line, time));
        }
        if !rows.is_empty() {
            let mut times: Vec<i64> = rows.iter().filter_map(|r| r.1).collect();
            // a block without any time keeps the clock of the one before
            let clock = median(&mut times).or(self.clocks.last().copied()).unwrap_or(0);
            self.clocks.push(clock);
            self.ahead.push_back(rows);
        }
        Ok(())
    }

    /// The window is symmetric, so it shrinks at both ends of the file: a lopsided one would shift the clock there.
    fn clock(&self, block: usize) -> i64 {
        let k = NEIGHBOURS.min(block).min(self.clocks.len() - 1 - block);
        let mut window = self.clocks[block - k..=block + k].to_vec();
        median(&mut window).expect("the window contains the block")
    }
}

#[cfg(test)]
mod tests {
    use super::{BLOCK, FeedClock, clock_ms, read_row, row_time};
    use std::io::Cursor;

    #[test]
    fn parses_feed_times() {
        assert_eq!(clock_ms(b"09:05:01.250"), Some(((9 * 60 + 5) * 60 + 1) * 1000 + 250));
        assert_eq!(clock_ms(b"09:05:01.5"), Some(((9 * 60 + 5) * 60 + 1) * 1000 + 500));
        assert_eq!(clock_ms(b"09:05:01"), Some(((9 * 60 + 5) * 60 + 1) * 1000));
        assert_eq!(clock_ms(b""), None);
        assert_eq!(clock_ms(b"9:05"), None);
        assert_eq!(row_time(b"A2ASZ7.ETR,E,08-11-2021,01:01:07.000,,,"), Some(3_667_000));
        assert_eq!(row_time(b"A2ASZ7.ETR,E"), None);
    }

    #[test]
    fn skips_comments_header_and_blank_lines() {
        let mut r = Cursor::new(&b"# licence\r\nID,SecType,Date,Time\n\n  \nA.FR,E\r\nB.FR,E"[..]);
        assert_eq!(read_row(&mut r).unwrap().as_deref(), Some(&b"A.FR,E"[..]));
        assert_eq!(read_row(&mut r).unwrap().as_deref(), Some(&b"B.FR,E"[..]));
        assert_eq!(read_row(&mut r).unwrap(), None);
    }

    const OPEN: i64 = 9 * 3_600_000;

    fn row(ms: i64) -> String {
        let s = ms / 1000;
        format!("A.FR,E,08-11-2021,{:02}:{:02}:{:02}.{:03},,\n", s / 3600, s / 60 % 60, s % 60, ms % 1000)
    }

    /// The feed times of rows given by their `Time` in ms.
    fn feed_times(times: &[i64]) -> Vec<i64> {
        let csv: String = times.iter().map(|&t| row(t)).collect();
        let (mut clock, mut r, mut out) = (FeedClock::default(), Cursor::new(csv), Vec::new());
        while let Some((_, t)) = clock.next_row(&mut r).unwrap() {
            out.push(t);
        }
        out
    }

    /// 60 blocks, one row every 10 ms.
    fn in_order() -> Vec<i64> {
        (0..60 * BLOCK as i64).map(|i| OPEN + 10 * i).collect()
    }

    #[test]
    fn rows_in_order_keep_their_time() {
        assert_eq!(feed_times(&in_order()), in_order());
        assert_eq!(feed_times(&in_order()[..200]), &in_order()[..200]); // less than a block
        assert_eq!(feed_times(&[]), Vec::<i64>::new());
    }

    #[test]
    fn stray_rows_and_blocks_do_not_move_the_clock() {
        let mut times = in_order();
        times[40] = OPEN + 736_665; // early rows with a much later time, as at the start of every file
        times[157] = OPEN + 1_267_348;
        times[30_500] = 0; // a late row
        times[31_000..33_000].fill(17 * 3_600_000 + 35 * 60_000); // snapshot blocks, later ...
        times[33_000..34_000].fill(0); // ... and earlier than the feed
        let feed = feed_times(&times);
        assert!(feed.is_sorted());
        let error = feed.iter().zip(in_order()).map(|(f, t)| (f - t).abs()).max().unwrap();
        // Each of the 3 blocks shifts the median by at most one block (10 s here), plus the neighbour's clock and
        // half a block; without the median the clock would jump by hours.
        assert!(error <= 45 * BLOCK as i64, "off by {error} ms");
        assert_eq!(feed[10_000], in_order()[10_000]);
        assert_eq!(feed[50_000], in_order()[50_000]); // recovered
    }

    #[test]
    fn rows_without_a_time_get_the_clock() {
        let csv: String = (0..3000).map(|i| if i % 3 == 0 { "A.FR,E,,,\n".into() } else { row(OPEN + 10 * i) }).collect();
        let (mut clock, mut r, mut n) = (FeedClock::default(), Cursor::new(csv), 0);
        while let Some((_, t)) = clock.next_row(&mut r).unwrap() {
            assert!((OPEN - 60_000..=OPEN + 30_000).contains(&t));
            n += 1;
        }
        assert_eq!(n, 3000);
    }
}
