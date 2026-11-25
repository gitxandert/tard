// `tard server watch`: takes over the terminal (its alternate screen, like less or tmux)
// with a live view of the server: running and waiting jobs on top, the server's event
// log below. q puts the terminal back exactly as it was.
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicBool, Ordering},
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    time::{Duration, Instant},
};
use crossterm::{
    cursor, execute, queue,
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    style::{Attribute, Print, SetAttribute},
    terminal::{self, ClearType},
};
use crate::config::Config;
use crate::server::server_log_path;

/// How often the server is asked for its state. Fetching happens off the UI thread.
const FETCH_EVERY: Duration = Duration::from_millis(500);
/// How long the UI waits for a key before checking for new state or log lines.
const FRAME: Duration = Duration::from_millis(50);
const MAX_LINES: usize = 10_000;
const TAIL_BYTES: u64 = 256 << 10;

/// Fetches `args`' stdout from the server; None if it isn't running.
pub type Fetch<'a> = dyn Fn(&[&str]) -> Option<String> + Sync + 'a;

pub fn run(config: &Config, fetch: &Fetch) -> io::Result<()> {
    let mut out = io::stdout();
    terminal::enable_raw_mode()?;
    let _restore = Restore; // leaves the alternate screen even on panic or error
    execute!(out, terminal::EnterAlternateScreen, cursor::Hide, terminal::Clear(ClearType::All))?;

    let stop = AtomicBool::new(false);
    let (tx, rx) = crossbeam_channel::bounded::<(String, String)>(1);
    std::thread::scope(|scope| {
        // the server round trips happen here, so a slow answer never stalls keys or drawing
        scope.spawn(|| {
            while !stop.load(Ordering::Relaxed) {
                let state = match fetch(&["ping"]) {
                    Some(p) => (p.trim().trim_start_matches("server ").to_string(), fetch(&["ls"]).unwrap_or_default()),
                    None => (format!("not running (port {})", config.server.port), "start it with: tard server start".into()),
                };
                if tx.send(state).is_err() {
                    return;
                }
                let until = Instant::now() + FETCH_EVERY;
                while Instant::now() < until && !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(FRAME);
                }
            }
        });
        let r = ui(&mut out, &rx);
        stop.store(true, Ordering::Relaxed);
        drop(rx); // unblocks a pending send
        r
    })
}

fn ui(out: &mut io::Stdout, rx: &crossbeam_channel::Receiver<(String, String)>) -> io::Result<()> {
    let mut log = LogTail::new();
    let mut scroll = 0usize; // lines up from the bottom of the event log
    let (mut status, mut jobs) = ("connecting...".to_string(), String::new());
    let mut screen = Screen::default();
    loop {
        if let Ok((s, j)) = rx.try_recv() {
            (status, jobs) = (s, j);
        }
        log.read_new();
        scroll = scroll.min(log.lines.len());
        screen.draw(out, &frame(&status, &jobs, &log.lines, scroll)?)?;

        if !event::poll(FRAME)? {
            continue;
        }
        let page = terminal::size().map_or(10, |(_, h)| (h as usize / 2).max(1));
        match event::read()? {
            Event::Key(k) if k.kind != KeyEventKind::Release => match k.code {
                KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                KeyCode::Char('c') if k.modifiers.contains(KeyModifiers::CONTROL) => return Ok(()),
                KeyCode::Up | KeyCode::Char('k') => scroll += 1,
                KeyCode::Down | KeyCode::Char('j') => scroll = scroll.saturating_sub(1),
                KeyCode::PageUp => scroll += page,
                KeyCode::PageDown => scroll = scroll.saturating_sub(page),
                KeyCode::Home | KeyCode::Char('g') => scroll = log.lines.len(),
                KeyCode::End | KeyCode::Char('G') => scroll = 0,
                _ => (),
            },
            Event::Resize(..) => screen.invalidate(),
            _ => (),
        }
    }
}

struct Restore;

impl Drop for Restore {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), cursor::Show, terminal::LeaveAlternateScreen);
        let _ = terminal::disable_raw_mode();
    }
}

/// What's on the terminal, so each frame only rewrites the rows that changed. Nothing
/// is cleared between frames; rows are padded instead, so there's no flicker.
#[derive(Default)]
struct Screen {
    rows: Vec<String>,
    size: (usize, usize),
}

impl Screen {
    fn invalidate(&mut self) {
        self.rows.clear();
    }

    fn draw(&mut self, out: &mut impl Write, (rows, size): &(Vec<String>, (usize, usize))) -> io::Result<()> {
        if *size != self.size {
            self.size = *size;
            self.rows.clear();
            queue!(out, terminal::Clear(ClearType::All))?;
        }
        let changed: Vec<usize> = (0..rows.len()).filter(|&i| self.rows.get(i) != Some(&rows[i])).collect();
        if changed.is_empty() {
            return Ok(());
        }
        // terminals that support it show the whole frame at once
        queue!(out, terminal::BeginSynchronizedUpdate)?;
        for i in changed {
            queue!(out, cursor::MoveTo(0, i as u16))?;
            if i == 0 {
                queue!(out, SetAttribute(Attribute::Reverse), Print(&rows[i]), SetAttribute(Attribute::Reset))?;
            } else {
                queue!(out, Print(&rows[i]))?;
            }
        }
        queue!(out, terminal::EndSynchronizedUpdate)?;
        self.rows = rows.clone();
        out.flush()
    }
}

/// The whole screen as rows padded to the width (one column short, so writing the last
/// row never scrolls a terminal that wraps at the edge), plus the size it was built for.
fn frame(status: &str, jobs: &str, log: &VecDeque<String>, scroll: usize) -> io::Result<(Vec<String>, (usize, usize))> {
    let (w, h) = terminal::size()?;
    let (w, h) = (w as usize, h as usize);
    let mut rows: Vec<String> = Vec::with_capacity(h);

    let keys = "q quit  ↑↓/PgUp/PgDn scroll  End follow ";
    let title = format!(" tard server · {}", status);
    let used = title.chars().count() + keys.chars().count();
    rows.push(if used + 2 <= w {
        format!("{}{}{}", title, " ".repeat(w - used), keys)
    } else {
        title // too narrow for the key hints
    });

    // jobs get up to half the screen; the event log gets the rest
    let job_lines: Vec<&str> = jobs.lines().collect();
    let job_room = (h.saturating_sub(3) / 2).max(1);
    rows.extend(job_lines.iter().take(job_room).map(|l| format!(" {}", l)));
    if job_lines.len() > job_room {
        rows.push(format!(" ... {} more (tard server ls)", job_lines.len() - job_room));
    }

    let follow = if scroll == 0 { "live".to_string() } else { format!("{} lines up", scroll) };
    let label = format!(" server log ({}) ", follow);
    rows.push(format!("──{}{}", label, "─".repeat(w.saturating_sub(label.chars().count() + 2))));
    let room = h.saturating_sub(rows.len());
    let end = log.len().saturating_sub(scroll);
    let start = end.saturating_sub(room);
    rows.extend(log.range(start..end).map(|l| format!(" {}", l)));
    rows.resize(h, String::new());

    let rows = rows.into_iter().enumerate()
        .map(|(i, r)| if i == 0 { fit(&r, w, true) } else { fit(&r, w.saturating_sub(1), true) })
        .collect();
    Ok((rows, (w, h)))
}

/// Cuts `s` to `w` columns, padding with spaces when `pad`.
fn fit(s: &str, w: usize, pad: bool) -> String {
    let mut t: String = s.chars().take(w).collect();
    if pad {
        let n = t.chars().count();
        t.extend(std::iter::repeat_n(' ', w.saturating_sub(n)));
    }
    t
}

/// The end of server.log, kept up to date as it grows.
struct LogTail {
    lines: VecDeque<String>,
    pos: u64,
    partial: String,
}

impl LogTail {
    fn new() -> Self {
        let mut t = Self { lines: VecDeque::new(), pos: 0, partial: String::new() };
        if let Ok(len) = std::fs::metadata(server_log_path()).map(|m| m.len()) {
            t.pos = len.saturating_sub(TAIL_BYTES);
            t.read_new();
            if t.pos > 0 && len > TAIL_BYTES {
                t.lines.pop_front(); // probably cut mid-line
            }
        }
        t
    }

    fn read_new(&mut self) {
        let Ok(mut f) = File::open(server_log_path()) else { return };
        let len = f.metadata().map_or(0, |m| m.len());
        if len < self.pos {
            self.pos = 0; // truncated or replaced
        }
        if f.seek(SeekFrom::Start(self.pos)).is_err() {
            return;
        }
        let mut buf = Vec::new();
        if f.read_to_end(&mut buf).is_err() {
            return;
        }
        self.pos += buf.len() as u64;
        self.partial.push_str(&String::from_utf8_lossy(&buf));
        while let Some(i) = self.partial.find('\n') {
            let l: String = self.partial.drain(..=i).collect();
            self.lines.push_back(l.trim_end().to_string());
        }
        while self.lines.len() > MAX_LINES {
            self.lines.pop_front();
        }
    }
}
