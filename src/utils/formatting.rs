use std::time::Duration;

pub fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

/// "42s", "4m05s", "2h07m", "3d04h"
pub fn format_duration(d: Duration) -> String {
    let s = d.as_secs();
    if s < 60 {
        format!("{}s", s)
    } else if s < 3600 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else if s < 2 * 86_400 {
        format!("{}h{:02}m", s / 3600, s / 60 % 60)
    } else {
        format!("{}d{:02}h", s / 86_400, s / 3600 % 24)
    }
}

/// "512KB", "64MB", "1GB", "1TB", "100B" or a bare byte count.
pub fn parse_size(s: &str) -> Result<usize, String> {
    let suffixes = [
        ("TB", 1usize << 40),
        ("GB", 1usize << 30),
        ("MB", 1usize << 20),
        ("KB", 1usize << 10),
        ("B", 1usize),
    ];
    let s = s.trim();

    for (suffix, multiplier) in &suffixes {
        if let Some(num_str) = s.strip_suffix(suffix) {
            return num_str
                .trim_end()
                .parse::<usize>()
                .ok()
                .and_then(|n| n.checked_mul(*multiplier))
                .ok_or_else(|| format!("invalid size '{}'", s));
        }
    }

    s.parse::<usize>()
        .map_err(|_| format!("invalid size '{}' (use e.g. 512KB, 64MB, 1GB)", s))
}

/// "90s", "30m", "12h", "2d", "1w".
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let err = || format!("invalid duration '{}' (use e.g. 30m, 12h, 2d, 1w)", s);
    let split = s.find(|c: char| !c.is_ascii_digit()).ok_or_else(err)?;
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().map_err(|_| err())?;
    let secs = match unit.trim() {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        "w" => 604_800,
        _ => return Err(err()),
    };
    n.checked_mul(secs).map(Duration::from_secs).ok_or_else(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes() {
        assert_eq!(parse_size("10MB"), Ok(10 << 20));
        assert_eq!(parse_size("4 GB"), Ok(4 << 30));
        assert_eq!(parse_size("123"), Ok(123));
        assert!(parse_size("MB").is_err());
        assert!(parse_size("1.5GB").is_err());
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("90s"), Ok(Duration::from_secs(90)));
        assert_eq!(parse_duration("12h"), Ok(Duration::from_secs(12 * 3600)));
        assert_eq!(parse_duration("2w"), Ok(Duration::from_secs(2 * 604_800)));
        assert!(parse_duration("h").is_err());
        assert!(parse_duration("5y").is_err());
    }
}
