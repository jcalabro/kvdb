//! Redis glob-style pattern matching for PSUBSCRIBE.
//!
//! Redis patterns support:
//! - `*` matches any sequence of characters (including empty)
//! - `?` matches exactly one character
//! - `[abc]` matches one character in the set
//! - `[a-z]` matches one character in the range
//! - `[^abc]` or `[!abc]` matches one character NOT in the set
//! - `\x` escapes the next character (literal match)
//!
//! This is compiled once at PSUBSCRIBE time and evaluated on each
//! PUBLISH for pattern subscribers.

/// A compiled Redis glob pattern.
#[derive(Debug, Clone)]
pub struct GlobMatcher {
    /// The original pattern bytes (for display/comparison).
    pattern: Vec<u8>,
    /// Compiled pattern segments.
    segments: Vec<Segment>,
}

#[derive(Debug, Clone)]
enum Segment {
    /// Match any sequence of characters (including empty).
    Star,
    /// Match exactly one character.
    Question,
    /// Match a literal byte.
    Literal(u8),
    /// Match one byte in the set (negated if `negate` is true).
    CharClass { members: Vec<ClassEntry>, negate: bool },
}

#[derive(Debug, Clone)]
enum ClassEntry {
    /// A single byte.
    Single(u8),
    /// An inclusive byte range.
    Range(u8, u8),
}

impl GlobMatcher {
    /// Compile a Redis glob pattern.
    pub fn new(pattern: &[u8]) -> Self {
        let segments = compile(pattern);
        Self {
            pattern: pattern.to_vec(),
            segments,
        }
    }

    /// Test whether `input` matches this pattern.
    pub fn matches(&self, input: &[u8]) -> bool {
        matches_recursive(&self.segments, input)
    }

    /// Return the original pattern bytes.
    pub fn pattern(&self) -> &[u8] {
        &self.pattern
    }
}

/// Compile pattern bytes into segments.
fn compile(pattern: &[u8]) -> Vec<Segment> {
    let mut segments = Vec::new();
    let mut i = 0;

    while i < pattern.len() {
        match pattern[i] {
            b'*' => {
                segments.push(Segment::Star);
                i += 1;
            }
            b'?' => {
                segments.push(Segment::Question);
                i += 1;
            }
            b'\\' => {
                // Escape: next char is literal.
                i += 1;
                if i < pattern.len() {
                    segments.push(Segment::Literal(pattern[i]));
                    i += 1;
                }
                // Trailing backslash: ignore (matches Redis behavior).
            }
            b'[' => {
                i += 1;
                let (class, consumed) = parse_char_class(&pattern[i..]);
                segments.push(class);
                i += consumed;
            }
            ch => {
                segments.push(Segment::Literal(ch));
                i += 1;
            }
        }
    }

    segments
}

/// Parse a character class starting after the opening `[`.
/// Returns the segment and how many bytes were consumed (including
/// the closing `]`).
fn parse_char_class(input: &[u8]) -> (Segment, usize) {
    let mut i = 0;
    let mut negate = false;
    let mut members = Vec::new();

    // Check for negation.
    if i < input.len() && (input[i] == b'^' || input[i] == b'!') {
        negate = true;
        i += 1;
    }

    // A `]` immediately after `[` or `[^` is treated as a literal
    // member, matching Redis behavior.
    if i < input.len() && input[i] == b']' {
        members.push(ClassEntry::Single(b']'));
        i += 1;
    }

    while i < input.len() {
        if input[i] == b']' {
            i += 1; // consume the closing bracket
            return (Segment::CharClass { members, negate }, i);
        }

        if input[i] == b'\\' && i + 1 < input.len() {
            // Escaped character inside class.
            i += 1;
            members.push(ClassEntry::Single(input[i]));
            i += 1;
            continue;
        }

        // Check for range: `a-z`.
        if i + 2 < input.len() && input[i + 1] == b'-' && input[i + 2] != b']' {
            let lo = input[i];
            let hi = input[i + 2];
            members.push(ClassEntry::Range(lo, hi));
            i += 3;
        } else {
            members.push(ClassEntry::Single(input[i]));
            i += 1;
        }
    }

    // Unclosed bracket: treat the whole thing as literal `[` + chars.
    // This is defensive — Redis would also not match well here.
    (Segment::CharClass { members, negate }, i)
}

/// Recursive matching with backtracking for `*`.
fn matches_recursive(segments: &[Segment], input: &[u8]) -> bool {
    let mut si = 0; // segment index
    let mut ii = 0; // input index

    // Track the last `*` position for backtracking.
    let mut star_si: Option<usize> = None;
    let mut star_ii: usize = 0;

    while ii < input.len() || si < segments.len() {
        if si < segments.len() {
            match &segments[si] {
                Segment::Star => {
                    // Record backtrack point: try matching zero chars first.
                    star_si = Some(si);
                    star_ii = ii;
                    si += 1;
                    continue;
                }
                Segment::Question => {
                    if ii < input.len() {
                        si += 1;
                        ii += 1;
                        continue;
                    }
                }
                Segment::Literal(ch) => {
                    if ii < input.len() && input[ii] == *ch {
                        si += 1;
                        ii += 1;
                        continue;
                    }
                }
                Segment::CharClass { members, negate } => {
                    if ii < input.len() {
                        let byte = input[ii];
                        let in_class = members.iter().any(|entry| match entry {
                            ClassEntry::Single(ch) => byte == *ch,
                            ClassEntry::Range(lo, hi) => byte >= *lo && byte <= *hi,
                        });
                        if in_class != *negate {
                            si += 1;
                            ii += 1;
                            continue;
                        }
                    }
                }
            }
        }

        // Current segment didn't match. Try backtracking to last `*`.
        if let Some(ssi) = star_si {
            si = ssi + 1;
            star_ii += 1;
            ii = star_ii;
            if ii <= input.len() {
                continue;
            }
        }

        // No backtrack available: mismatch.
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pattern: &str, input: &str) -> bool {
        GlobMatcher::new(pattern.as_bytes()).matches(input.as_bytes())
    }

    #[test]
    fn literal_match() {
        assert!(m("hello", "hello"));
        assert!(!m("hello", "hell"));
        assert!(!m("hello", "helloo"));
        assert!(!m("hello", "world"));
    }

    #[test]
    fn star_wildcard() {
        assert!(m("*", ""));
        assert!(m("*", "anything"));
        assert!(m("h*o", "hello"));
        assert!(m("h*o", "ho"));
        assert!(m("h*o", "hellooo"));
        assert!(!m("h*o", "hellooop"));
        assert!(m("*.*", "news.sports"));
        assert!(m("news.*", "news.sports"));
        assert!(m("news.*", "news."));
        assert!(!m("news.*", "news"));
    }

    #[test]
    fn question_mark() {
        assert!(m("h?llo", "hello"));
        assert!(m("h?llo", "hallo"));
        assert!(!m("h?llo", "hllo"));
        assert!(!m("h?llo", "heello"));
    }

    #[test]
    fn char_class() {
        assert!(m("h[ae]llo", "hello"));
        assert!(m("h[ae]llo", "hallo"));
        assert!(!m("h[ae]llo", "hillo"));
    }

    #[test]
    fn char_class_range() {
        assert!(m("h[a-e]llo", "hallo"));
        assert!(m("h[a-e]llo", "hello"));
        assert!(!m("h[a-e]llo", "hzllo"));
    }

    #[test]
    fn negated_char_class() {
        assert!(!m("h[^ae]llo", "hello"));
        assert!(!m("h[^ae]llo", "hallo"));
        assert!(m("h[^ae]llo", "hillo"));
        // Also test with `!` negation
        assert!(!m("h[!ae]llo", "hello"));
        assert!(m("h[!ae]llo", "hillo"));
    }

    #[test]
    fn escape() {
        assert!(m("h\\*llo", "h*llo"));
        assert!(!m("h\\*llo", "hello"));
        assert!(m("h\\?llo", "h?llo"));
        assert!(!m("h\\?llo", "hello"));
    }

    #[test]
    fn complex_patterns() {
        // Redis-like channel patterns
        assert!(m("__keyevent@*__:*", "__keyevent@0__:set"));
        assert!(m("news.*", "news.sports"));
        assert!(m("news.*", "news.art.music"));
        assert!(m("user.[0-9]*", "user.42"));
        assert!(!m("user.[0-9]*", "user.abc"));
    }

    #[test]
    fn empty_pattern_and_input() {
        assert!(m("", ""));
        assert!(!m("", "a"));
        assert!(m("*", ""));
    }

    #[test]
    fn multiple_stars() {
        assert!(m("*.*.*", "a.b.c"));
        assert!(m("*.*.*", ".."));
        assert!(!m("*.*.*", "a.b"));
    }
}
