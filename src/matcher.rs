/// Return true if `candidate` matches `pattern`. This is a very simple
/// wildcard match where '*' matches on any number of characters
/// and '?' matches on any one character.
/// pattern:
///   { term }
/// term:
/// 	 '*'         matches any sequence of non-Separator characters
/// 	 '?'         matches any single non-Separator character
/// 	 c           matches character c (c != '*', '?')
/// 	'\\' c       matches character c
pub fn matches(pattern: &str, candidate: &str) -> bool {
    let mut plen = pattern.len();
    let mut slen = candidate.len();
    let mut pattern_index = 0;
    let mut candidate_index = 0;
    let pat_chars: Vec<char> = pattern.chars().collect();
    let candidate_chars: Vec<char> = candidate.chars().collect();

    while plen > 0 {
        if pat_chars[pattern_index] == '\\' {
            if plen == 1 {
                return false;
            }
            pattern_index += 1;
            plen -= 1;
        } else if pat_chars[pattern_index] == '*' {
            if plen == 1 {
                return true;
            }
            if pat_chars[pattern_index + 1] == '*' {
                pattern_index += 1;
                plen -= 1;
                continue;
            }
            if matches(&pattern[pattern_index + 1..], &candidate[candidate_index..]) {
                return true;
            }

            if slen == 0 {
                return false;
            }

            candidate_index += 1;
            slen -= 1;
            continue;
        }

        if slen == 0 {
            return false;
        }

        if pat_chars[pattern_index] != '?'
            && candidate_chars[candidate_index] != pat_chars[pattern_index]
        {
            return false;
        }

        pattern_index += 1;
        plen -= 1;
        candidate_index += 1;
        slen -= 1;
    }

    slen == 0 && plen == 0
}

pub fn allowable(pattern: &str) -> (String, String) {
    assert!(!pattern.is_empty());
    assert_ne!(pattern.chars().nth(0).unwrap(), '*');

    let min = String::new();
    let max = String::new();

    todo!();

    (min, max)
}

#[test]
fn test_matches() {
    assert!(matches("*", ""));
    assert!(matches("", ""));

    assert!(!matches("", "hello world"));
    assert!(!matches("jello world", "hello world"));

    assert!(matches("*", "hello world"));
    assert!(matches("*world*", "hello world"));
    assert!(matches("*world", "hello world"));
    assert!(matches("hello*", "hello world"));
    assert!(!matches("jello*", "hello world"));
    assert!(matches("hello?world", "hello world"));
    assert!(!matches("jello?world", "hello world"));
    assert!(matches("he*o?world", "hello world"));
    assert!(matches("he*o?wor*", "hello world"));
    assert!(matches("he*o*r*", "hello world"));
    assert!(matches("h\\*ello", "h*ello"));
    assert!(!matches("hello\\", "hello\\"));
    assert!(matches("hello\\?", "hello?"));
    assert!(matches("hello\\\\", "hello\\"));
}
