use once_cell::sync::OnceCell;
use std::time;

/// DbItemOpts holds various meta information about an item.
#[derive(Clone, Eq, PartialEq)]
pub struct DbItemOpts {
    /// does this item expire?
    pub ex: bool,
    /// when does this item expire?
    pub exat: time::SystemTime,
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct DbItem {
    // the binary key
    pub key: String,
    // the binary value
    pub val: String,
    // optional meta information
    pub opts: Option<DbItemOpts>,
    // keyless item for scanning
    pub keyless: bool,
}

// This is a long time in the future. It's an imaginary number that is
// used for b-tree ordering.
static MAX_TIME: OnceCell<time::SystemTime> = OnceCell::new();

fn get_max_time() -> time::SystemTime {
    *MAX_TIME.get_or_init(|| time::SystemTime::now() + time::Duration::MAX)
}

impl DbItem {
    // expired evaluates id the item has expired. This will always return false when
    // the item does not have `opts.ex` set to true.
    pub fn expired(&self) -> bool {
        if let Some(opts) = &self.opts {
            return opts.ex && opts.exat < time::SystemTime::now();
        }

        false
    }

    // expiresAt will return the time when the item will expire. When an item does
    // not expire `maxTime` is used.
    pub fn expires_at(&self) -> time::SystemTime {
        if let Some(opts) = &self.opts {
            if !opts.ex {
                return get_max_time();
            }

            return opts.exat;
        }

        get_max_time()
    }

    // writeSetTo writes an item as a single SET record to the a bufio Writer.
    pub fn write_set_to(&self, buf: &mut Vec<u8>) {
        if let Some(opts) = &self.opts {
            if opts.ex {
                let now = time::SystemTime::now();
                let ex = opts.exat.duration_since(now).unwrap_or_default().as_secs();
                append_array(buf, 5);
                append_bulk_string(buf, "set");
                append_bulk_string(buf, &self.key);
                append_bulk_string(buf, &self.val);
                append_bulk_string(buf, "ex");
                append_bulk_string(buf, &format!("{}", ex));
                return;
            }
        }

        append_array(buf, 3);
        append_bulk_string(buf, "set");
        append_bulk_string(buf, &self.key);
        append_bulk_string(buf, &self.val);
    }

    // writeDeleteTo deletes an item as a single DEL record to the a bufio Writer.
    pub fn write_delete_to(&self, buf: &mut Vec<u8>) {
        append_array(buf, 2);
        append_bulk_string(buf, "del");
        append_bulk_string(buf, &self.key);
    }
}

fn append_array(buf: &mut Vec<u8>, count: i64) {
    buf.extend(format!("*{}\r\n", count).as_bytes());
}

fn append_bulk_string(buf: &mut Vec<u8>, s: &str) {
    buf.extend(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
}
