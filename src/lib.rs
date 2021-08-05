//! Package buntdb implements a low-level in-memory key/value store in pure Go.
//! It persists to disk, is ACID compliant, and uses locking for multiple
//! readers and a single writer. Bunt is ideal for projects that need a
//! dependable database, and favor speed over data size.

use btreec::BTreeC;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::time;

mod btree_helpers;
mod index;
mod item;
mod tx;

use crate::btree_helpers::btree_ascend_greater_or_equal;
use crate::btree_helpers::btree_ascend_less_than;
use crate::index::*;
use crate::item::DbItem;
use crate::item::DbItemOpts;
use crate::tx::*;

type RectFn = dyn Fn(String) -> (Vec<f64>, Vec<f64>) + Send + Sync;
type LessFn = dyn Fn(&str, &str) -> bool + Send + Sync;

#[derive(Debug)]
pub enum DbError {
    // ErrTxNotWritable is returned when performing a write operation on a
    // read-only transaction.
    TxNotWritable,

    // ErrTxClosed is returned when committing or rolling back a transaction
    // that has already been committed or rolled back.
    TxClosed,

    // ErrNotFound is returned when an item or index is not in the database.
    NotFound,

    // ErrInvalid is returned when the database file is an invalid format.
    Invalid,

    // ErrDatabaseClosed is returned when the database is closed.
    DatabaseClosed,

    // ErrIndexExists is returned when an index already exists in the database.
    IndexExists,

    // ErrInvalidOperation is returned when an operation cannot be completed.
    InvalidOperation,

    // ErrInvalidSyncPolicy is returned for an invalid SyncPolicy value.
    InvalidSyncPolicy,

    // ErrShrinkInProcess is returned when a shrink operation is in-process.
    ShrinkInProcess,

    // ErrPersistenceActive is returned when post-loading data from an database
    // not opened with Open(":memory:").
    PersistenceActive,

    // ErrTxIterating is returned when Set or Delete are called while iterating.
    TxIterating,

    Io(io::Error),

    // FIXME: there should be more general error handling than relying on internal
    // type for user errors
    Custom(String),
}

impl From<io::Error> for DbError {
    fn from(error: io::Error) -> Self {
        DbError::Io(error)
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use DbError::*;
        match self {
            TxNotWritable => write!(f, "tx not writable"),
            TxClosed => write!(f, "tx closed"),
            NotFound => write!(f, "not found"),
            Invalid => write!(f, "invalid database"),
            DatabaseClosed => write!(f, "database closed"),
            IndexExists => write!(f, "index exists"),
            InvalidOperation => write!(f, "invalid operation"),
            InvalidSyncPolicy => write!(f, "invalid sync policy"),
            ShrinkInProcess => write!(f, "shrink is in-process"),
            PersistenceActive => write!(f, "persistence active"),
            TxIterating => write!(f, "tx is iterating"),
            Io(s) => write!(f, "{}", s.to_string()),
            Custom(s) => write!(f, "{}", s),
        }
    }
}

impl Error for DbError {}

pub(crate) enum DbLock<'db> {
    Read(RwLockReadGuard<'db, DbInner>),
    Write(RwLockWriteGuard<'db, DbInner>),
}

impl<'db> DbLock<'db> {
    fn as_ref(&self) -> &DbInner {
        match self {
            DbLock::Read(g) => &g,
            DbLock::Write(g) => &g,
        }
    }

    fn as_mut(&mut self) -> &mut DbInner {
        match self {
            DbLock::Read(_) => panic!("read-only transaction as_mut()"),
            DbLock::Write(g) => g,
        }
    }
}

pub(crate) struct DbInner {
    /// the underlying file
    file: Option<File>,

    /// a buffer to write to
    buf: Vec<u8>,

    /// a tree of all item ordered by key
    keys: BTreeC<DbItem>,

    /// a tree of items ordered by expiration
    exps: BTreeC<DbItem>,

    /// the index trees.
    idxs: HashMap<String, Index>,

    // TODO: use me
    /// a reuse buffer for gathering indexes
    #[allow(unused)]
    ins_idxs: Vec<Index>,

    /// a count of the number of disk flushes
    flushes: i64,

    /// set when the database has been closed
    closed: bool,

    /// the database configuration
    config: Config,

    /// do we write to disk
    persist: bool,

    /// when an aof shrink is in-process.
    shrinking: bool,

    /// the size of the last shrink aof size
    lastaofsz: u64,
}

impl DbInner {
    /// get return an item or nil if not found.
    pub fn get(&self, key: String) -> Option<&DbItem> {
        self.keys.get(DbItem {
            key,
            ..Default::default()
        })
    }

    /// insertIntoDatabase performs inserts an item in to the database and updates
    /// all indexes. If a previous item with the same key already exists, that item
    /// will be replaced with the new one, and return the previous item.
    pub(crate) fn insert_into_database(&mut self, item: DbItem) -> Option<DbItem> {
        // Generate a list of indexes that this item will be inserted into
        let mut ins_idxs = vec![];
        for (_, idx) in self.idxs.iter_mut() {
            if idx.matches(&item.key) {
                ins_idxs.push(idx);
            }
        }

        let maybe_prev = self.keys.set(item.clone()).map(|p| p.to_owned());
        if let Some(prev) = &maybe_prev {
            // A previous item was removed from the keys tree. Let's
            // full delete this item from all indexes.
            if let Some(opts) = &prev.opts {
                if opts.ex {
                    self.exps.delete(prev.clone());
                }
            }
            for idx in ins_idxs.iter_mut() {
                if let Some(btr) = idx.btr.as_mut() {
                    // Remove it from the btree index
                    btr.delete(item.clone());
                }
                //     if let Some(rtr) = idx.rtr.as_mut() {
                //         // Remove it from the rtree index
                //         rtr.delete()
                //     }
            }
        }
        if let Some(opts) = &item.opts {
            if opts.ex {
                // The new item has eviction options. Add it to the
                // expires tree.
                self.exps.set(item.clone());
            }
        }
        for idx in ins_idxs.drain(..) {
            if let Some(btr) = idx.btr.as_mut() {
                // Remove it from the btree index
                btr.set(item.clone());
            }
            // TODO:
            // if let Some(rtr) = idx.rtr.as_mut() {
            //     // Remove it from the rtree index
            //     rtr.set(item.clone())
            // }
        }

        // we must return previous item to the caller
        maybe_prev
    }

    /// deleteFromDatabase removes and item from the database and indexes. The input
    /// item must only have the key field specified thus "&dbItem{key: key}" is all
    /// that is needed to fully remove the item with the matching key. If an item
    /// with the matching key was found in the database, it will be removed and
    /// returned to the caller. A nil return value means that the item was not
    /// found in the database
    pub fn delete_from_database(&mut self, item: DbItem) -> Option<DbItem> {
        let maybe_prev = self.keys.delete(item.clone()).map(|p| p.to_owned());

        if let Some(prev) = &maybe_prev {
            if let Some(opts) = &prev.opts {
                if opts.ex {
                    // Remove it from the expires tree.
                    self.exps.delete(prev.clone());
                }
            }
            for (_, idx) in self.idxs.iter_mut() {
                if !idx.matches(&item.key) {
                    continue;
                }
                if let Some(btr) = idx.btr.as_mut() {
                    // Remove it from the btree index
                    btr.delete(prev.clone());
                }
                // TODO:
                //     if let Some(rtr) = idx.rtr.as_mut() {
                //         // Remove it from the rtree index
                //         rtr.delete()
                //     }
            }
        }

        maybe_prev
    }
}

/// Db represents a collection of key-value pairs that persist on disk.
/// Transactions are used for all forms of data access to the Db.
#[derive(Clone)]
pub struct Db(Arc<RwLock<DbInner>>);

/// SyncPolicy represents how often data is synced to disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncPolicy {
    /// Never is used to disable syncing data to disk.
    /// The faster and less safe method.
    Never,

    /// EverySecond is used to sync data to disk every second.
    /// It's pretty fast and you can lose 1 second of data if there
    /// is a disaster.
    /// This is the recommended setting.
    EverySecond,

    /// Always is used to sync data after every write to disk.
    /// Slow. Very safe.
    Always,
}

impl Default for SyncPolicy {
    fn default() -> Self {
        Self::EverySecond
    }
}

pub type OnExpiredFn = dyn for<'e> Fn(Vec<String>) + Send + Sync;
pub type OnExpiredSyncFn =
    dyn for<'e> Fn(String, String, &mut Tx) -> Result<(), DbError> + Send + Sync;

// Config represents database configuration options. These
// options are used to change various behaviors of the database.
#[derive(Clone, Default)]
pub struct Config {
    // SyncPolicy adjusts how often the data is synced to disk.
    // This value can be Never, EverySecond, or Always.
    // The default is EverySecond.
    sync_policy: SyncPolicy,

    // `auto_shrink_percentage` is used by the background process to trigger
    // a shrink of the aof file when the size of the file is larger than the
    // percentage of the result of the previous shrunk file.
    // For example, if this value is 100, and the last shrink process
    // resulted in a 100mb file, then the new aof file must be 200mb before
    // a shrink is triggered.
    auto_shrink_percentage: u64,

    // `auto_shrink_min_size` defines the minimum size of the aof file before
    // an automatic shrink can occur.
    auto_shrink_min_size: u64,

    // auto_shrink_disabled turns off automatic background shrinking
    auto_shrink_disabled: bool,

    // `on_expired` is used to custom handle the deletion option when a key
    // has been expired.
    on_expired: Option<Arc<OnExpiredFn>>,

    // `on_expired_sync` will be called inside the same transaction that is
    // performing the deletion of expired items. If OnExpired is present then
    // this callback will not be called. If this callback is present, then the
    // deletion of the timeed-out item is the explicit responsibility of this
    // callback.
    on_expired_sync: Option<Arc<OnExpiredSyncFn>>,
}

fn keys_compare_fn(a: &DbItem, b: &DbItem) -> Ordering {
    if a.keyless {
        return Ordering::Less;
    } else if b.keyless {
        return Ordering::Greater;
    }
    a.key.cmp(&b.key)
}

fn exps_compare_fn(a: &DbItem, b: &DbItem) -> Ordering {
    // The expires b-tree formula
    if b.expires_at() > a.expires_at() {
        return Ordering::Greater;
    }
    if a.expires_at() > b.expires_at() {
        return Ordering::Less;
    }

    if a.keyless {
        return Ordering::Less;
    } else if b.keyless {
        return Ordering::Greater;
    }
    a.key.cmp(&b.key)
}

impl Db {
    pub fn open(path: &str) -> Result<Db, io::Error> {
        // initialize default configuration
        let config = Config {
            auto_shrink_percentage: 100,
            auto_shrink_min_size: 32 * 1024 * 1024,
            ..Default::default()
        };

        let mut inner = DbInner {
            file: None,
            buf: Vec::new(),
            keys: BTreeC::new(Box::new(keys_compare_fn)),
            exps: BTreeC::new(Box::new(exps_compare_fn)),
            idxs: HashMap::new(),
            ins_idxs: Vec::new(),
            flushes: 0,
            closed: false,
            config,
            persist: path != ":memory:",
            shrinking: false,
            lastaofsz: 0,
        };

        if inner.persist {
            // hardcoding 0666 as the default mode.
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path)?;
            inner.file = Some(file);

            // load the database from disk
            // TODO:
            // if let Err(err) = db.load_from_disk() {
            //     // close on error, ignore close error
            //     db.file.take();
            //     return Err(err);
            // }
        }

        let db = Db(Arc::new(RwLock::new(inner)));
        // TODO:
        // start the background manager
        db.background_manager();

        Ok(db)
    }

    /// `close` releases all database resources.
    /// All transactions must be closed before closing the database.
    pub fn close(&mut self) -> Result<(), DbError> {
        let mut db = self.0.write();

        if db.closed {
            return Err(DbError::DatabaseClosed);
        }

        db.closed = true;
        if db.persist {
            let file = db.file.take().unwrap();
            // do a sync but ignore the error
            let _ = file.sync_all();
            drop(file);
        }

        Ok(())
    }

    /// `save` writes a snapshot of the database to a writer. This operation blocks all
    /// writes, but not reads. This can be used for snapshots and backups for pure
    /// in-memory databases using the ":memory:". Database that persist to disk
    /// can be snapshotted by simply copying the database file.
    pub fn save(&mut self, writer: &mut dyn io::Write) -> Result<(), io::Error> {
        let db = self.0.read();
        let mut err = None;
        // use a buffered writer and flush every 4MB
        let mut buf = Vec::with_capacity(4 * 1024 * 1024);
        // iterate through every item in the database and write to the buffer
        db.keys.ascend(None, |item| {
            item.write_set_to(&mut buf);

            if buf.len() > 4 * 1024 * 1024 {
                // flush when buffer is over 4MB
                if let Err(e) = writer.write_all(&buf) {
                    err = Some(e);
                    return false;
                }
                buf.clear();
            }

            true
        });

        if let Some(e) = err {
            return Err(e);
        }

        // one final flush
        if !buf.is_empty() {
            writer.write_all(&buf)?;
        }

        Ok(())
    }

    /// `read_load` reads from the reader and loads commands into the database.
    /// modTime is the modified time of the reader, should be no greater than
    /// the current time.Now().
    /// Returns the number of bytes of the last command read and the error if any.
    pub fn read_load(
        &self,
        reader: Box<dyn io::Read>,
        mod_time: time::Instant,
    ) -> (usize, Option<DbError>) {
        let mut total_size: usize = 0;
        let mut data = Vec::with_capacity(4096);

        use io::BufRead;
        use io::Read;

        let mut buf_reader = io::BufReader::new(reader);
        loop {
            // TODO: discrepancy with Go version, not checking
            // for nul byte first
            // TODO: check if we've reached EOF

            // read a single command
            // first we should read the number of parts that command has
            let mut cmd_byte_size = 0;
            let mut line = String::new();
            let r = buf_reader.read_line(&mut line);
            match r {
                Ok(n) => {
                    // we've reached EOF
                    if n == 0 {
                        return (total_size, None);
                    }
                }
                Err(err) => {
                    return (total_size, Some(DbError::from(err)));
                }
            }
            if !line.starts_with('*') {
                return (total_size, Some(DbError::Invalid));
            }
            cmd_byte_size += line.len();

            // TODO: need to strip leading '*'
            // convert the string number to an int
            let n = line.parse::<usize>().unwrap();
            // read each part of the command
            let mut parts = vec![];
            for _ in 0..n {
                // read the number of bytes of the part.
                let mut line = String::new();
                let r = buf_reader.read_line(&mut line);
                match r {
                    Ok(n) => {
                        if n == 0 {
                            let e = io::Error::new(io::ErrorKind::UnexpectedEof, "");
                            return (total_size, Some(DbError::from(e)));
                        }
                    }
                    Err(err) => {
                        return (total_size, Some(DbError::from(err)));
                    }
                }
                if let Err(err) = r {
                    return (total_size, Some(DbError::from(err)));
                }
                if !line.starts_with('$') {
                    return (total_size, Some(DbError::Invalid));
                }
                cmd_byte_size += line.len();
                // TODO: need to strip leading '$'
                // convert the string number to an int
                let n = line.parse::<usize>().unwrap();

                // resize the read buffer
                if data.capacity() < n + 2 {
                    let mut dataln = data.capacity();
                    while dataln < n + 2 {
                        dataln *= 2;
                    }
                    data.reserve(dataln - data.capacity());
                }
                let r = buf_reader.read_exact(&mut data[0..n + 2]);
                if let Err(err) = r {
                    return (total_size, Some(DbError::from(err)));
                }
                if let Err(err) = r {
                    return (total_size, Some(DbError::from(err)));
                }
                if data[n] != b'\r' || data[n + 1] != b'\n' {
                    return (total_size, Some(DbError::Invalid));
                }
                // copy string
                parts.push(String::from_utf8(data[..n].to_vec()).unwrap());
                cmd_byte_size += n + 2;
            }
            // finished reading the command

            if parts.is_empty() {
                continue;
            }

            if parts[0].to_lowercase() == "set" {
                // SET
                if parts.len() < 3 || parts.len() == 4 || parts.len() > 5 {
                    return (total_size, Some(DbError::Invalid));
                }
                if parts.len() == 5 {
                    if parts[3].to_lowercase() != "ex" {
                        return (total_size, Some(DbError::Invalid));
                    }
                    let ex = parts[4].parse::<u64>().unwrap();
                    let now = time::Instant::now();
                    let dur = time::Duration::from_secs(ex) - (time::Instant::now() - mod_time);
                    if dur.as_secs() > 0 {
                        let exat = time::Instant::now() + dur;
                        let mut db = self.0.write();
                        db.insert_into_database(DbItem {
                            key: parts[1].to_string(),
                            val: parts[2].to_string(),
                            opts: Some(DbItemOpts { ex: true, exat }),
                            keyless: false,
                        });
                    }
                } else {
                    let mut db = self.0.write();
                    db.insert_into_database(DbItem {
                        key: parts[1].to_string(),
                        val: parts[2].to_string(),
                        ..Default::default()
                    });
                }
            } else if parts[0].to_lowercase() == "del" {
                // DEL
                if parts.len() != 2 {
                    return (total_size, Some(DbError::Invalid));
                }
                let mut db = self.0.write();
                db.delete_from_database(DbItem {
                    key: parts[1].to_string(),
                    ..Default::default()
                });
            } else if parts[0].to_lowercase() == "flushdb" {
                let mut db = self.0.write();
                db.keys = BTreeC::new(Box::new(keys_compare_fn));
                db.exps = BTreeC::new(Box::new(exps_compare_fn));
                db.idxs = HashMap::default();
            } else {
                return (total_size, Some(DbError::Invalid));
            }
            total_size += cmd_byte_size;
        }
    }

    /// `load_from_disk` reads entries from the append only database file and fills the database.
    /// The file format uses the Redis append only file format, which is and a series
    /// of RESP commands. For more information on RESP please read
    /// http://redis.io/topics/protocol. The only supported RESP commands are DEL and
    /// SET.
    #[allow(unused)]
    fn load_from_disk(&mut self) -> Result<(), DbError> {
        let boxed_file = {
            let mut db = self.0.write();
            let f = db.file.as_ref().unwrap().try_clone().unwrap();
            Box::new(f)
        };

        let metadata = boxed_file.metadata()?;
        let mod_time = metadata.modified()?;

        use std::convert::TryInto;
        let (n, maybe_err) = self.read_load(boxed_file, mod_time.try_into().unwrap());
        let n = n as u64;

        if let Some(err) = maybe_err {
            if let DbError::Io(e) = err {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    // The db file has ended mid-command, which is allowed but the
                    // data file should be truncated to the end of the last valid
                    // command
                    let mut db = self.0.write();
                    let mut file = db.file.as_mut().unwrap();
                    file.set_len(n)?;
                }
            } else {
                return Err(err);
            }
        }

        use std::io::Seek;
        let mut db = self.0.write();
        let mut file = db.file.as_mut().unwrap();
        let pos = file.seek(io::SeekFrom::Start(n))?;
        db.lastaofsz = pos;
        Ok(())
    }

    /// `load` loads commands from reader. This operation blocks all reads and writes.
    /// Note that this can only work for fully in-memory databases opened with
    /// Open(":memory:").
    pub fn load(&mut self, reader: Box<dyn io::Read>) -> Result<(), DbError> {
        let db = self.0.write();

        if db.persist {
            return Err(DbError::PersistenceActive);
        }

        let (_, maybe_err) = self.read_load(reader, time::Instant::now());

        if let Some(err) = maybe_err {
            return Err(err);
        }

        Ok(())
    }

    /// CreateIndex builds a new index and populates it with items.
    /// The items are ordered in an b-tree and can be retrieved using the
    /// Ascend* and Descend* methods.
    /// An error will occur if an index with the same name already exists.
    ///
    /// When a pattern is provided, the index will be populated with
    /// keys that match the specified pattern. This is a very simple pattern
    /// match where '*' matches on any number characters and '?' matches on
    /// any one character.
    /// The less function compares if string 'a' is less than string 'b'.
    /// It allows for indexes to create custom ordering. It's possible
    /// that the strings may be textual or binary. It's up to the provided
    /// less function to handle the content format and comparison.
    /// There are some default less function that can be used such as
    /// IndexString, IndexBinary, etc.
    pub fn create_index(
        &mut self,
        name: String,
        pattern: String,
        less: Vec<Arc<LessFn>>,
    ) -> Result<(), DbError> {
        self.update(move |tx| tx.create_index(name, pattern, less))
    }

    /// ReplaceIndex builds a new index and populates it with items.
    /// The items are ordered in an b-tree and can be retrieved using the
    /// Ascend* and Descend* methods.
    /// If a previous index with the same name exists, that index will be deleted.
    pub fn replace_index(
        &mut self,
        name: String,
        pattern: String,
        less: Vec<Arc<LessFn>>,
    ) -> Result<(), DbError> {
        self.update(move |tx| {
            if let Err(err) = tx.create_index(name.clone(), pattern.clone(), less.clone()) {
                if matches!(err, DbError::IndexExists) {
                    if let Err(err) = tx.drop_index(name.clone()) {
                        return Err(err);
                    }
                    return tx.create_index(name, pattern, less);
                }
                return Err(err);
            }
            Ok(())
        })
    }

    // CreateSpatialIndex builds a new index and populates it with items.
    // The items are organized in an r-tree and can be retrieved using the
    // Intersects method.
    // An error will occur if an index with the same name already exists.
    //
    // The rect function converts a string to a rectangle. The rectangle is
    // represented by two arrays, min and max. Both arrays may have a length
    // between 1 and 20, and both arrays must match in length. A length of 1 is a
    // one dimensional rectangle, and a length of 4 is a four dimension rectangle.
    // There is support for up to 20 dimensions.
    // The values of min must be less than the values of max at the same dimension.
    // Thus min[0] must be less-than-or-equal-to max[0].
    // The IndexRect is a default function that can be used for the rect
    // parameter.
    pub fn create_spatial_index(
        &mut self,
        name: String,
        pattern: String,
        rect: Arc<RectFn>,
    ) -> Result<(), DbError> {
        self.update(|tx| tx.create_spatial_index(name, pattern, rect))
    }

    // ReplaceSpatialIndex builds a new index and populates it with items.
    // The items are organized in an r-tree and can be retrieved using the
    // Intersects method.
    // If a previous index with the same name exists, that index will be deleted.
    pub fn replace_spatial_index(
        &mut self,
        name: String,
        pattern: String,
        rect: Arc<RectFn>,
    ) -> Result<(), DbError> {
        self.update(move |tx| {
            if let Err(err) = tx.create_spatial_index(name.clone(), pattern.clone(), rect.clone()) {
                if matches!(err, DbError::IndexExists) {
                    if let Err(err) = tx.drop_index(name.clone()) {
                        return Err(err);
                    }
                    return tx.create_spatial_index(name, pattern, rect);
                }
                return Err(err);
            }
            Ok(())
        })
    }

    /// DropIndex removes an index.
    pub fn drop_index(&mut self, name: String) -> Result<(), DbError> {
        self.update(|tx| tx.drop_index(name.clone()))
    }

    /// Indexes returns a list of index names.
    pub fn indexes(&mut self) -> Result<Vec<String>, DbError> {
        self.view(|tx| tx.indexes())
    }

    /// ReadConfig returns the database configuration.
    pub fn read_config(&self) -> Result<Config, DbError> {
        let db = self.0.read();
        if db.closed {
            return Err(DbError::DatabaseClosed);
        }
        let c = db.config.clone();
        Ok(c)
    }

    /// SetConfig updates the database configuration.
    pub fn set_config(&mut self, config: Config) -> Result<(), DbError> {
        let mut db = self.0.write();
        if db.closed {
            return Err(DbError::DatabaseClosed);
        }
        db.config = config;
        Ok(())
    }

    // Returns true if database has been closed.
    fn background_manager_inner(&mut self, flushes: &mut i64) -> bool {
        let mut shrink = false;
        let mut expired = vec![];
        let mut on_expired = None;
        let mut on_expired_sync = None;

        // Open a standard view. This will take a full lock of the
        // database thus allowing for access to anything we need.
        let update_result = self.update(|tx| {
            let db = tx.db_lock.as_mut().unwrap().as_mut();
            on_expired = db.config.on_expired.clone();

            if on_expired.is_none() {
                on_expired_sync = db.config.on_expired_sync.clone();
            }

            if db.persist && !db.config.auto_shrink_disabled {
                use std::io::Seek;
                let aofsz = db
                    .file
                    .as_mut()
                    .unwrap()
                    .seek(io::SeekFrom::Current(0))
                    .expect("Failed to get current file position");

                if aofsz > db.config.auto_shrink_min_size {
                    let prc: f64 = db.config.auto_shrink_percentage as f64 / 100.0;
                    let prcsz = db.lastaofsz as f64 * prc;
                    let prcsz = ((prcsz / 100_000.0).round() as u64) * 100_000;
                    shrink = aofsz > db.lastaofsz + prcsz;
                }
            }

            // produce a list of expired items that need removing
            let key_item = DbItem {
                opts: Some(DbItemOpts {
                    ex: true,
                    exat: time::Instant::now(),
                }),
                ..Default::default()
            };
            btree_ascend_less_than(&db.exps, &key_item, |k, v| {
                expired.push((k.to_string(), v.to_string()));
                true
            });
            if on_expired.is_none() && on_expired_sync.is_none() {
                for (key, _) in &expired {
                    if let Err(err) = tx.delete(key.to_string()) {
                        // it's ok to get a "not found" because the
                        // 'Delete' method reports "not found" for
                        // expired items.
                        if !matches!(err, DbError::NotFound) {
                            return Err(err);
                        }
                    }
                }
            } else if let Some(on_expired_sync_) = on_expired_sync {
                for (key, value) in &expired {
                    if let Err(err) = on_expired_sync_(key.to_string(), value.to_string(), tx) {
                        return Err(err);
                    }
                }
            }
            Ok(())
        });

        if let Err(err) = update_result {
            if matches!(err, DbError::DatabaseClosed) {
                return true;
            }
        }

        // send expired event, if needed
        if !expired.is_empty() {
            if let Some(on_expired_) = on_expired {
                let expired_keys = expired.into_iter().map(|(k, _)| k).collect();
                on_expired_(expired_keys);
            }
        }

        // execute a disk synk, if needed
        {
            let mut db = self.0.write();
            if db.persist
                && db.config.sync_policy == SyncPolicy::EverySecond
                && *flushes != db.flushes
            {
                let _ = db.file.as_mut().unwrap().sync_all();
                *flushes = db.flushes;
            }
        }

        if shrink {
            if let Err(err) = self.shrink() {
                if matches!(err, DbError::DatabaseClosed) {
                    return true;
                }
            }
        }

        false
    }

    /// backgroundManager runs continuously in the background and performs various
    /// operations such as removing expired items and syncing to disk.
    fn background_manager(&self) -> std::thread::JoinHandle<()> {
        let db = self.clone();
        // TODO: join handle should be saved in the struct?
        std::thread::spawn(move || {
            let mut flushes = 0;
            loop {
                // FIXME: this is naive, we'll be sleeping 1s between `background_manager_inner`
                // calls, instead of calling `background_manager_inner` every second
                std::thread::sleep(time::Duration::from_secs(1));

                if db.clone().background_manager_inner(&mut flushes) {
                    break;
                }
            }
        })
    }

    // TODO: this function uses to much `.unwrap()` and `expect()`, should
    // be rewritten to handle errors gracefully.
    /// Shrink will make the database file smaller by removing redundant
    /// log entries. This operation does not block the database.
    fn shrink(&mut self) -> Result<(), DbError> {
        let mut db = self.0.write();

        if db.closed {
            return Err(DbError::DatabaseClosed);
        }

        if !db.persist {
            // The database was opened with ":memory:" as the path.
            // There is no persistence, and no need to do anything here.
            return Ok(());
        }

        if db.shrinking {
            // The database is already in the process of shrinking.
            return Err(DbError::ShrinkInProcess);
        }

        db.shrinking = true;

        let file_name = "";
        let tmp_file_name = format!("{}.tmp", file_name);
        // the endpos is used to return to the end of the file when we are
        // finished writing all of the current items.
        use std::io::Seek;
        let endpos = db
            .file
            .as_mut()
            .unwrap()
            .seek(io::SeekFrom::End(0))
            .expect("Failed to seek db file.");

        drop(db);
        // wait just a bit before starting
        std::thread::sleep(time::Duration::from_millis(250));

        let mut temp_file =
            std::fs::File::create(&tmp_file_name).expect("Failed to create database file");

        // we are going to read items in as chunks as to not hold up the database
        // for too long.
        let mut buf = Vec::new();
        let mut pivot_key = "".to_string();
        let mut done = false;

        while !done {
            let db = self.0.read();
            if db.closed {
                return Err(DbError::DatabaseClosed);
            }
            done = true;
            let mut n = 0;
            let pivot_item = DbItem {
                key: pivot_key.to_string(),
                ..Default::default()
            };
            // TODO: extraneous DB lookup because iterator is using (key, value) instead of item ref
            btree_ascend_greater_or_equal(&db.keys, Some(pivot_item), |k, _v| {
                // 1000 items or 64MB buffer
                if n > 1000 || buf.len() > 64 * 1024 * 1024 {
                    pivot_key = k.to_string();
                    done = false;
                    return false;
                }
                let item = db.get(k.to_string()).unwrap();
                item.write_set_to(&mut buf);
                n += 1;
                true
            });
            if !buf.is_empty() {
                temp_file
                    .write_all(&buf)
                    .expect("Failed to write to temporary db file");
                buf.clear();
            }
        }

        // We reached this far so all of the items have been written to a new tmp
        // There's some more work to do by appending the new line from the aof
        // to the tmp file and finally swap the files out.
        {
            let mut db = self.0.write();
            if db.closed {
                return Err(DbError::DatabaseClosed);
            }
            // We are going to open a new version of the aof file so that we do
            // not change the seek position of the previous. This may cause a
            // problem in the future if we choose to use syscall file locking.
            let mut aof = std::fs::File::open(file_name).expect("Failed to open AOF db file");
            aof.seek(io::SeekFrom::Start(endpos))
                .expect("Failed to seek AOF db file");
            // Just copy all of the new command that have ocurred since we started the shrink process.
            std::io::copy(&mut aof, &mut temp_file).expect("Failed to sync tmp and AOF file");
            // Close all files
            drop(aof);
            drop(temp_file);
            drop(db.file.take());
            // Any failures below here is really bad. So just panic.
            std::fs::rename(&tmp_file_name, file_name).expect("Failed to rename tmp DB file");
            let db_file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(file_name)
                .expect("Failed to open new DB file");
            db.file = Some(db_file);
            let pos = db
                .file
                .as_mut()
                .unwrap()
                .seek(io::SeekFrom::End(0))
                .expect("Failed to seek new DB file");
            db.lastaofsz = pos;
        }

        let _ = std::fs::remove_file(&tmp_file_name);
        {
            let mut db = self.0.write();
            db.shrinking = false;
        }

        Ok(())
    }

    /// managed calls a block of code that is fully contained in a transaction.
    /// This method is intended to be wrapped by Update and View
    fn managed<F, R>(&mut self, writable: bool, func: F) -> Result<R, DbError>
    where
        F: FnOnce(&mut Tx) -> Result<R, DbError>,
    {
        let mut tx = self.begin(writable)?;
        let func_result = tx.with_managed(func);
        if let Err(err) = func_result {
            // The caller returned an error. We must rollback;
            let _ = tx.rollback();
            return Err(err);
        }
        let rt = func_result.unwrap();

        let result = if writable {
            // Everything went well. Lets commit
            tx.commit()
        } else {
            // read-only transaction can only roll back
            tx.rollback()
        };
        result.map(|_| rt)
    }

    /// View executes a function within a managed read-only transaction.
    /// When a non-nil error is returned from the function that error will be return
    /// to the caller of View().
    ///
    /// Executing a manual commit or rollback from inside the function will result
    /// in a panic.
    // TODO: it should give `&Tx` to the func
    pub fn view<F, R>(&mut self, func: F) -> Result<R, DbError>
    where
        F: FnOnce(&mut Tx) -> Result<R, DbError>,
    {
        self.managed(false, func)
    }

    /// Update executes a function within a managed read/write transaction.
    /// The transaction has been committed when no error is returned.
    /// In the event that an error is returned, the transaction will be rolled back.
    /// When a non-nil error is returned from the function, the transaction will be
    /// rolled back and the that error will be return to the caller of Update().
    ///
    /// Executing a manual commit or rollback from inside the function will result
    /// in a panic.
    pub fn update<F, R>(&mut self, func: F) -> Result<R, DbError>
    where
        F: FnOnce(&mut Tx) -> Result<R, DbError>,
    {
        self.managed(true, func)
    }

    // Begin opens a new transaction.
    // Multiple read-only transactions can be opened at the same time but there can
    // only be one read/write transaction at a time. Attempting to open a read/write
    // transactions while another one is in progress will result in blocking until
    // the current read/write transaction is completed.
    //
    // All transactions must be closed by calling Commit() or Rollback() when done.
    fn begin(&self, writable: bool) -> Result<Tx, DbError> {
        let db_lock = if writable {
            DbLock::Write(self.0.write())
        } else {
            DbLock::Read(self.0.read())
        };
        Tx::new(db_lock, writable)
    }
}

// SetOptions represents options that may be included with the Set() command.
pub struct SetOptions {
    // Expires indicates that the Set() key-value will expire
    expires: bool,
    // TTL is how much time the key-value will exist in the database
    // before being evicted. The Expires field must also be set to true.
    // TTL stands for Time-To-Live.
    ttl: time::Duration,
}

// rect is used by Intersects and Nearby
#[allow(unused)]
struct Rect {
    min: Vec<f64>,
    max: Vec<f64>,
}

// index_int is a helper function that returns true if 'a` is less than 'b'
pub fn index_int(a: &str, b: &str) -> bool {
    let ia = a.parse::<i64>().unwrap();
    let ib = b.parse::<i64>().unwrap();
    ia < ib
}

// IndexUint is a helper function that returns true if 'a' is less than 'b'.
// This compares uint64s that are added to the database using the
// Uint() conversion function.
pub fn index_uint(a: &str, b: &str) -> bool {
    let ia = a.parse::<u64>().unwrap();
    let ib = b.parse::<u64>().unwrap();
    ia < ib
}

// index_float is a helper function that returns true if 'a` is less than 'b'.
// This compares float64s that are added to the database using the
// Float() conversion function.
pub fn index_float(a: &str, b: &str) -> bool {
    let ia = a.parse::<f64>().unwrap();
    let ib = b.parse::<f64>().unwrap();
    ia < ib
}

// IndexString is a helper function that return true if 'a' is less than 'b'.
// This is a case-insensitive comparison. Use the IndexBinary() for comparing
// case-sensitive strings.
pub fn index_string(a: &str, b: &str) -> bool {
    a.to_lowercase().cmp(&b.to_lowercase()) == Ordering::Less
}

pub fn index_string_case_sensitive(a: &str, b: &str) -> bool {
    a.cmp(&b) == Ordering::Less
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! svec {
        ($($x:expr),*) => (vec![$($x.to_string()),*]);
    }

    fn test_open() -> Db {
        let _ = std::fs::remove_file("data.db");
        test_reopen(None)
    }

    fn test_reopen(maybe_db: Option<Db>) -> Db {
        test_reopen_delay(maybe_db, time::Duration::new(0, 0))
    }

    fn test_reopen_delay(maybe_db: Option<Db>, duration: time::Duration) -> Db {
        if let Some(mut db) = maybe_db {
            db.close().unwrap();
        }
        std::thread::sleep(duration);
        Db::open("data.db").unwrap()
    }

    fn test_close(mut db: Db) {
        let _ = db.close();
        // let _ = std::fs::remove_file("data.db");
    }

    #[test]
    fn save_load() {
        let _db = Db::open(":memory:").unwrap();
    }

    #[test]
    fn test_index_transaction() {
        let mut db = test_open();

        fn ascend(tx: &mut Tx, index: &str) -> Vec<String> {
            let mut vals = vec![];

            tx.ascend(index.to_string(), |key, val| {
                eprintln!("ascend {} {}", key, val);
                vals.push(key.to_string());
                vals.push(val.to_string());
                true
            })
            .unwrap();

            vals
        }

        fn ascend_equal(tx: &mut Tx, index: &str, vals: Vec<String>) {
            let vals2 = ascend(tx, index);
            assert_eq!(vals.len(), vals2.len(), "invalid size match");
            for i in 0..vals.len() {
                assert_eq!(vals[i], vals2[i], "invalid order");
            }
        }

        // test creating an index and adding items
        db.update(|tx| {
            tx.set("1".to_string(), "3".to_string(), None).unwrap();
            tx.set("2".to_string(), "2".to_string(), None).unwrap();
            tx.set("3".to_string(), "1".to_string(), None).unwrap();
            tx.create_index(
                "idx1".to_string(),
                "*".to_string(),
                vec![Arc::new(index_int)],
            )?;
            ascend_equal(tx, "idx1", svec!["3", "1", "2", "2", "1", "3"]);
            Ok(())
        })
        .unwrap();

        // test to see if the items persisted from previous transaction
        // test add item.
        // test force rollback.
        db.update::<_, ()>(|tx| {
            ascend_equal(tx, "idx1", svec!["3", "1", "2", "2", "1", "3"]);
            tx.set("4".to_string(), "0".to_string(), None).unwrap();
            ascend_equal(tx, "idx1", svec!["4", "0", "3", "1", "2", "2", "1", "3"]);
            Err(DbError::Custom("this is fine".to_string()))
        })
        .unwrap_err();

        // test to see if rollback happened
        db.view(|tx| {
            ascend_equal(tx, "idx1", svec!["3", "1", "2", "2", "1", "3"]);
            Ok(())
        })
        .unwrap();

        // del item, drop index, rollback
        db.update::<_, ()>(|tx| {
            tx.drop_index("idx1".to_string()).unwrap();
            Err(DbError::Custom("this is fine".to_string()))
        })
        .unwrap_err();

        // test to see if rollback happened
        db.view(|tx| {
            ascend_equal(tx, "idx1", svec!["3", "1", "2", "2", "1", "3"]);
            Ok(())
        })
        .unwrap();

        fn various(tx: &mut Tx) {
            // del item 3, add index 2, add item 4, test index 1 and 2.
            // flushdb, test index 1 and 2.
            // add item 1 and 2, add index 2 and 3, test index 2 and 3
            tx.delete("3".to_string()).unwrap();
            tx.create_index(
                "idx2".to_string(),
                "*".to_string(),
                vec![Arc::new(index_int)],
            )
            .unwrap();
            tx.set("4".to_string(), "0".to_string(), None).unwrap();
            ascend_equal(tx, "idx1", svec!["4", "0", "2", "2", "1", "3"]);
            ascend_equal(tx, "idx2", svec!["4", "0", "2", "2", "1", "3"]);
            tx.delete_all().unwrap();
            ascend_equal(tx, "idx1", svec![]);
            ascend_equal(tx, "idx2", svec![]);
            tx.set("1".to_string(), "3".to_string(), None).unwrap();
            tx.set("2".to_string(), "2".to_string(), None).unwrap();
            // FIXME: there should be unwraps here, but it panics on `IndexExists`.
            // It seems these are spurious? Indexes are not deleted by `delete_all()`
            let _ = tx.create_index(
                "idx1".to_string(),
                "*".to_string(),
                vec![Arc::new(index_int)],
            );
            let _ = tx.create_index(
                "idx2".to_string(),
                "*".to_string(),
                vec![Arc::new(index_int)],
            );
            ascend_equal(tx, "idx1", svec!["2", "2", "1", "3"]);
            ascend_equal(tx, "idx2", svec!["2", "2", "1", "3"]);
        }

        // various rollback
        db.update::<_, ()>(|tx| {
            various(tx);
            Err(DbError::Custom("this is fine".to_string()))
        })
        .unwrap_err();

        // test to see if the rollback happened
        db.view(|tx| {
            ascend_equal(tx, "idx1", svec!["3", "1", "2", "2", "1", "3"]);
            let err = tx.ascend("idx2".to_string(), |_, _| true).unwrap_err();
            assert!(matches!(err, DbError::NotFound));

            Ok(())
        })
        .unwrap();

        // various commit
        db.update(|tx| {
            various(tx);
            Ok(())
        })
        .unwrap();

        // test to see if commit happened
        db.view(|tx| {
            ascend_equal(tx, "idx1", svec!["2", "2", "1", "3"]);
            ascend_equal(tx, "idx2", svec!["2", "2", "1", "3"]);
            Ok(())
        })
        .unwrap();

        test_close(db);
    }

    #[test]
    #[ignore]
    fn test_delete_all() {
        let mut db = test_open();

        db.update(|tx| {
            tx.set("hello1".to_string(), "planet1".to_string(), None)?;
            tx.set("hello2".to_string(), "planet2".to_string(), None)?;
            tx.set("hello3".to_string(), "planet3".to_string(), None)?;
            Ok(())
        })
        .unwrap();
        db.create_index(
            "all".to_string(),
            "*".to_string(),
            vec![Arc::new(index_string)],
        )
        .unwrap();
        db.update(|tx| {
            tx.set("hello1".to_string(), "planet1.1".to_string(), None)?;
            tx.delete_all()?;
            tx.set("bb".to_string(), "11".to_string(), None)?;
            tx.set("aa".to_string(), "**".to_string(), None)?;
            tx.delete("aa".to_string())?;
            tx.set("aa".to_string(), "22".to_string(), None)?;
            Ok(())
        })
        .unwrap();
        let mut res = String::new();
        let mut res2 = String::new();
        db.view(|tx| {
            tx.ascend("".to_string(), |key, val| {
                res.push_str(key);
                res.push(':');
                res.push_str(val);
                res.push('\n');
                true
            })
            .unwrap();
            tx.ascend("all".to_string(), |key, val| {
                res2.push_str(key);
                res2.push(':');
                res2.push_str(val);
                res2.push('\n');
                true
            })
            .unwrap();
            Ok(())
        })
        .unwrap();
        assert_eq!(res, "aa:22\nbb:11\n");
        assert_eq!(res2, "bb:11\naa:22\n");
        db = test_reopen(Some(db));
        res = String::new();
        res2 = String::new();
        db.create_index(
            "all".to_string(),
            "*".to_string(),
            vec![Arc::new(index_string)],
        )
        .unwrap();
        db.view(|tx| {
            tx.ascend("".to_string(), |key, val| {
                res.push_str(key);
                res.push(':');
                res.push_str(val);
                res.push('\n');
                true
            })
            .unwrap();
            tx.ascend("all".to_string(), |key, val| {
                res2.push_str(key);
                res2.push(':');
                res2.push_str(val);
                res2.push('\n');
                true
            })
            .unwrap();
            Ok(())
        })
        .unwrap();
        assert_eq!(res, "aa:22\nbb:11\n");
        assert_eq!(res2, "bb:11\naa:22\n");

        test_close(db);
    }

    #[test]
    fn test_ascend_equal() {
        let mut db = test_open();

        db.update(|tx| {
            for i in 0..300 {
                tx.set(format!("key:{:05}A", i), format!("{}", i + 1000), None)
                    .unwrap();
                tx.set(format!("key:{:05}B", i), format!("{}", i + 1000), None)
                    .unwrap();
            }
            tx.create_index(
                "num".to_string(),
                "*".to_string(),
                vec![Arc::new(index_int)],
            )
        })
        .unwrap();

        let mut res = vec![];
        let res_mut = &mut res;
        db.view(|tx| {
            tx.ascend_equal("", "key:00055A", |k, _| {
                res_mut.push(k.to_string());
                true
            })
        })
        .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res, svec!["key:00055A"]);

        res = vec![];
        let res_mut = &mut res;

        db.view(|tx| {
            tx.ascend_equal("num", "1125", |k, _| {
                res_mut.push(k.to_string());
                true
            })
        })
        .unwrap();

        assert_eq!(res.len(), 2);
        assert_eq!(res, svec!["key:00125A", "key:00125B"]);

        test_close(db);
    }

    #[test]
    fn test_descend_equal() {
        let mut db = test_open();

        db.update(|tx| {
            for i in 0..300 {
                tx.set(format!("key:{:05}A", i), format!("{}", i + 1000), None)
                    .unwrap();
                tx.set(format!("key:{:05}B", i), format!("{}", i + 1000), None)
                    .unwrap();
            }
            tx.create_index(
                "num".to_string(),
                "*".to_string(),
                vec![Arc::new(index_int)],
            )
        })
        .unwrap();

        let mut res = vec![];
        let res_mut = &mut res;
        db.view(|tx| {
            tx.descend_equal("", "key:00055A", |k, _| {
                res_mut.push(k.to_string());
                true
            })
        })
        .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res, svec!["key:00055A"]);

        res = vec![];
        let res_mut = &mut res;

        db.view(|tx| {
            tx.descend_equal("num", "1125", |k, _| {
                eprintln!("descend equal");
                res_mut.push(k.to_string());
                true
            })
        })
        .unwrap();

        assert_eq!(res.len(), 2);
        assert_eq!(res, svec!["key:00125B", "key:00125A"]);

        test_close(db);
    }

    #[test]
    fn test_various_tx() {
        let mut db = test_open();

        db.update(|tx| {
            tx.set("hello".to_string(), "planet".to_string(), None)
                .unwrap();
            Ok(())
        })
        .unwrap();

        let err_broken = DbError::Custom("broken".to_string());
        let e = db
            .update::<_, ()>(|tx| {
                tx.set("hello".to_string(), "world".to_string(), None)
                    .unwrap();
                Err(err_broken)
            })
            .unwrap_err();
        let err = DbError::Custom("broken".to_string());
        assert!(matches!(e, err));

        let val = db.view(|tx| tx.get("hello".to_string(), true)).unwrap();
        assert_eq!(val, "planet");

        db.update(|tx| {
            let saved_db = tx.db_lock.take().unwrap();
            let e = tx
                .set("hello".to_string(), "planet".to_string(), None)
                .unwrap_err();
            assert!(matches!(e, DbError::TxClosed));
            let e = tx.delete("hello".to_string()).unwrap_err();
            assert!(matches!(e, DbError::TxClosed));
            let e = tx.get("hello".to_string(), true).unwrap_err();
            assert!(matches!(e, DbError::TxClosed));

            tx.db_lock = Some(saved_db);
            tx.writable = false;
            let e = tx
                .set("hello".to_string(), "planet".to_string(), None)
                .unwrap_err();
            assert!(matches!(e, DbError::TxNotWritable));
            let e = tx.delete("hello".to_string()).unwrap_err();
            assert!(matches!(e, DbError::TxNotWritable));
            tx.writable = true;

            let e = tx.get("something".to_string(), true).unwrap_err();
            assert!(matches!(e, DbError::NotFound));
            let e = tx.delete("something".to_string()).unwrap_err();
            assert!(matches!(e, DbError::NotFound));

            tx.set(
                "var".to_string(),
                "val".to_string(),
                Some(SetOptions {
                    expires: true,
                    ttl: time::Duration::from_secs(0),
                }),
            )
            .unwrap();
            let e = tx.get("something".to_string(), true).unwrap_err();
            assert!(matches!(e, DbError::NotFound));
            let e = tx.delete("something".to_string()).unwrap_err();
            assert!(matches!(e, DbError::NotFound));

            Ok(())
        })
        .unwrap();

        // test non-managed transactions
        let mut tx = db.begin(true).unwrap();
        tx.set("howdy".to_string(), "world".to_string(), None)
            .unwrap();
        tx.commit().unwrap();
        drop(tx);

        let mut tx1 = db.begin(false).unwrap();
        let v = tx1.get("howdy".to_string(), false).unwrap();
        assert_eq!(v, "world");
        tx1.rollback().unwrap();
        drop(tx1);

        let mut tx2 = db.begin(true).unwrap();
        let v = tx2.get("howdy".to_string(), false).unwrap();
        assert_eq!(v, "world");
        tx2.delete("howdy".to_string()).unwrap();
        tx2.commit().unwrap();
        drop(tx2);

        // test fo closed transactions
        let err = db
            .update(|tx| {
                tx.db_lock = None;
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(err, DbError::TxClosed));

        // test for invalid writes

        test_close(db);
    }

    #[test]
    #[should_panic]
    fn test_panic_during_commit_in_managed_tx() {
        let mut db = Db::open(":memory:").unwrap();
        db.update(|tx| {
            tx.commit()?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    #[should_panic]
    fn test_panic_during_rollback_in_managed_tx() {
        let mut db = Db::open(":memory:").unwrap();
        db.update(|tx| {
            tx.rollback()?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_create_index_strings() {
        let mut db = Db::open(":memory:").unwrap();
        let mut collected = vec![];
        db.create_index(
            "name".to_string(),
            "*".to_string(),
            vec![Arc::new(index_string)],
        )
        .unwrap();
        db.update(|tx| {
            tx.set("1".to_string(), "Tom".to_string(), None).unwrap();
            tx.set("2".to_string(), "Janet".to_string(), None).unwrap();
            tx.set("3".to_string(), "Carol".to_string(), None).unwrap();
            tx.set("4".to_string(), "Alan".to_string(), None).unwrap();
            tx.set("5".to_string(), "Sam".to_string(), None).unwrap();
            tx.set("6".to_string(), "Melinda".to_string(), None)
                .unwrap();
            Ok(())
        })
        .unwrap();
        db.view(|tx| {
            tx.ascend("name".to_string(), |k, v| {
                collected.push(format!("{}: {}", k, v));
                true
            })
            .unwrap();
            Ok(())
        })
        .unwrap();
        assert_eq!(
            collected,
            svec![
                "4: Alan",
                "3: Carol",
                "2: Janet",
                "6: Melinda",
                "5: Sam",
                "1: Tom"
            ]
        );
    }

    #[test]
    fn test_create_index_ints() {
        let mut db = Db::open(":memory:").unwrap();
        let mut collected = vec![];
        db.create_index(
            "age".to_string(),
            "*".to_string(),
            vec![Arc::new(index_int)],
        )
        .unwrap();
        db.update(|tx| {
            tx.set("1".to_string(), "30".to_string(), None).unwrap();
            tx.set("2".to_string(), "51".to_string(), None).unwrap();
            tx.set("3".to_string(), "16".to_string(), None).unwrap();
            tx.set("4".to_string(), "76".to_string(), None).unwrap();
            tx.set("5".to_string(), "23".to_string(), None).unwrap();
            tx.set("6".to_string(), "43".to_string(), None).unwrap();
            Ok(())
        })
        .unwrap();
        db.view(|tx| {
            tx.ascend("age".to_string(), |k, v| {
                collected.push(format!("{}: {}", k, v));
                true
            })
            .unwrap();
            Ok(())
        })
        .unwrap();
        assert_eq!(
            collected,
            svec!["3: 16", "5: 23", "1: 30", "6: 43", "2: 51", "4: 76"]
        );
    }

    #[test]
    fn test_inserts_and_deleted() {
        let mut db = test_open();

        db.create_index(
            "any".to_string(),
            "*".to_string(),
            vec![Arc::new(index_string)],
        )
        .unwrap();
        // TODO:
        // db.create_spatial_index("rect".to_string(), "*".to_string(), vec![Arc::new(index_string)]).unwrap();

        db.update(|tx| {
            tx.set(
                "item1".to_string(),
                "value1".to_string(),
                Some(SetOptions {
                    expires: true,
                    ttl: time::Duration::from_secs(1),
                }),
            )?;
            tx.set("item2".to_string(), "value2".to_string(), None)?;
            tx.set(
                "item3".to_string(),
                "value3".to_string(),
                Some(SetOptions {
                    expires: true,
                    ttl: time::Duration::from_secs(1),
                }),
            )?;
            Ok(())
        })
        .unwrap();

        // test replacing items in the database
        db.update(|tx| {
            tx.set("item1".to_string(), "nvalue1".to_string(), None)?;
            tx.set("item2".to_string(), "nvalue2".to_string(), None)?;
            tx.delete("item3".to_string())?;
            Ok(())
        })
        .unwrap();

        test_close(db);
    }

    #[test]
    fn test_insert_does_not_misuse_index() {
        let mut db = test_open();

        // Only one item is eligible for the index, so no comparison is necessary.
        fn fail(_a: &str, _b: &str) -> bool {
            unreachable!()
        }

        db.create_index("some".to_string(), "a*".to_string(), vec![Arc::new(fail)])
            .unwrap();
        db.update(|tx| {
            tx.set("a".to_string(), "1".to_string(), None)?;
            tx.set("b".to_string(), "1".to_string(), None)?;
            Ok(())
        })
        .unwrap();

        db.update(|tx| {
            tx.set("b".to_string(), "2".to_string(), None)?;
            Ok(())
        })
        .unwrap();

        test_close(db);
    }

    #[test]
    fn test_delete_does_not_misuse_index() {
        let mut db = test_open();

        // Only one item is eligible for the index, so no comparison is necessary.
        fn fail(_a: &str, _b: &str) -> bool {
            unreachable!()
        }

        db.create_index("some".to_string(), "a*".to_string(), vec![Arc::new(fail)])
            .unwrap();
        db.update(|tx| {
            tx.set("a".to_string(), "1".to_string(), None)?;
            tx.set("b".to_string(), "1".to_string(), None)?;
            Ok(())
        })
        .unwrap();

        db.update(|tx| {
            tx.delete("b".to_string())?;
            Ok(())
        })
        .unwrap();

        test_close(db);
    }

    #[test]
    fn test_index_compare() {
        assert!(index_float("1.5", "1.6"));
        assert!(index_int("-1", "2"));
        assert!(index_uint("10", "25"));
        assert!(index_string_case_sensitive("Hello", "hello"));
        assert!(!index_string("hello", "hello"));
        assert!(!index_string("Hello", "hello"));
        assert!(!index_string("hello", "Hello"));
        assert!(index_string("gello", "Hello"));
        assert!(!index_string("Hello", "gello"));
        // TODO:
        // rect, point
    }

    #[test]
    fn test_opening_a_folder() {
        let _ = std::fs::remove_dir_all("dir.tmp");
        std::fs::create_dir("dir.tmp").unwrap();

        assert!(Db::open("dir.tmp").is_err());

        std::fs::remove_dir_all("dir.tmp").unwrap();
    }

    #[test]
    fn test_opening_a_closed_database() {
        let _ = std::fs::remove_file("data.db");

        let mut db = Db::open("data.db").unwrap();
        db.close().unwrap();
        let err = db.close().unwrap_err();
        assert!(matches!(err, DbError::DatabaseClosed));

        let mut db = Db::open(":memory:").unwrap();
        db.close().unwrap();
        let err = db.close().unwrap_err();
        assert!(matches!(err, DbError::DatabaseClosed));

        let _ = std::fs::remove_file("data.db");
    }

    #[test]
    fn test_config() {
        let mut db = test_open();

        db.set_config(Config {
            sync_policy: SyncPolicy::Never,
            ..Default::default()
        })
        .unwrap();

        db.set_config(Config {
            sync_policy: SyncPolicy::EverySecond,
            ..Default::default()
        })
        .unwrap();

        db.set_config(Config {
            auto_shrink_min_size: 100,
            auto_shrink_percentage: 200,
            sync_policy: SyncPolicy::Always,
            ..Default::default()
        })
        .unwrap();

        let config = db.read_config().unwrap();
        assert_eq!(config.auto_shrink_min_size, 100);
        assert_eq!(config.auto_shrink_percentage, 200);
        assert_eq!(config.sync_policy, SyncPolicy::Always);

        test_close(db);
    }
}
