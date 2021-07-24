//! Package buntdb implements a low-level in-memory key/value store in pure Go.
//! It persists to disk, is ACID compliant, and uses locking for multiple
//! readers and a single writer. Bunt is ideal for projects that need a
//! dependable database, and favor speed over data size.

#![allow(unused)]

use btreec::BTreeC;
use once_cell::sync::OnceCell;
use parking_lot::lock_api::RawRwLock as _;
use parking_lot::RawRwLock;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::sync::RwLock;
use std::time;

type RectFn = Arc<dyn Fn(String) -> (Vec<f64>, Vec<f64>)>;
type LessFn = Arc<dyn Fn(&str, &str) -> bool>;

#[derive(Clone, Debug, PartialEq)]
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

    // FIXME: there should be more general error handling than relying on internal
    // type for user errors
    Custom(String),
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
            Custom(s) => write!(f, "{}", s),
        }
    }
}

impl Error for DbError {}

/// Db represents a collection of key-value pairs that persist on disk.
/// Transactions are used for all forms of data access to the Db.
pub struct Db {
    /// the gatekeeper for all fields
    mu: RawRwLock,

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

    /// a reuse buffer for gathering indexes
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

/// SyncPolicy represents how often data is synced to disk.
#[derive(Clone, Eq, PartialEq)]
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

pub type OnExpiredFn = &'static dyn for<'e> Fn(Vec<String>);
pub type OnExpiredSyncFn = &'static dyn for<'e> Fn(String, String, &mut Tx) -> Result<(), DbError>;

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
    auto_shrink_percentage: i64,

    // `auto_shrink_min_size` defines the minimum size of the aof file before
    // an automatic shrink can occur.
    auto_shrink_min_size: i64,

    // auto_shrink_disabled turns off automatic background shrinking
    auto_shrink_disabled: bool,

    // `on_expired` is used to custom handle the deletion option when a key
    // has been expired.
    on_expired: Option<OnExpiredFn>,

    // `on_expired_sync` will be called inside the same transaction that is
    // performing the deletion of expired items. If OnExpired is present then
    // this callback will not be called. If this callback is present, then the
    // deletion of the timeed-out item is the explicit responsibility of this
    // callback.
    on_expired_sync: Option<OnExpiredSyncFn>,
}

// `ExCtx` is a simple b-tree context for ordering by expiration.
struct ExCtx {}

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

        let mut db = Db {
            mu: RawRwLock::INIT,
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

        if db.persist {
            // hardcoding 0666 as the default mode.
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path)?;
            db.file = Some(file);

            // load the database from disk
            // TODO:
            // if let Err(err) = db.load_from_disk() {
            //     // close on error, ignore close error
            //     db.file.take();
            //     return Err(err);
            // }
        }

        // TODO:
        // start the background manager

        Ok(db)
    }

    /// `close` releases all database resources.
    /// All transactions must be closed before closing the database.
    pub fn close(mut self) -> Result<(), DbError> {
        self.mu.lock_exclusive();

        if self.closed {
            unsafe { self.mu.unlock_exclusive() };
            return Err(DbError::DatabaseClosed);
        }

        self.closed = true;
        if self.persist {
            let file = self.file.take().unwrap();
            // do a sync but ignore the error
            let _ = file.sync_all();
            drop(file);
        }

        unsafe { self.mu.unlock_exclusive() };
        Ok(())
    }

    /// `save` writes a snapshot of the database to a writer. This operation blocks all
    /// writes, but not reads. This can be used for snapshots and backups for pure
    /// in-memory databases using the ":memory:". Database that persist to disk
    /// can be snapshotted by simply copying the database file.
    pub fn save(&mut self, writer: &mut dyn io::Write) -> Result<(), io::Error> {
        self.mu.lock_shared();
        let mut err = None;
        // use a buffered writer and flush every 4MB
        let mut buf = Vec::with_capacity(4 * 1024 * 1024);
        // iterate through every item in the database and write to the buffer
        self.keys.ascend(None, |item| {
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
            unsafe { self.mu.unlock_shared() };
            return Err(e);
        }

        // one final flush
        if !buf.is_empty() {
            writer.write_all(&buf)?;
        }

        unsafe { self.mu.unlock_shared() };
        Ok(())
    }

    /// `read_load` reads from the reader and loads commands into the database.
    /// modTime is the modified time of the reader, should be no greater than
    /// the current time.Now().
    /// Returns the number of bytes of the last command read and the error if any.
    pub fn read_load(
        &self,
        reader: &dyn io::Read,
        mod_time: time::SystemTime,
    ) -> (u64, Option<io::Error>) {
        // let mut total_size = 0;
        // let mut data = Vec::with_capacity(4096);
        // let mut parts = vec![];

        // loop {
        // peek at the first byte. If it's a 'nul' control character then
        // ignore it and move to the next byte.

        // }

        // (0, None)
        todo!()
    }

    /// `load_from_disk` reads entries from the append only database file and fills the database.
    /// The file format uses the Redis append only file format, which is and a series
    /// of RESP commands. For more information on RESP please read
    /// http://redis.io/topics/protocol. The only supported RESP commands are DEL and
    /// SET.
    fn load_from_disk(&mut self) -> Result<(), io::Error> {
        let mut file = &(*self.file.as_ref().unwrap());
        let metadata = file.metadata()?;
        let mod_time = metadata.modified()?;

        let (n, maybe_err) = self.read_load(file, mod_time);

        if let Some(err) = maybe_err {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                // The db file has ended mid-command, which is allowed but the
                // data file should be truncated to the end of the last valid
                // command
                file.set_len(n)?;
            } else {
                return Err(err);
            }
        }

        use std::io::Seek;
        let pos = file.seek(io::SeekFrom::Start(n))?;
        self.lastaofsz = pos;
        Ok(())
    }

    /// `load` loads commands from reader. This operation blocks all reads and writes.
    /// Note that this can only work for fully in-memory databases opened with
    /// Open(":memory:").
    pub fn load(&mut self, reader: &dyn io::Read) -> Result<(), io::Error> {
        self.mu.lock_exclusive();

        if self.persist {
            let err = io::Error::new(io::ErrorKind::Other, DbError::PersistenceActive);
            return Err(err);
        }

        let (_, maybe_err) = self.read_load(reader, time::SystemTime::now());

        unsafe { self.mu.unlock_exclusive() };

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
        less: Vec<LessFn>,
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
        less: Vec<LessFn>,
    ) -> Result<(), DbError> {
        self.update(move |tx| {
            if let Err(err) = tx.create_index(name.clone(), pattern.clone(), less.clone()) {
                if err == DbError::IndexExists {
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
        rect: RectFn,
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
        rect: RectFn,
    ) -> Result<(), DbError> {
        self.update(move |tx| {
            if let Err(err) = tx.create_spatial_index(name.clone(), pattern.clone(), rect.clone()) {
                if err == DbError::IndexExists {
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
        self.mu.lock_shared();
        if self.closed {
            unsafe { self.mu.unlock_shared() };
            return Err(DbError::DatabaseClosed);
        }
        let c = self.config.clone();
        unsafe { self.mu.unlock_shared() };
        Ok(c)
    }

    /// SetConfig updates the database configuration.
    pub fn set_config(&mut self, config: Config) -> Result<(), DbError> {
        self.mu.lock_exclusive();
        if self.closed {
            unsafe { self.mu.unlock_exclusive() };
            return Err(DbError::DatabaseClosed);
        }
        self.config = config;
        unsafe { self.mu.unlock_exclusive() };
        Ok(())
    }

    /// insertIntoDatabase performs inserts an item in to the database and updates
    /// all indexes. If a previous item with the same key already exists, that item
    /// will be replaced with the new one, and return the previous item.
    pub fn insert_into_database(&mut self, item: DbItem) -> Option<DbItem> {
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

    /// backgroundManager runs continuously in the background and performs various
    /// operations such as removing expired items and syncing to disk.
    fn background_manager() {
        todo!()
    }

    /// Shrink will make the database file smaller by removing redundant
    /// log entries. This operation does not block the database.
    fn shrink() {
        todo!()
    }

    /// managed calls a block of code that is fully contained in a transaction.
    /// This method is intended to be wrapped by Update and View
    fn managed<F, R>(&mut self, writable: bool, func: F) -> Result<R, DbError>
    where
        F: FnOnce(&mut Tx) -> Result<R, DbError>,
    {
        let mut tx = self.begin(writable)?;
        tx.funcd = true;
        let func_result = func(&mut tx);
        tx.funcd = false;
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

    /// get return an item or nil if not found.
    pub fn get(&self, key: String) -> Option<&DbItem> {
        self.keys.get(DbItem {
            key,
            ..Default::default()
        })
    }

    // Begin opens a new transaction.
    // Multiple read-only transactions can be opened at the same time but there can
    // only be one read/write transaction at a time. Attempting to open a read/write
    // transactions while another one is in progress will result in blocking until
    // the current read/write transaction is completed.
    //
    // All transactions must be closed by calling Commit() or Rollback() when done.
    fn begin(&mut self, writable: bool) -> Result<Tx, DbError> {
        let mut tx = Tx {
            db: Some(self),
            has_lock: false,
            writable,
            funcd: false,
            wc: None,
        };

        tx.lock();

        if tx.db.as_ref().unwrap().closed {
            tx.unlock();
            return Err(DbError::DatabaseClosed);
        }

        if writable {
            tx.wc = Some(TxWriteContext::default());
        }

        Ok(tx)
    }
}

/// `IndexOptions` provides an index with additional features or
/// alternate functionality.
#[derive(Clone, Default)]
struct IndexOptions {
    /// `case_insensitive_key_matching` allow for case-insensitive
    /// matching on keys when setting key/values.
    case_insensitive_key_matching: bool,
}

/// `Index` represents a b-tree or r-tree index and also acts as the
/// b-tree/r-tree context for itself.
struct Index {
    // contains the items
    btr: Option<BTreeC<DbItem>>,

    /// contains the items
    // rtr     *rtred.RTree

    /// name of the index
    name: String,
    /// a required key pattern
    pattern: String,

    /// less comparison function
    less: Option<LessFn>,

    /// rect from string function
    rect: Option<RectFn>,

    /// the origin database
    // db: Arc<Db>,

    /// index options
    opts: IndexOptions,
}

impl Index {
    pub fn matches(&self, key: &str) -> bool {
        let mut key = key.to_string();
        if self.pattern == "*" {
            return true;
        }

        if self.opts.case_insensitive_key_matching {
            let mut chars_iter = key.chars();
            for char_ in chars_iter {
                if ('A'..='Z').contains(&char_) {
                    key = key.to_lowercase();
                    break;
                }
            }
        }

        // TODO: need to port https://github.com/tidwall/match package
        let r = self.pattern.matches(&key).peekable().peek().is_some();
        r
    }

    // `clear_copy` creates a copy of the index, but with an empty dataset.
    pub fn clear_copy(&self) -> Index {
        // copy the index meta information
        let mut nidx = Index {
            btr: None,
            name: self.name.clone(),
            pattern: self.pattern.clone(),
            // db: self.db.clone(),
            less: self.less.clone(),
            rect: self.rect.clone(),
            opts: self.opts.clone(),
        };

        // initialize with empty trees
        if nidx.less.is_some() {
            // TODO: duplicated in `rebuild`
            let less_fn = nidx.less.clone().unwrap();
            let compare_fn = Box::new(move |a: &DbItem, b: &DbItem| {
                // TODO: remove these clones
                if less_fn(&a.val, &b.val) {
                    return Ordering::Less;
                }
                if less_fn(&b.val, &a.val) {
                    return Ordering::Greater;
                }

                if a.keyless {
                    return Ordering::Greater;
                } else if b.keyless {
                    return Ordering::Less;
                }
                a.key.cmp(&b.key)
            });
            let btree = BTreeC::new(compare_fn);
            nidx.btr = Some(btree);
        }
        if nidx.rect.is_some() {
            // TODO:
            // nidx.rtr = rtred.New(nidx)
        }

        nidx
    }

    // `rebuild` rebuilds the index
    pub fn rebuild(&mut self, db: &Db) {
        // initialize trees
        if let Some(less_fn) = self.less.clone() {
            // TODO: less_ctx(self)
            self.btr = Some(BTreeC::new(Box::new(move |a: &DbItem, b: &DbItem| {
                eprintln!("index compare fn a: {} b: {}", a.val, b.val);
                // using an index less_fn
                if less_fn(&a.val, &b.val) {
                    return Ordering::Less;
                }
                eprintln!("second index compare fn a: {} b: {}", a.val, b.val);
                if less_fn(&b.val, &a.val) {
                    return Ordering::Greater;
                }

                // Always fall back to the key comparison. This creates absolute uniqueness.
                if a.keyless {
                    return Ordering::Greater;
                } else if b.keyless {
                    return Ordering::Less;
                }
                a.key.cmp(&b.key)
            })));
        }
        if self.rect.is_some() {
            // TODO:
            // self.rtr =
        }
        // iterate through all keys and fill the index
        db.keys.ascend(None, |item| {
            if !self.matches(&item.key) {
                // does not match the pattern continue
                return true;
            }
            if self.less.is_some() {
                // FIXME: this should probably be an Arc or Rc
                // instead of a copy
                self.btr.as_mut().unwrap().set(item.to_owned());
            }
            if self.rect.is_some() {
                // TODO:
                // self.rtr
            }

            true
        });
    }
}

/// DbItemOpts holds various meta information about an item.
#[derive(Clone, Eq, PartialEq)]
pub struct DbItemOpts {
    /// does this item expire?
    ex: bool,
    /// when does this item expire?
    exat: time::Instant,
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct DbItem {
    // the binary key
    key: String,
    // the binary value
    val: String,
    // optional meta information
    opts: Option<DbItemOpts>,
    // keyless item for scanning
    keyless: bool,
}

// This is a long time in the future. It's an imaginary number that is
// used for b-tree ordering.
static MAX_TIME: OnceCell<time::Instant> = OnceCell::new();

fn get_max_time() -> time::Instant {
    *MAX_TIME.get_or_init(|| time::Instant::now() + time::Duration::MAX)
}

impl DbItem {
    // expired evaluates id the item has expired. This will always return false when
    // the item does not have `opts.ex` set to true.
    fn expired(&self) -> bool {
        if let Some(opts) = &self.opts {
            return opts.ex && opts.exat < time::Instant::now();
        }

        false
    }

    // expiresAt will return the time when the item will expire. When an item does
    // not expire `maxTime` is used.
    fn expires_at(&self) -> time::Instant {
        if let Some(opts) = &self.opts {
            if !opts.ex {
                return get_max_time();
            }

            return opts.exat;
        }

        get_max_time()
    }

    // writeSetTo writes an item as a single SET record to the a bufio Writer.
    fn write_set_to(&self, buf: &mut Vec<u8>) {
        if let Some(opts) = &self.opts {
            if opts.ex {
                let now = time::Instant::now();
                let ex = opts.exat.saturating_duration_since(now).as_secs();
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
    fn write_delete_to(&self, buf: &mut Vec<u8>) {
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

// Tx represents a transaction on the database. This transaction can either be
// read-only or read/write. Read-only transactions can be used for retrieving
// values for keys and iterating through keys and values. Read/write
// transactions can set and delete keys.
//
// All transactions must be committed or rolled-back when done.
// TODO: this lifetime makes no sense - we clear db option when transaction
// is no longer valid - it should be an arc<mutex<>> or something like this
pub struct Tx<'db> {
    /// the underlying database.
    db: Option<&'db mut Db>,
    /// are we currently holding DB lock?
    has_lock: bool,
    /// when false mutable operations fail.
    writable: bool,
    /// when true Commit and Rollback panic.
    funcd: bool,
    /// context for writable transactions.
    wc: Option<TxWriteContext>,
}

#[derive(Default)]
pub struct TxWriteContext {
    // rollback when deleteAll is called

    // a tree of all item ordered by key
    rbkeys: Option<BTreeC<DbItem>>,
    // a tree of items ordered by expiration
    rbexps: Option<BTreeC<DbItem>>,
    // the index trees.
    rbidxs: Option<HashMap<String, Index>>,

    /// details for rolling back tx.
    rollback_items: HashMap<String, Option<DbItem>>,
    // details for committing tx.
    commit_items: HashMap<String, Option<DbItem>>,
    // stack of iterators
    itercount: i64,
    // details for dropped indexes.
    rollback_indexes: HashMap<String, Option<Index>>,
}

impl<'db> Tx<'db> {
    // DeleteAll deletes all items from the database.
    fn delete_all(&mut self) -> Result<(), DbError> {
        if self.db.is_none() {
            return Err(DbError::TxClosed);
        } else if !self.writable {
            return Err(DbError::TxNotWritable);
        } else if self.wc.as_ref().unwrap().itercount > 0 {
            return Err(DbError::TxIterating);
        }

        let db = self.db.as_mut().unwrap();
        let wc = self.wc.as_mut().unwrap();

        // now reset the live database trees
        let old_keys = std::mem::replace(&mut db.keys, BTreeC::new(Box::new(keys_compare_fn)));
        let old_exps = std::mem::replace(&mut db.exps, BTreeC::new(Box::new(exps_compare_fn)));
        let old_idxs = std::mem::take(&mut db.idxs);

        // check to see if we've already deleted everything
        if wc.rbkeys.is_none() {
            // we need to backup the live data in case of a rollback
            wc.rbkeys = Some(old_keys);
            wc.rbexps = Some(old_exps);
            wc.rbidxs = Some(old_idxs);
        }

        // finally re-create the indexes
        for (name, idx) in wc.rbidxs.as_ref().unwrap().iter() {
            db.idxs.insert(name.to_string(), idx.clear_copy());
        }

        // always clear out the commits
        wc.commit_items = HashMap::new();

        Ok(())
    }

    fn indexes(&self) -> Result<Vec<String>, DbError> {
        if self.db.is_none() {
            return Err(DbError::TxClosed);
        }

        let db = self.db.as_ref().unwrap();
        let mut names = db
            .idxs
            .keys()
            .map(|k| k.to_string())
            .collect::<Vec<String>>();
        names.sort();

        Ok(names)
    }

    // createIndex is called by CreateIndex() and CreateSpatialIndex()
    fn create_index_inner(
        &mut self,
        name: String,
        pattern: String,
        lessers: Vec<LessFn>,
        rect: Option<RectFn>,
        opts: Option<IndexOptions>,
    ) -> Result<(), DbError> {
        if self.db.is_none() {
            return Err(DbError::TxClosed);
        } else if !self.writable {
            return Err(DbError::TxNotWritable);
        } else if self.wc.as_ref().unwrap().itercount > 0 {
            return Err(DbError::TxIterating);
        }

        if name.is_empty() {
            // cannot create an index without a name.
            // an empty name index is designated for the main "keys" tree.
            return Err(DbError::IndexExists);
        }

        let db = self.db.as_mut().unwrap();
        let wc = self.wc.as_mut().unwrap();

        // check if an index with that name already exists
        if db.idxs.contains_key(&name) {
            // index with name already exists. error.
            return Err(DbError::IndexExists);
        }

        // generate a less function
        let less = match lessers.len() {
            // no less function
            0 => None,
            1 => Some(lessers[0].clone()),
            _ => {
                // FIXME: probably need to make it into a trait object
                // let func = Arc::new(|a, b| {
                //     for i in 0..lessers.len() {
                //         if lessers[i](a, b) {
                //             return true;
                //         }
                //         if lessers[i](b, a) {
                //             return false;
                //         }
                //     }
                //     lessers[lessers.len() - 1](a, b)
                // });
                // Some(func)
                None
            }
        };

        eprintln!("creating index");
        let mut pattern = pattern;
        let options = opts.unwrap_or_default();
        if options.case_insensitive_key_matching {
            pattern = pattern.to_lowercase();
        }

        let mut idx = Index {
            btr: None,
            name,
            pattern,
            less,
            rect,
            opts: options,
        };
        idx.rebuild(db);
        // store the index in the rollback map.
        if wc.rbkeys.is_none() {
            // store the index in the rollback map
            if !wc.rollback_indexes.contains_key(&idx.name) {
                // we use None to indicate that the index should be removed upon
                // rollback.
                wc.rollback_indexes.insert(idx.name.clone(), None);
            }
        }
        // save the index
        db.idxs.insert(idx.name.clone(), idx);

        Ok(())
    }

    // CreateIndex builds a new index and populates it with items.
    // The items are ordered in an b-tree and can be retrieved using the
    // Ascend* and Descend* methods.
    // An error will occur if an index with the same name already exists.
    //
    // When a pattern is provided, the index will be populated with
    // keys that match the specified pattern. This is a very simple pattern
    // match where '*' matches on any number characters and '?' matches on
    // any one character.
    // The less function compares if string 'a' is less than string 'b'.
    // It allows for indexes to create custom ordering. It's possible
    // that the strings may be textual or binary. It's up to the provided
    // less function to handle the content format and comparison.
    // There are some default less function that can be used such as
    // IndexString, IndexBinary, etc.
    fn create_index(
        &mut self,
        name: String,
        pattern: String,
        less: Vec<LessFn>,
    ) -> Result<(), DbError> {
        self.create_index_inner(name, pattern, less, None, None)
    }

    // CreateIndexOptions is the same as CreateIndex except that it allows
    // for additional options.
    fn create_index_options(
        &mut self,
        name: String,
        pattern: String,
        opts: IndexOptions,
        less: Vec<LessFn>,
    ) -> Result<(), DbError> {
        self.create_index_inner(name, pattern, less, None, Some(opts))
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
    fn create_spatial_index(
        &mut self,
        name: String,
        pattern: String,
        rect: RectFn,
    ) -> Result<(), DbError> {
        self.create_index_inner(name, pattern, vec![], Some(rect), None)
    }

    // CreateSpatialIndexOptions is the same as CreateSpatialIndex except that
    // it allows for additional options.
    fn create_spatial_index_options(
        &mut self,
        name: String,
        pattern: String,
        rect: RectFn,
        opts: IndexOptions,
    ) -> Result<(), DbError> {
        self.create_index_inner(name, pattern, vec![], Some(rect), Some(opts))
    }

    fn drop_index(&mut self, name: String) -> Result<(), DbError> {
        if self.db.is_none() {
            return Err(DbError::TxClosed);
        } else if !self.writable {
            return Err(DbError::TxNotWritable);
        } else if self.wc.as_ref().unwrap().itercount > 0 {
            return Err(DbError::TxIterating);
        }

        if name.is_empty() {
            // cannot drop the default "keys" index
            return Err(DbError::InvalidOperation);
        }

        let wc = self.wc.as_mut().unwrap();
        let db = self.db.as_mut().unwrap();
        if !db.idxs.contains_key(&name) {
            return Err(DbError::NotFound);
        }

        // delete from the map.
        // this is all that is needed to delete an index.
        let idx = db.idxs.remove(&name).unwrap();
        if wc.rbkeys.is_none() {
            // store the index in the rollback map.
            // we use a non-nil copy of the index without the data to indicate
            // that the index should be rebuilt upon rollback.
            wc.rollback_indexes
                .entry(name)
                .or_insert_with(|| Some(idx.clear_copy()));
        }

        Ok(())
    }

    // lock locks the database based on the transaction type.
    fn lock(&mut self) {
        let db = self.db.as_ref().unwrap();
        if self.writable {
            db.mu.lock_exclusive();
        } else {
            db.mu.lock_shared();
        }
        self.has_lock = true;
    }

    // unlock unlocks the database based on the transaction type.
    fn unlock(&mut self) {
        let db = self.db.as_ref().unwrap();
        if self.writable {
            unsafe { db.mu.unlock_exclusive() };
        } else {
            unsafe { db.mu.unlock_shared() };
        }
        self.has_lock = false;
    }

    // rollbackInner handles the underlying rollback logic.
    // Intended to be called from Commit() and Rollback().
    fn rollback_inner(&mut self) {
        // rollback the deleteAll if needed
        let wc = self.wc.as_mut().unwrap();
        let db = self.db.as_mut().unwrap();

        if wc.rbkeys.is_some() {
            db.keys = wc.rbkeys.take().unwrap();
            db.idxs = wc.rbidxs.take().unwrap();
            db.exps = wc.rbexps.take().unwrap();
        }

        for (key, maybe_item) in wc.rollback_items.drain() {
            // TODO: make a helper on DbItem
            db.delete_from_database(DbItem {
                key: key.to_string(),
                ..Default::default()
            });
            if let Some(item) = maybe_item {
                // when an item is not None, we will need to reinsert that item
                // into the database overwriting the current one.
                db.insert_into_database(item);
            }
        }
        for (name, maybe_idx) in wc.rollback_indexes.drain() {
            db.idxs.remove(&name);
            if let Some(mut idx) = maybe_idx {
                // When an index is not None, we will need to rebuild that index
                // this could an expensive process if the database has many
                // items or the index is complex.
                eprintln!("rebuilding index {}", name);
                idx.rebuild(db);
                db.idxs.insert(name, idx);
            }
        }
    }

    // Commit writes all changes to disk.
    // An error is returned when a write error occurs, or when a Commit() is called
    // from a read-only transaction.
    fn commit(&mut self) -> Result<(), DbError> {
        if self.funcd {
            panic!("managed tx rollback not allowed");
        }

        if self.db.is_none() {
            return Err(DbError::TxClosed);
        } else if !self.writable {
            return Err(DbError::TxNotWritable);
        }

        let mut result = Ok(());
        let mut db = self.db.as_mut().unwrap();
        let mut wc = self.wc.as_mut().unwrap();
        if db.persist && (!wc.commit_items.is_empty() || wc.rbkeys.is_some()) {
            db.buf.clear();
            // write a flushdb if a deleteAll was called
            if wc.rbkeys.is_some() {
                db.buf.extend("*1\r\n$7\r\nflushdb\r\n".as_bytes());
            }
            // Each commited record is written to disk
            for (key, maybe_item) in wc.commit_items.drain() {
                if let Some(item) = maybe_item {
                    item.write_set_to(&mut db.buf);
                } else {
                    let item = DbItem {
                        key,
                        ..Default::default()
                    };
                    item.write_delete_to(&mut db.buf);
                }
            }
            // Flushing the buffer only once per transaction.
            // If this operation fails then the write did failed and we must
            // rollback.
            let mut n = 0;

            // TODO: handle partial writes
            let db_file = db.file.as_mut().unwrap();
            let sync_policy = db.config.sync_policy.clone();

            if let Err(e) = db.file.as_mut().unwrap().write_all(&db.buf) {
                self.rollback_inner();
                // TODO: return error
            }

            if sync_policy == SyncPolicy::Always {
                let _ = self.db.as_mut().unwrap().file.as_mut().unwrap().sync_all();
            }

            // Increment the number of flushes. The background syncing uses this.
            self.db.as_mut().unwrap().flushes += 1;
        }
        // Unlock the database and allow for another writable transaction.
        self.unlock();
        // Clear the db field to disable this transaction from future use.
        self.db = None;
        result
    }

    // Rollback closes the transaction and reverts all mutable operations that
    // were performed on the transaction such as Set() and Delete().
    //
    // Read-only transactions can only be rolled back, not committed.
    fn rollback(&mut self) -> Result<(), DbError> {
        if self.funcd {
            panic!("managed tx rollback not allowed");
        }

        if self.db.is_none() {
            return Err(DbError::TxClosed);
        }
        // The rollback func does the heavy lifting.
        if self.writable {
            self.rollback_inner();
        }
        self.unlock();
        // Clear the db field to disable this transaction from future use.
        self.db = None;
        Ok(())
    }

    // GetLess returns the less function for an index. This is handy for
    // doing ad-hoc compares inside a transaction.
    // Returns ErrNotFound if the index is not found or there is no less
    // function bound to the index
    fn get_less(&self, index: String) -> Result<LessFn, DbError> {
        if self.db.is_none() {
            return Err(DbError::TxClosed);
        }

        let db = self.db.as_ref().unwrap();
        if let Some(idx) = db.idxs.get(&index) {
            if let Some(less_fn) = idx.less.clone() {
                return Ok(less_fn);
            }
        }

        Err(DbError::NotFound)
    }

    // GetRect returns the rect function for an index. This is handy for
    // doing ad-hoc searches inside a transaction.
    // Returns ErrNotFound if the index is not found or there is no rect
    // function bound to the index
    fn get_rect(&self, index: String) -> Result<(), DbError> {
        todo!()
    }

    // Set inserts or replaces an item in the database based on the key.
    // The opt params may be used for additional functionality such as forcing
    // the item to be evicted at a specified time. When the return value
    // for err is nil the operation succeeded. When the return value of
    // replaced is true, then the operaton replaced an existing item whose
    // value will be returned through the previousValue variable.
    // The results of this operation will not be available to other
    // transactions until the current transaction has successfully committed.
    //
    // Only a writable transaction can be used with this operation.
    // This operation is not allowed during iterations such as Ascend* & Descend*.
    fn set(
        &mut self,
        key: String,
        val: String,
        set_opts: Option<SetOptions>,
        // TODO: could probably return Option<String> instead
    ) -> Result<(Option<String>, bool), DbError> {
        if self.db.is_none() {
            return Err(DbError::TxClosed);
        } else if !self.writable {
            return Err(DbError::TxNotWritable);
        } else if self.wc.as_ref().unwrap().itercount > 0 {
            return Err(DbError::TxIterating);
        }

        let mut item = DbItem {
            key: key.to_string(),
            val,
            keyless: false,
            opts: None,
        };

        if let Some(opts) = set_opts {
            if opts.expires {
                // The caller is requesting that this item expires. Convert the
                // TTL to an absolute time and bind it to the item.
                item.opts = Some(DbItemOpts {
                    ex: true,
                    exat: time::Instant::now() + opts.ttl,
                });
            }
        }

        // Insert the item into the keys tree.
        let db = self.db.as_mut().unwrap();
        let wc = self.wc.as_mut().unwrap();
        let maybe_prev = db.insert_into_database(item.clone());

        let mut prev_value = None;
        let mut replaced = false;

        // insert into the rollback map if there has not been a deleteAll.
        if wc.rbkeys.is_none() {
            if let Some(prev) = maybe_prev {
                // A previous item already exists in the database. Let's create a
                // rollback entry with the item as the value. We need to check the
                // map to see if there isn't already an item that matches the
                // same key.
                if !wc.rollback_items.contains_key(&key) {
                    wc.rollback_items
                        .insert(key.to_string(), Some(prev.clone()));
                }
                if !prev.expired() {
                    prev_value = Some(prev.val);
                    replaced = true;
                }
            } else {
                // An item with the same key did not previously exist. Let's
                // create a rollback entry with a nil value. A nil value indicates
                // that the entry should be deleted on rollback. When the value is
                // *not* nil, that means the entry should be reverted.
                if !wc.rollback_items.contains_key(&key) {
                    wc.rollback_items.insert(key.to_string(), None);
                }
            }
        }
        // For commits we simply assign the item to the map. We use this map to
        // write the entry to disk.
        if db.persist {
            wc.commit_items.insert(key, Some(item));
        }

        Ok((prev_value, replaced))
    }

    // Get returns a value for a key. If the item does not exist or if the item
    // has expired then ErrNotFound is returned. If ignoreExpired is true, then
    // the found value will be returned even if it is expired.
    fn get(&mut self, key: String, ignore_expired: bool) -> Result<String, DbError> {
        if self.db.is_none() {
            return Err(DbError::TxClosed);
        }
        let maybe_item = self.db.as_ref().unwrap().get(key);

        match maybe_item {
            None => Err(DbError::NotFound),
            Some(item) => {
                if item.expired() && !ignore_expired {
                    // The item does not exists or has expired. Let's assume that
                    // the caller is only interested in items that have not expired.
                    return Err(DbError::NotFound);
                }
                Ok(item.val.to_string())
            }
        }
    }

    // Delete removes an item from the database based on the item's key. If the item
    // does not exist or if the item has expired then ErrNotFound is returned.
    //
    // Only a writable transaction can be used for this operation.
    // This operation is not allowed during iterations such as Ascend* & Descend*.
    fn delete(&mut self, key: String) -> Result<String, DbError> {
        if self.db.is_none() {
            return Err(DbError::TxClosed);
        } else if !self.writable {
            return Err(DbError::TxNotWritable);
        } else if self.wc.as_ref().unwrap().itercount > 0 {
            return Err(DbError::TxIterating);
        }

        let wc = self.wc.as_mut().unwrap();
        let db = self.db.as_mut().unwrap();
        let maybe_item = db.delete_from_database(DbItem {
            key: key.to_string(),
            ..Default::default()
        });
        if maybe_item.is_none() {
            return Err(DbError::NotFound);
        }
        let item = maybe_item.unwrap();
        // create a rollback entry if there has not been a deleteAll call
        if wc.rbkeys.is_none() && !wc.rollback_items.contains_key(&key) {
            wc.rollback_items
                .insert(key.to_string(), Some(item.clone()));
        }
        if db.persist {
            wc.commit_items.insert(key, None);
        }
        // Even though the item has been deleted. we still want to check
        // if it has expired. An expired item should not be returned.
        if item.expired() {
            // The item exists in the tree, but has expired. Let's assume that
            // the caller is only interested in items that have not expired.
            return Err(DbError::NotFound);
        }
        Ok(item.val)
    }

    // TTL returns the remaining time-to-live for an item.
    // A negative duration will be returned for items that do not have an
    // expiration.
    fn ttl(&mut self, key: String) -> Result<time::Duration, DbError> {
        todo!()
    }

    // scan iterates through a specified index and calls user-defined iterator
    // function for each item encountered.
    // The desc param indicates that the iterator should descend.
    // The gt param indicates that there is a greaterThan limit.
    // The lt param indicates that there is a lessThan limit.
    // The index param tells the scanner to use the specified index tree. An
    // empty string for the index means to scan the keys, not the values.
    // The start and stop params are the greaterThan, lessThan limits. For
    // descending order, these will be lessThan, greaterThan.
    // An error will be returned if the tx is closed or the index is not found.
    #[allow(clippy::too_many_arguments)]
    fn scan<F>(
        &mut self,
        desc: bool,
        gt: bool,
        lt: bool,
        index: &str,
        start: &str,
        stop: &str,
        mut iterator: F,
    ) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        if self.db.is_none() {
            return Err(DbError::TxClosed);
        }

        let db = self.db.as_ref().unwrap();
        let mut tr;

        if index.is_empty() {
            // empty index means we will use the keys tree
            tr = &db.keys;
        } else {
            #[allow(clippy::collapsible_else_if)]
            if let Some(idx) = db.idxs.get(index) {
                if let Some(btr) = &idx.btr {
                    eprintln!("using index {}", btr.count());
                    tr = btr;
                } else {
                    return Ok(());
                }
            } else {
                return Err(DbError::NotFound);
            }
        }

        // create some limit items
        let mut item_a = None;
        let mut item_b = None;

        if gt || lt {
            #[allow(clippy::collapsible_else_if)]
            if index.is_empty() {
                item_a = Some(DbItem {
                    key: start.to_string(),
                    ..Default::default()
                });
                item_b = Some(DbItem {
                    key: stop.to_string(),
                    ..Default::default()
                });
            } else {
                eprintln!("item a {:#?} item b {:#?}", start, stop);
                item_a = Some(DbItem {
                    val: start.to_string(),
                    keyless: desc,
                    ..Default::default()
                });
                item_b = Some(DbItem {
                    val: stop.to_string(),
                    keyless: desc,
                    ..Default::default()
                });
            }
        }

        // execute the scan on the underlying tree.
        if let Some(wc) = self.wc.as_mut() {
            wc.itercount += 1;
        }

        eprintln!("desc {} gt {} lt {}", desc, gt, lt);

        #[allow(clippy::collapsible_else_if)]
        if desc {
            if gt && lt {
                btree_descend_range(
                    tr,
                    item_a.as_ref().unwrap(),
                    item_b.as_ref().unwrap(),
                    iterator,
                );
            } else if gt {
                btree_descend_greater_than(tr, item_a.as_ref().unwrap(), iterator);
            } else if lt {
                btree_descend_less_or_equal(tr, item_a, iterator);
            } else {
                btree_descend(tr, iterator);
            }
        } else {
            if gt && lt {
                btree_ascend_range(
                    tr,
                    item_a.as_ref().unwrap(),
                    item_b.as_ref().unwrap(),
                    iterator,
                );
            } else if gt {
                btree_ascend_greater_or_equal(tr, item_a, iterator);
            } else if lt {
                btree_ascend_less_than(tr, item_a.as_ref().unwrap(), iterator);
            } else {
                btree_ascend(tr, iterator);
            }
        }

        if let Some(wc) = self.wc.as_mut() {
            wc.itercount -= 1;
        }

        Ok(())
    }

    // AscendKeys allows for iterating through keys based on the specified pattern.
    fn ascend_keys<F>(&mut self, pattern: &str, iterator: F) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        todo!()
    }

    // DescendKeys allows for iterating through keys based on the specified pattern.
    fn descend_keys<F>(&mut self, pattern: String, iterator: F) -> Result<(), DbError>
    where
        F: Fn(String, String) -> bool,
    {
        todo!()
    }

    // Ascend calls the iterator for every item in the database within the range
    // [first, last], until iterator returns false.
    // When an index is provided, the results will be ordered by the item values
    // as specified by the less() function of the defined index.
    // When an index is not provided, the results will be ordered by the item key.
    // An invalid index will return an error.
    fn ascend<F>(&mut self, index: String, iterator: F) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        self.scan(false, false, false, &index, "", "", iterator)
    }

    // AscendGreaterOrEqual calls the iterator for every item in the database within
    // the range [pivot, last], until iterator returns false.
    // When an index is provided, the results will be ordered by the item values
    // as specified by the less() function of the defined index.
    // When an index is not provided, the results will be ordered by the item key.
    // An invalid index will return an error.
    fn ascend_greater_or_equal<F>(
        &mut self,
        index: &str,
        pivot: &str,
        iterator: F,
    ) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        self.scan(false, true, false, index, pivot, "", iterator)
    }

    // AscendLessThan calls the iterator for every item in the database within the
    // range [first, pivot), until iterator returns false.
    // When an index is provided, the results will be ordered by the item values
    // as specified by the less() function of the defined index.
    // When an index is not provided, the results will be ordered by the item key.
    // An invalid index will return an error.
    fn ascend_less_than<F>(&mut self, index: &str, pivot: &str, iterator: F) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        self.scan(false, false, true, index, pivot, "", iterator)
    }

    // AscendRange calls the iterator for every item in the database within
    // the range [greaterOrEqual, lessThan), until iterator returns false.
    // When an index is provided, the results will be ordered by the item values
    // as specified by the less() function of the defined index.
    // When an index is not provided, the results will be ordered by the item key.
    // An invalid index will return an error.
    fn ascend_range<F>(
        &mut self,
        index: &str,
        greater_or_equal: &str,
        less_than: &str,
        iterator: F,
    ) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        self.scan(
            false,
            true,
            true,
            index,
            greater_or_equal,
            less_than,
            iterator,
        )
    }

    // Descend calls the iterator for every item in the database within the range
    // [last, first], until iterator returns false.
    // When an index is provided, the results will be ordered by the item values
    // as specified by the less() function of the defined index.
    // When an index is not provided, the results will be ordered by the item key.
    // An invalid index will return an error.
    fn descend<F>(&mut self, index: &str, iterator: F) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        self.scan(true, false, false, index, "", "", iterator)
    }

    // DescendGreaterThan calls the iterator for every item in the database within
    // the range [last, pivot), until iterator returns false.
    // When an index is provided, the results will be ordered by the item values
    // as specified by the less() function of the defined index.
    // When an index is not provided, the results will be ordered by the item key.
    // An invalid index will return an error.
    fn descend_greater_than<F>(
        &mut self,
        index: &str,
        pivot: &str,
        iterator: F,
    ) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        self.scan(true, true, false, index, pivot, "", iterator)
    }

    // DescendLessOrEqual calls the iterator for every item in the database within
    // the range [pivot, first], until iterator returns false.
    // When an index is provided, the results will be ordered by the item values
    // as specified by the less() function of the defined index.
    // When an index is not provided, the results will be ordered by the item key.
    // An invalid index will return an error.
    fn descend_less_or_equal<F>(
        &mut self,
        index: &str,
        pivot: &str,
        iterator: F,
    ) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        self.scan(true, false, true, index, pivot, "", iterator)
    }

    // DescendRange calls the iterator for every item in the database within
    // the range [lessOrEqual, greaterThan), until iterator returns false.
    // When an index is provided, the results will be ordered by the item values
    // as specified by the less() function of the defined index.
    // When an index is not provided, the results will be ordered by the item key.
    // An invalid index will return an error.
    fn descend_range<F>(
        &mut self,
        index: &str,
        less_or_equal: &str,
        greater_than: &str,
        iterator: F,
    ) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        self.scan(
            true,
            true,
            true,
            index,
            less_or_equal,
            greater_than,
            iterator,
        )
    }

    // AscendEqual calls the iterator for every item in the database that equals
    // pivot, until iterator returns false.
    // When an index is provided, the results will be ordered by the item values
    // as specified by the less() function of the defined index.
    // When an index is not provided, the results will be ordered by the item key.
    // An invalid index will return an error.
    fn ascend_equal<F>(&mut self, index: &str, pivot: &str, mut iterator: F) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        let mut less_fn = None;
        if !index.is_empty() {
            less_fn = Some(self.get_less(index.to_string())?);
        }

        self.ascend_greater_or_equal(&index, &pivot, |k, v| {
            eprintln!(
                "ascend greater or eqal pivot: {:#?}, key: {}, val: {}",
                pivot, k, v
            );
            if let Some(less_fn_) = &less_fn {
                if less_fn_(pivot, v) {
                    eprintln!("less fn true, stop {} {}", pivot, v);
                    return false;
                }
            } else if k != pivot {
                eprintln!("k != pivot, stop");
                return false;
            }
            let i = iterator(k, v);
            eprintln!("iter result {} {} {}", k, v, i);
            i
        })
    }

    // DescendEqual calls the iterator for every item in the database that equals
    // pivot, until iterator returns false.
    // When an index is provided, the results will be ordered by the item values
    // as specified by the less() function of the defined index.
    // When an index is not provided, the results will be ordered by the item key.
    // An invalid index will return an error.
    fn descend_equal<F>(&mut self, index: &str, pivot: &str, mut iterator: F) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        let mut less_fn = None;
        if !index.is_empty() {
            less_fn = Some(self.get_less(index.to_string())?);
        }
        eprintln!("descend equal called!");

        self.descend_less_or_equal(&index, &pivot, |k, v| {
            if let Some(less_fn_) = &less_fn {
                if less_fn_(v, pivot) {
                    eprintln!("less fn false, stop");
                    return false;
                }
            } else if k != pivot {
                eprintln!("k != pivot, stop");
                return false;
            }
            let i = iterator(k, v);
            eprintln!("iterator result {}", i);
            i
        })
    }

    // Nearby searches for rectangle items that are nearby a target rect.
    // All items belonging to the specified index will be returned in order of
    // nearest to farthest.
    // The specified index must have been created by AddIndex() and the target
    // is represented by the rect string. This string will be processed by the
    // same bounds function that was passed to the CreateSpatialIndex() function.
    // An invalid index will return an error.
    // The dist param is the distance of the bounding boxes. In the case of
    // simple 2D points, it's the distance of the two 2D points squared.
    fn nearby<F>(&mut self, index: String, bounds: String, iterator: F) -> Result<(), DbError>
    where
        F: Fn(String, String, f64) -> bool,
    {
        todo!()
    }

    // Intersects searches for rectangle items that intersect a target rect.
    // The specified index must have been created by AddIndex() and the target
    // is represented by the rect string. This string will be processed by the
    // same bounds function that was passed to the CreateSpatialIndex() function.
    // An invalid index will return an error.
    fn intersects<F>(&mut self, index: String, bounds: String, iterator: F) -> Result<(), DbError>
    where
        F: Fn(String, String) -> bool,
    {
        todo!()
    }

    // Len returns the number of items in the database
    fn len(&self) -> Result<u64, DbError> {
        if self.db.is_none() {
            return Err(DbError::TxClosed);
        }

        let db = self.db.as_ref().unwrap();
        Ok(db.keys.count())
    }
}

impl<'db> Drop for Tx<'db> {
    fn drop(&mut self) {
        if self.db.is_some() && self.has_lock {
            self.unlock();
        }
    }
}

// SetOptions represents options that may be included with the Set() command.
struct SetOptions {
    // Expires indicates that the Set() key-value will expire
    expires: bool,
    // TTL is how much time the key-value will exist in the database
    // before being evicted. The Expires field must also be set to true.
    // TTL stands for Time-To-Live.
    ttl: time::Duration,
}

// rect is used by Intersects and Nearby
struct Rect {
    min: Vec<f64>,
    max: Vec<f64>,
}

fn btree_lt(tree: &BTreeC<DbItem>, a: &DbItem, b: &DbItem) -> bool {
    tree.less(a, b)
}

fn btree_gt(tree: &BTreeC<DbItem>, a: &DbItem, b: &DbItem) -> bool {
    tree.less(b, a)
}

// Ascend helpers

fn btree_ascend<F>(tree: &BTreeC<DbItem>, mut iterator: F)
where
    F: FnMut(&str, &str) -> bool,
{
    tree.ascend(None, |item| iterator(&item.key, &item.val));
}

fn btree_ascend_less_than<F>(tree: &BTreeC<DbItem>, pivot: &DbItem, mut iterator: F)
where
    F: FnMut(&str, &str) -> bool,
{
    tree.ascend(None, |item| {
        btree_lt(tree, item, pivot) && iterator(&item.key, &item.val)
    });
}

fn btree_ascend_greater_or_equal<F>(tree: &BTreeC<DbItem>, pivot: Option<DbItem>, mut iterator: F)
where
    F: FnMut(&str, &str) -> bool,
{
    eprintln!(
        "ascent greater or equal pivot, key: {} val: {}",
        pivot.as_ref().unwrap().key,
        pivot.as_ref().unwrap().val
    );
    tree.ascend(pivot, |item| {
        eprintln!("tree ascend key: {} val: {}", item.key, item.val);
        iterator(&item.key, &item.val)
    });
    eprintln!("called tree ascend");
}

fn btree_ascend_range<F>(
    tree: &BTreeC<DbItem>,
    greater_or_equal: &DbItem,
    less_than: &DbItem,
    mut iterator: F,
) where
    F: FnMut(&str, &str) -> bool,
{
    tree.ascend(Some(greater_or_equal.clone()), |item| {
        btree_lt(tree, item, less_than) && iterator(&item.key, &item.val)
    });
}

// Descend helpers

fn btree_descend<F>(tree: &BTreeC<DbItem>, mut iterator: F)
where
    F: FnMut(&str, &str) -> bool,
{
    tree.descend(None, |item| iterator(&item.key, &item.val));
}

fn btree_descend_greater_than<F>(tree: &BTreeC<DbItem>, pivot: &DbItem, mut iterator: F)
where
    F: FnMut(&str, &str) -> bool,
{
    tree.descend(None, |item| {
        btree_gt(tree, item, pivot) && iterator(&item.key, &item.val)
    });
}

fn btree_descend_range<F>(
    tree: &BTreeC<DbItem>,
    less_or_equal: &DbItem,
    greater_than: &DbItem,
    mut iterator: F,
) where
    F: FnMut(&str, &str) -> bool,
{
    tree.descend(Some(less_or_equal.clone()), |item| {
        btree_gt(tree, item, greater_than) && iterator(&item.key, &item.val)
    });
}

fn btree_descend_less_or_equal<F>(tree: &BTreeC<DbItem>, pivot: Option<DbItem>, mut iterator: F)
where
    F: FnMut(&str, &str) -> bool,
{
    eprintln!(
        "descent less or equal pivot, key: {} val: {}",
        pivot.as_ref().unwrap().key,
        pivot.as_ref().unwrap().val
    );
    tree.descend(pivot, |item| {
        eprintln!("called!");
        iterator(&item.key, &item.val)
    });
}

// index_int is a helper function that returns true if 'a` is less than 'b'
fn index_int(a: &str, b: &str) -> bool {
    let ia = a.parse::<i32>().unwrap();
    let ib = b.parse::<i32>().unwrap();
    eprintln!("index_int a: {} b: {}", ia, ib);
    ia < ib
}

// IndexString is a helper function that return true if 'a' is less than 'b'.
// This is a case-insensitive comparison. Use the IndexBinary() for comparing
// case-sensitive strings.
fn index_string(a: &str, b: &str) -> bool {
    let min_len = std::cmp::min(a.len(), b.len());
    let a_chars = a.chars().collect::<Vec<_>>();
    let b_chars = b.chars().collect::<Vec<_>>();

    for i in 0..min_len {
        if a_chars[i] >= 'A' && a_chars[i] <= 'Z' {
            if b_chars[i] >= 'A' && b_chars[i] <= 'Z' {
                // both are upper case, do nothing
                if a_chars[i] < b_chars[i] {
                    return true;
                }
                if a_chars[i] > b_chars[i] {
                    return true;
                }
            } else {
                // a is uppercase, convert to lowercase
                if a_chars[i].to_ascii_lowercase() < b_chars[i] {
                    return true;
                }
                if a_chars[i].to_ascii_lowercase() > b_chars[i] {
                    return true;
                }
            }
        } else if b_chars[i] >= 'A' && b_chars[i] <= 'Z' {
            // a is uppercase, convert to lowercase
            if a_chars[i] < b_chars[i].to_ascii_lowercase() {
                return true;
            }
            if a_chars[i] > b_chars[i].to_ascii_lowercase() {
                return true;
            }
        } else {
            // neither are uppercase
            if a_chars[i] < b_chars[i] {
                return true;
            }
            if a_chars[i] > b_chars[i] {
                return false;
            }
        }
    }

    a.len() < b.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! svec {
        ($($x:expr),*) => (vec![$($x.to_string()),*]);
    }

    fn test_open() -> Db {
        std::fs::remove_file("data.db");
        test_reopen(None)
    }

    fn test_reopen(maybe_db: Option<Db>) -> Db {
        test_reopen_delay(maybe_db, time::Duration::new(0, 0))
    }

    fn test_reopen_delay(maybe_db: Option<Db>, duration: time::Duration) -> Db {
        if let Some(db) = maybe_db {
            db.close().unwrap();
        }
        std::thread::sleep(duration);
        Db::open("data.db").unwrap()
    }

    fn test_close(db: Db) {
        let _ = db.close();
        // let _ = std::fs::remove_file("data.db");
    }

    #[test]
    fn save_load() {
        let db = Db::open(":memory:").unwrap();
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
            tx.set("1".to_string(), "3".to_string(), None);
            tx.set("2".to_string(), "2".to_string(), None);
            tx.set("3".to_string(), "1".to_string(), None);
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
            tx.set("4".to_string(), "0".to_string(), None);
            ascend_equal(tx, "idx1", svec!["4", "0", "3", "1", "2", "2", "1", "3"]);
            Err(DbError::Custom("this is fine".to_string()))
        })
        .unwrap_err();

        // test to see if rollback happened
        db.view(|tx| {
            ascend_equal(tx, "idx1", svec!["3", "1", "2", "2", "1", "3"]);
            Ok(())
        });

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
            tx.set("4".to_string(), "0".to_string(), None);
            ascend_equal(tx, "idx1", svec!["4", "0", "2", "2", "1", "3"]);
            ascend_equal(tx, "idx2", svec!["4", "0", "2", "2", "1", "3"]);
            tx.delete_all();
            ascend_equal(tx, "idx1", svec![]);
            ascend_equal(tx, "idx2", svec![]);
            tx.set("1".to_string(), "3".to_string(), None);
            tx.set("2".to_string(), "2".to_string(), None);
            // FIXME: there should be unwraps here, but it panics on `IndexExists`.
            // It seems these are spurious? Indexes are not deleted by `delete_all()`
            tx.create_index(
                "idx1".to_string(),
                "*".to_string(),
                vec![Arc::new(index_int)],
            );
            tx.create_index(
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
            assert_eq!(err, DbError::NotFound);

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
            tx.set("hello1".to_string(), "planet1".to_string(), None);
            tx.set("hello2".to_string(), "planet2".to_string(), None);
            tx.set("hello3".to_string(), "planet3".to_string(), None);
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
            tx.set("hello1".to_string(), "planet1.1".to_string(), None);
            tx.delete_all();
            tx.set("bb".to_string(), "11".to_string(), None);
            tx.set("aa".to_string(), "**".to_string(), None);
            tx.delete("aa".to_string());
            tx.set("aa".to_string(), "22".to_string(), None);
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
            tx.set("hello".to_string(), "planet".to_string(), None);
            Ok(())
        })
        .unwrap();

        let err_broken = DbError::Custom("broken".to_string());
        let e = db
            .update::<_, ()>(|tx| {
                tx.set("hello".to_string(), "world".to_string(), None);
                Err(err_broken.clone())
            })
            .unwrap_err();
        assert_eq!(e, err_broken);

        let val = db.view(|tx| tx.get("hello".to_string(), true)).unwrap();
        assert_eq!(val, "planet");

        db.update(|tx| {
            let saved_db = tx.db.take().unwrap();
            let e = tx
                .set("hello".to_string(), "planet".to_string(), None)
                .unwrap_err();
            assert_eq!(e, DbError::TxClosed);
            let e = tx.delete("hello".to_string()).unwrap_err();
            assert_eq!(e, DbError::TxClosed);
            let e = tx.get("hello".to_string(), true).unwrap_err();
            assert_eq!(e, DbError::TxClosed);

            tx.db = Some(saved_db);
            tx.writable = false;
            let e = tx
                .set("hello".to_string(), "planet".to_string(), None)
                .unwrap_err();
            assert_eq!(e, DbError::TxNotWritable);
            let e = tx.delete("hello".to_string()).unwrap_err();
            assert_eq!(e, DbError::TxNotWritable);
            tx.writable = true;

            let e = tx.get("something".to_string(), true).unwrap_err();
            assert_eq!(e, DbError::NotFound);
            let e = tx.delete("something".to_string()).unwrap_err();
            assert_eq!(e, DbError::NotFound);

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
            assert_eq!(e, DbError::NotFound);
            let e = tx.delete("something".to_string()).unwrap_err();
            assert_eq!(e, DbError::NotFound);

            Ok(())
        })
        .unwrap();

        // test non-managed transactions
        let mut tx = db.begin(true).unwrap();
        tx.set("howdy".to_string(), "world".to_string(), None);
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
        tx2.commit();
        drop(tx2);

        // test fo closed transactions
        let err = db
            .update(|tx| {
                tx.db = None;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(err, DbError::TxClosed);
        unsafe { db.mu.unlock_exclusive() };

        // test for invalid writes

        test_close(db);
    }

    #[test]
    #[should_panic]
    fn test_panic_during_commit_in_managed_tx() {
        let mut db = Db::open(":memory:").unwrap();
        db.update(|tx| {
            tx.commit();
            Ok(())
        });
    }

    #[test]
    #[should_panic]
    fn test_panic_during_rollback_in_managed_tx() {
        let mut db = Db::open(":memory:").unwrap();
        db.update(|tx| {
            tx.rollback();
            Ok(())
        });
    }
}
