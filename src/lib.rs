//! Package buntdb implements a low-level in-memory key/value store in pure Go.
//! It persists to disk, is ACID compliant, and uses locking for multiple
//! readers and a single writer. Bunt is ideal for projects that need a
//! dependable database, and favor speed over data size.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::sync::RwLock;
use std::time::SystemTime;

#[derive(Debug)]
pub enum BuntDBError {
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
}

impl fmt::Display for BuntDBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use BuntDBError::*;
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
        }
    }
}

impl Error for BuntDBError {}

/// DB represents a collection of key-value pairs that persist on disk.
/// Transactions are used for all forms of data access to the DB.
pub struct DB {
    /// the gatekeeper for all fields
    mu: RwLock<()>,

    /// the underlying file
    file: Option<File>,

    /// a buffer to write to
    // buf:       []byte,

    /// a tree of all item ordered by key
    // keys:      *btree.BTree,

    /// a tree of items ordered by expiration
    // exps:      *btree.BTree,

    /// the index trees.
    idxs: HashMap<String, Index>,

    /// a reuse buffer for gathering indexes
    // insIdxs:   []*index,

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
#[derive(Clone)]
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
    // on_expired: func(keys []string),

    // `on_expired_sync` will be called inside the same transaction that is
    // performing the deletion of expired items. If OnExpired is present then
    // this callback will not be called. If this callback is present, then the
    // deletion of the timeed-out item is the explicit responsibility of this
    // callback.
    // on_expired_sync: func(key, value string, tx *Tx) error,
}

// `ExCtx` is a simple b-tree context for ordering by expiration.
struct ExCtx {
    db: DB,
}

impl DB {
    pub fn open(path: String) -> Result<DB, io::Error> {
        // initialize default configuration
        let config = Config {
            auto_shrink_percentage: 100,
            auto_shrink_min_size: 32 * 1024 * 1024,
            ..Default::default()
        };

        let mut db = DB {
            mu: RwLock::new(()),
            file: None,

            // TODO:
            // buf: ,
            // exps: ,
            idxs: HashMap::new(),
            // insIdxs: ,
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
            if let Err(err) = db.load_from_disk() {
                // close on error, ignore close error
                db.file.take();
                return Err(err);
            }
        }

        // TODO:
        // start the background manager

        Ok(db)
    }

    /// `close` releases all database resources.
    /// All transactions must be closed before closing the database.
    pub fn close(mut self) -> Result<(), BuntDBError> {
        let _g = self.mu.write().unwrap();

        if self.closed {
            return Err(BuntDBError::DatabaseClosed);
        }

        self.closed = true;
        if self.persist {
            let file = self.file.take().unwrap();
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
    pub fn save(&mut self, writer: &dyn io::Write) -> Result<(), io::Error> {
        todo!();
    }

    /// `read_load` reads from the reader and loads commands into the database.
    /// modTime is the modified time of the reader, should be no greater than
    /// the current time.Now().
    /// Returns the number of bytes of the last command read and the error if any.
    pub fn read_load(
        &self,
        _reader: &dyn io::Read,
        _mod_time: SystemTime,
    ) -> (u64, Option<io::Error>) {
        todo!();
    }

    /// `load_from_disk` reads entries from the append only database file and fills the database.
    /// The file format uses the Redis append only file format, which is and a series
    /// of RESP commands. For more information on RESP please read
    /// http://redis.io/topics/protocol. The only supported RESP commands are DEL and
    /// SET.
    fn load_from_disk(&mut self) -> Result<(), io::Error> {
        let mut file = self.file.as_ref().unwrap().clone();
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
        let _g = self.mu.write().unwrap();

        if self.persist {
            let err = io::Error::new(io::ErrorKind::Other, BuntDBError::PersistenceActive);
            return Err(err);
        }

        let (_, maybe_err) = self.read_load(reader, SystemTime::now());

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
        less: Box<dyn Fn(String, String) -> bool>,
    ) -> Result<(), io::Error> {
        todo!()
    }

    /// ReplaceIndex builds a new index and populates it with items.
    /// The items are ordered in an b-tree and can be retrieved using the
    /// Ascend* and Descend* methods.
    /// If a previous index with the same name exists, that index will be deleted.
    pub fn replace_index(
        &mut self,
        name: String,
        pattern: String,
        less: Box<dyn Fn(String, String) -> bool>,
    ) -> Result<(), io::Error> {
        todo!()
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
        rect: Box<dyn Fn(String) -> (Vec<f64>, Vec<f64>)>,
    ) -> Result<(), io::Error> {
        todo!()
    }

    // ReplaceSpatialIndex builds a new index and populates it with items.
    // The items are organized in an r-tree and can be retrieved using the
    // Intersects method.
    // If a previous index with the same name exists, that index will be deleted.
    pub fn replace_spatial_index(
        &mut self,
        name: String,
        pattern: String,
        rect: Box<dyn Fn(String) -> (Vec<f64>, Vec<f64>)>,
    ) -> Result<(), io::Error> {
        todo!()
    }

    /// DropIndex removes an index.
    pub fn drop_index(&mut self, name: String) -> Result<(), io::Error> {
        todo!()
    }

    /// Indexes returns a list of index names.
    pub fn indexes(&self) -> (Vec<String>, io::Error) {
        todo!()
    }

    /// ReadConfig returns the database configuration.
    pub fn read_config(&self) -> Result<Config, BuntDBError> {
        let _g = self.mu.read().unwrap();
        if self.closed {
            return Err(BuntDBError::DatabaseClosed);
        }
        Ok(self.config.clone())
    }

    /// SetConfig updates the database configuration.
    pub fn set_config(&mut self, config: Config) -> Result<(), BuntDBError> {
        let _g = self.mu.read().unwrap();
        if self.closed {
            return Err(BuntDBError::DatabaseClosed);
        }
        self.config = config;
        Ok(())
    }

    /// insertIntoDatabase performs inserts an item in to the database and updates
    /// all indexes. If a previous item with the same key already exists, that item
    /// will be replaced with the new one, and return the previous item.
    pub fn insert_into_database(&mut self, item: DbItem) -> DbItem {
        todo!();
    }
}

/// `IndexOptions` provides an index with additional features or
/// alternate functionality.
#[derive(Clone)]
struct IndexOptions {
    /// `case_insensitive_key_matching` allow for case-insensitive
    /// matching on keys when setting key/values.
    case_insensitive_key_matching: bool,
}

/// `Index` represents a b-tree or r-tree index and also acts as the
/// b-tree/r-tree context for itself.
struct Index {
    // TODO: this should be an option
    // contains the items
    btr: Option<BTreeMap<String, String>>,

    /// contains the items
    // rtr     *rtred.RTree

    /// name of the index
    name: String,
    /// a required key pattern
    pattern: String,

    /// less comparison function
    // less: Box<dyn Fn(String, String) -> bool>,

    /// rect from string function
    // rect:    func(item string) (min, max []float64)

    /// the origin database
    // db: DB,

    /// index options
    opts: IndexOptions,
}

impl Index {
    pub fn r#match(&self, key: &str) -> bool {
        let mut key = key;
        if self.pattern == "*" {
            return true;
        }

        todo!()

        // if self.opts.case_insensitive_key_matching {
        //     let len = key.len();
        //     for i in 0..len {
        //         if key.get(i).unwrap() >= 'A' && key.get(i).unwrap() <= 'Z' {
        //             key = &key.to_lowercase();
        //             break;
        //         }
        //     }
        // }

        // self.pattern.matches(key).peekable().peek().is_some()
    }

    // `clear_copy` creates a copy of the index, but with an empty dataset.
    pub fn clear_copy(&self) -> Index {
        // copy the index meta information
        let nidx = Index {
            btr: None,
            name: self.name.clone(),
            pattern: self.pattern.clone(),
            // db: self.db.clone(),
            // less: self.less.clone(),
            // rect: self.rect.clone(),
            opts: self.opts.clone(),
        };

        // TODO:
        // initialize with empty trees
        // if nidx.less != nil {
        //     nidx.btr = btree.New(lessCtx(nidx))
        // }
        // if nidx.rect != nil {
        //     nidx.rtr = rtred.New(nidx)
        // }

        if nidx.opts.case_insensitive_key_matching {
            //
        }

        nidx
    }

    // `rebuild` rebuilds the index
    pub fn rebuild(&mut self) {
        todo!()
    }
}

/// DbItemOpts holds various meta information about an item.
pub struct DbItemOpts {
    /// does this item expire?
    ex: bool,
    /// when does this item expire?
    // TODO: probably wrong type?
    exat: SystemTime,
}

pub struct DbItem {
    // the binary key
    key: String,
    // the binary value
    val: String,
    // optional meta information
    opts: DbItemOpts,
    // keyless item for scanning
    keyless: bool,
}
