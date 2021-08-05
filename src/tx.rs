use btreec::BTreeC;
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::time;

use crate::btree_helpers::*;
use crate::exps_compare_fn;
use crate::index::Index;
use crate::index::IndexOptions;
use crate::item::DbItem;
use crate::item::DbItemOpts;
use crate::keys_compare_fn;
use crate::DbError;
use crate::DbLock;
use crate::LessFn;
use crate::RectFn;
use crate::SetOptions;
use crate::SyncPolicy;
use std::sync::Arc;

// Tx represents a transaction on the database. This transaction can either be
// read-only or read/write. Read-only transactions can be used for retrieving
// values for keys and iterating through keys and values. Read/write
// transactions can set and delete keys.
//
// All transactions must be committed or rolled-back when done.
pub struct Tx<'db> {
    /// the underlying database.
    pub(crate) db_lock: Option<DbLock<'db>>,
    /// when false mutable operations fail.
    pub(crate) writable: bool,
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
    pub(crate) fn new(db_lock: DbLock<'db>, writable: bool) -> Result<Self, DbError> {
        let mut tx = Tx {
            db_lock: Some(db_lock),
            writable,
            funcd: false,
            wc: None,
        };

        if tx.db_lock.as_ref().unwrap().as_ref().closed {
            tx.db_lock.take();
            return Err(DbError::DatabaseClosed);
        }

        if writable {
            tx.wc = Some(TxWriteContext::default());
        }

        Ok(tx)
    }

    pub fn with_managed<F, R>(&mut self, func: F) -> Result<R, DbError>
    where
        F: FnOnce(&mut Self) -> Result<R, DbError>,
    {
        self.funcd = true;
        let func_result = func(self);
        self.funcd = false;
        func_result
    }

    // DeleteAll deletes all items from the database.
    pub fn delete_all(&mut self) -> Result<(), DbError> {
        if self.db_lock.is_none() {
            return Err(DbError::TxClosed);
        } else if !self.writable {
            return Err(DbError::TxNotWritable);
        } else if self.wc.as_ref().unwrap().itercount > 0 {
            return Err(DbError::TxIterating);
        }

        let wc = self.wc.as_mut().unwrap();
        let db = self.db_lock.as_mut().unwrap().as_mut();

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

    pub fn indexes(&self) -> Result<Vec<String>, DbError> {
        if self.db_lock.is_none() {
            return Err(DbError::TxClosed);
        }

        let db = self.db_lock.as_ref().unwrap().as_ref();
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
        lessers: Vec<Arc<LessFn>>,
        rect: Option<Arc<RectFn>>,
        opts: Option<IndexOptions>,
    ) -> Result<(), DbError> {
        if self.db_lock.is_none() {
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

        let db = self.db_lock.as_mut().unwrap().as_mut();
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

        let mut pattern = pattern;
        let options = opts.unwrap_or_default();
        if options.case_insensitive_key_matching {
            pattern = pattern.to_lowercase();
        }
        eprintln!("creating index: {}", pattern);

        let mut idx = Index::new(name, pattern, less, rect, options);
        idx.rebuild(&db.keys);
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
    pub fn create_index(
        &mut self,
        name: String,
        pattern: String,
        less: Vec<Arc<LessFn>>,
    ) -> Result<(), DbError> {
        self.create_index_inner(name, pattern, less, None, None)
    }

    // CreateIndexOptions is the same as CreateIndex except that it allows
    // for additional options.
    pub fn create_index_options(
        &mut self,
        name: String,
        pattern: String,
        opts: IndexOptions,
        less: Vec<Arc<LessFn>>,
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
    pub fn create_spatial_index(
        &mut self,
        name: String,
        pattern: String,
        rect: Arc<RectFn>,
    ) -> Result<(), DbError> {
        self.create_index_inner(name, pattern, vec![], Some(rect), None)
    }

    // CreateSpatialIndexOptions is the same as CreateSpatialIndex except that
    // it allows for additional options.
    pub fn create_spatial_index_options(
        &mut self,
        name: String,
        pattern: String,
        rect: Arc<RectFn>,
        opts: IndexOptions,
    ) -> Result<(), DbError> {
        self.create_index_inner(name, pattern, vec![], Some(rect), Some(opts))
    }

    pub fn drop_index(&mut self, name: String) -> Result<(), DbError> {
        if self.db_lock.is_none() {
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
        let db = self.db_lock.as_mut().unwrap().as_mut();
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

    // rollbackInner handles the underlying rollback logic.
    // Intended to be called from Commit() and Rollback().
    fn rollback_inner(&mut self) {
        // rollback the deleteAll if needed
        let wc = self.wc.as_mut().unwrap();
        let db = self.db_lock.as_mut().unwrap().as_mut();

        if wc.rbkeys.is_some() {
            db.keys = wc.rbkeys.take().unwrap();
            db.idxs = wc.rbidxs.take().unwrap();
            db.exps = wc.rbexps.take().unwrap();
        }

        for (key, maybe_item) in wc.rollback_items.drain() {
            // TODO: make a helper on DbItem to create "key item"
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
                idx.rebuild(&db.keys);
                db.idxs.insert(name, idx);
            }
        }
    }

    // Commit writes all changes to disk.
    // An error is returned when a write error occurs, or when a Commit() is called
    // from a read-only transaction.
    pub fn commit(&mut self) -> Result<(), DbError> {
        if self.funcd {
            panic!("managed tx rollback not allowed");
        }

        if self.db_lock.is_none() {
            return Err(DbError::TxClosed);
        } else if !self.writable {
            return Err(DbError::TxNotWritable);
        }

        let db = self.db_lock.as_mut().unwrap().as_mut();
        let wc = self.wc.as_mut().unwrap();
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
            {
                let file = db.file.as_mut().unwrap();
                let mut total_written: i64 = 0;
                let mut buf = &mut db.buf[..];
                let mut has_err = false;

                while !buf.is_empty() {
                    match file.write(buf) {
                        Ok(0) => {
                            has_err = true;
                        }
                        Ok(n) => {
                            buf = &mut buf[n..];
                            total_written += n as i64;
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(_) => {
                            has_err = true;
                        }
                    };

                    if has_err {
                        break;
                    }
                }

                if has_err {
                    if total_written > 0 {
                        // There was a partial write to disk.
                        // We are possibly out of disk space.
                        // Delete the partially written bytes from the data file by
                        // seeking to the previously known position and performing
                        // a truncate operation.
                        // At this point a syscall failure is fatal and the process
                        // should be killed to avoid corrupting the file.
                        use std::io::Seek;
                        let pos = file.seek(io::SeekFrom::Current(-total_written)).unwrap();
                        file.set_len(pos).unwrap();
                    }
                    self.rollback_inner();
                }
            }

            let db = self.db_lock.as_mut().unwrap().as_mut();
            let file = db.file.as_mut().unwrap();

            if db.config.sync_policy == SyncPolicy::Always {
                let _ = file.sync_all();
            }

            // Increment the number of flushes. The background syncing uses this.
            db.flushes += 1;
        }
        // Clear the db field to disable this transaction from future use.
        // Unlock the database and allow for another writable transaction.
        self.db_lock.take();
        Ok(())
    }

    // Rollback closes the transaction and reverts all mutable operations that
    // were performed on the transaction such as Set() and Delete().
    //
    // Read-only transactions can only be rolled back, not committed.
    pub fn rollback(&mut self) -> Result<(), DbError> {
        if self.funcd {
            panic!("managed tx rollback not allowed");
        }

        if self.db_lock.is_none() {
            return Err(DbError::TxClosed);
        }
        // The rollback func does the heavy lifting.
        if self.writable {
            self.rollback_inner();
        }
        // Clear the db field to disable this transaction from future use
        self.db_lock.take();
        Ok(())
    }

    // GetLess returns the less function for an index. This is handy for
    // doing ad-hoc compares inside a transaction.
    // Returns ErrNotFound if the index is not found or there is no less
    // function bound to the index
    fn get_less(&self, index: String) -> Result<Arc<LessFn>, DbError> {
        if self.db_lock.is_none() {
            return Err(DbError::TxClosed);
        }

        let db = self.db_lock.as_ref().unwrap().as_ref();
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
    #[allow(unused)]
    fn get_rect(&self, _index: String) -> Result<(), DbError> {
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
    pub fn set(
        &mut self,
        key: String,
        val: String,
        set_opts: Option<SetOptions>,
        // TODO: could probably return Option<String> instead
    ) -> Result<(Option<String>, bool), DbError> {
        if self.db_lock.is_none() {
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
                    exat: time::SystemTime::now() + opts.ttl,
                });
            }
        }

        // Insert the item into the keys tree.
        let db = self.db_lock.as_mut().unwrap().as_mut();
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
    pub fn get(&mut self, key: String, ignore_expired: bool) -> Result<String, DbError> {
        if self.db_lock.is_none() {
            return Err(DbError::TxClosed);
        }
        let maybe_item = self.db_lock.as_ref().unwrap().as_ref().get(key);

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
    pub fn delete(&mut self, key: String) -> Result<String, DbError> {
        if self.db_lock.is_none() {
            return Err(DbError::TxClosed);
        } else if !self.writable {
            return Err(DbError::TxNotWritable);
        } else if self.wc.as_ref().unwrap().itercount > 0 {
            return Err(DbError::TxIterating);
        }

        let wc = self.wc.as_mut().unwrap();
        let db = self.db_lock.as_mut().unwrap().as_mut();
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
    pub fn ttl(&mut self, key: String) -> Result<Option<time::Duration>, DbError> {
        if self.db_lock.is_none() {
            return Err(DbError::TxClosed);
        }
        let db = self.db_lock.as_ref().unwrap().as_ref();
        let maybe_item = db.get(key);
        match maybe_item {
            None => Err(DbError::NotFound),
            Some(item) => {
                if let Some(opts) = &item.opts {
                    if opts.ex {
                        let dur = opts.exat.duration_since(time::SystemTime::now());
                        if dur.is_err() {
                            return Err(DbError::NotFound);
                        }
                        return Ok(Some(dur.unwrap()));
                    }
                }

                Ok(None)
            }
        }
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
        iterator: F,
    ) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        if self.db_lock.is_none() {
            return Err(DbError::TxClosed);
        }

        let db = self.db_lock.as_ref().unwrap().as_ref();
        let tr;

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
    pub fn ascend_keys<F>(&mut self, _pattern: &str, _iterator: F) -> Result<(), DbError>
    where
        F: FnMut(&str, &str) -> bool,
    {
        todo!()
    }

    // DescendKeys allows for iterating through keys based on the specified pattern.
    pub fn descend_keys<F>(&mut self, _pattern: String, _iterator: F) -> Result<(), DbError>
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
    pub fn ascend<F>(&mut self, index: String, iterator: F) -> Result<(), DbError>
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
    pub fn ascend_greater_or_equal<F>(
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
    pub fn ascend_less_than<F>(
        &mut self,
        index: &str,
        pivot: &str,
        iterator: F,
    ) -> Result<(), DbError>
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
    pub fn ascend_range<F>(
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
    pub fn descend<F>(&mut self, index: &str, iterator: F) -> Result<(), DbError>
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
    pub fn descend_greater_than<F>(
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
    pub fn descend_less_or_equal<F>(
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
    pub fn descend_range<F>(
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
    pub fn ascend_equal<F>(
        &mut self,
        index: &str,
        pivot: &str,
        mut iterator: F,
    ) -> Result<(), DbError>
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
    pub fn descend_equal<F>(
        &mut self,
        index: &str,
        pivot: &str,
        mut iterator: F,
    ) -> Result<(), DbError>
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
    pub fn nearby<F>(
        &mut self,
        _index: String,
        _bounds: String,
        _iterator: F,
    ) -> Result<(), DbError>
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
    pub fn intersects<F>(
        &mut self,
        _index: String,
        _bounds: String,
        _iterator: F,
    ) -> Result<(), DbError>
    where
        F: Fn(String, String) -> bool,
    {
        todo!()
    }

    // Len returns the number of items in the database
    pub fn len(&self) -> Result<u64, DbError> {
        if self.db_lock.is_none() {
            return Err(DbError::TxClosed);
        }

        let db = self.db_lock.as_ref().unwrap().as_ref();
        Ok(db.keys.count())
    }
}
