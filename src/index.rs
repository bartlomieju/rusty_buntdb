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

use crate::Db;
use crate::DbItem;
use crate::LessFn;
use crate::RectFn;

/// `IndexOptions` provides an index with additional features or
/// alternate functionality.
#[derive(Clone, Default)]
pub struct IndexOptions {
    /// `case_insensitive_key_matching` allow for case-insensitive
    /// matching on keys when setting key/values.
    pub case_insensitive_key_matching: bool,
}

/// `Index` represents a b-tree or r-tree index and also acts as the
/// b-tree/r-tree context for itself.
pub struct Index {
    // contains the items
    pub btr: Option<BTreeC<DbItem>>,

    /// contains the items
    // rtr     *rtred.RTree

    /// name of the index
    pub name: String,

    /// a required key pattern
    pub pattern: String,

    /// less comparison function
    pub less: Option<LessFn>,

    /// rect from string function
    pub rect: Option<RectFn>,

    /// the origin database
    // db: Arc<Db>,

    /// index options
    pub opts: IndexOptions,
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
