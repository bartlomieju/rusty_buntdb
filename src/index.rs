use btreec::BTreeC;
use std::cmp::Ordering;

use crate::item::DbItem;
use crate::LessFn;
use crate::RectFn;
use std::sync::Arc;

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
    pattern: String,

    /// less comparison function
    pub less: Option<Arc<LessFn>>,

    /// rect from string function
    pub rect: Option<Arc<RectFn>>,

    /// index options
    opts: IndexOptions,
}

impl Index {
    pub fn new(
        name: String,
        pattern: String,
        less: Option<Arc<LessFn>>,
        rect: Option<Arc<RectFn>>,
        opts: IndexOptions,
    ) -> Self {
        Index {
            btr: None,
            name,
            pattern,
            less,
            rect,
            opts,
        }
    }

    pub fn matches(&self, key: &str) -> bool {
        let mut key = key.to_string();
        if self.pattern == "*" {
            return true;
        }

        if self.opts.case_insensitive_key_matching {
            let chars_iter = key.chars();
            for char_ in chars_iter {
                if ('A'..='Z').contains(&char_) {
                    eprintln!("got lower case");
                    key = key.to_lowercase();
                    break;
                }
            }
        }

        crate::matcher::matches(&self.pattern, &key)
    }

    // `clear_copy` creates a copy of the index, but with an empty dataset.
    pub fn clear_copy(&self) -> Index {
        // copy the index meta information
        let mut nidx = Index {
            btr: None,
            name: self.name.clone(),
            pattern: self.pattern.clone(),
            less: self.less.clone(),
            rect: self.rect.clone(),
            opts: self.opts.clone(),
        };

        // initialize with empty trees
        // NOTE: keep in sync with fn in `rebuild`
        if let Some(less_fn) = self.less.clone() {
            let compare_fn = Box::new(move |a: &DbItem, b: &DbItem| {
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
    pub fn rebuild(&mut self, keys: &BTreeC<DbItem>) {
        // initialize trees
        // NOTE: keep in sync with fn in `clear_copy`
        if let Some(less_fn) = self.less.clone() {
            let compare_fn = Box::new(move |a: &DbItem, b: &DbItem| {
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
            self.btr = Some(btree);
        }
        if self.rect.is_some() {
            // TODO:
            // self.rtr =
        }
        // iterate through all keys and fill the index
        keys.ascend(None, |item| {
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
