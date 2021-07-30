use crate::DbItem;
use btreec::BTreeC;

fn btree_lt(tree: &BTreeC<DbItem>, a: &DbItem, b: &DbItem) -> bool {
    tree.less(a, b)
}

fn btree_gt(tree: &BTreeC<DbItem>, a: &DbItem, b: &DbItem) -> bool {
    tree.less(b, a)
}

// Ascend helpers

pub fn btree_ascend<F>(tree: &BTreeC<DbItem>, mut iterator: F)
where
    F: FnMut(&str, &str) -> bool,
{
    tree.ascend(None, |item| iterator(&item.key, &item.val));
}

pub fn btree_ascend_less_than<F>(tree: &BTreeC<DbItem>, pivot: &DbItem, mut iterator: F)
where
    F: FnMut(&str, &str) -> bool,
{
    tree.ascend(None, |item| {
        btree_lt(tree, item, pivot) && iterator(&item.key, &item.val)
    });
}

pub fn btree_ascend_greater_or_equal<F>(
    tree: &BTreeC<DbItem>,
    pivot: Option<DbItem>,
    mut iterator: F,
) where
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

pub fn btree_ascend_range<F>(
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

pub fn btree_descend<F>(tree: &BTreeC<DbItem>, mut iterator: F)
where
    F: FnMut(&str, &str) -> bool,
{
    tree.descend(None, |item| iterator(&item.key, &item.val));
}

pub fn btree_descend_greater_than<F>(tree: &BTreeC<DbItem>, pivot: &DbItem, mut iterator: F)
where
    F: FnMut(&str, &str) -> bool,
{
    tree.descend(None, |item| {
        btree_gt(tree, item, pivot) && iterator(&item.key, &item.val)
    });
}

pub fn btree_descend_range<F>(
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

pub fn btree_descend_less_or_equal<F>(tree: &BTreeC<DbItem>, pivot: Option<DbItem>, mut iterator: F)
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
