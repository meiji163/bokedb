pub mod btree {
    use std::cmp::Ord;
    use std::collections::VecDeque;
    use std::fmt;
    use std::marker::Sized;

    /// Key and Val are trait aliases
    pub trait Key: Ord + Clone + fmt::Debug
    where
        Self: Sized,
    {
    }
    pub trait Val: Clone + fmt::Debug
    where
        Self: Sized,
    {
    }
    impl<T> Key for T where T: Ord + Clone + fmt::Debug {}
    impl<T> Val for T where T: Clone + fmt::Debug {}

    /// ------------------- Error Types -------------------
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct KeyNotFoundError;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct DuplicateKeyError;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct PageNotFoundError;

    impl fmt::Display for KeyNotFoundError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "key not found")
        }
    }

    impl fmt::Display for DuplicateKeyError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "duplicate key")
        }
    }

    impl fmt::Display for PageNotFoundError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "page not found")
        }
    }

    pub const PAGE_SIZE: usize = 65536;

    /// Page is a BTree page, which can hold keys or key-vals
    #[derive(Debug, Clone)]
    pub struct Page<K: Key, V: Val> {
        id: u32,
        deleted: Vec<bool>,   // soft delete info for leaf pages
        keys: Vec<K>,         // keys for interior and leaf pages
        vals: Vec<V>,         // vals corresponding to keys for leaf pages
        children: Vec<u32>,   // child page IDs for interior pages
        sibling: Option<u32>, // right sibling page ID for leaf pages
        is_leaf: bool,
    }

    /// BTree implements a B+Tree.
    /// Each page has at most b children, where b is odd.
    #[derive(Debug)]
    pub struct BTree<K: Key, V: Val> {
        b: usize,
        is_unique: bool,
        depth: usize,
        root_id: u32,
        next_id: u32,
        pager: Box<dyn Pager<K, V>>,
    }

    // TODO
    pub trait Pager<K: Key, V: Val>: fmt::Debug {
        fn read_page(&self, id: u32) -> Result<&Page<K, V>, PageNotFoundError>;
        fn write_page(&mut self, page: &Page<K, V>);
    }

    #[derive(Debug)]
    pub struct MemPager<K: Key, V: Val> {
        pages: Vec<Page<K, V>>,
    }

    impl<K: Key, V: Val> Pager<K, V> for MemPager<K, V> {
        fn read_page(&self, id: u32) -> Result<&Page<K, V>, PageNotFoundError> {
            let res = self.pages.binary_search_by_key(&id, |p| p.id);
            match res {
                Ok(idx) => Ok(&self.pages[idx]),
                Err(_) => Err(PageNotFoundError),
            }
        }
        fn write_page(&mut self, page: &Page<K, V>) {
            let res = self.pages.binary_search_by_key(&page.id, |p| p.id);
            match res {
                Ok(idx) => {
                    self.pages[idx] = page.clone();
                }
                Err(idx) => {
                    self.pages.insert(idx, page.clone());
                }
            }
        }
    }

    impl<K: Key, V: Val> Page<K, V> {
        // Return the first index <= key
        fn find(&self, key: &K) -> usize {
            let mut i = 0;
            for k in self.keys.iter() {
                if key > k {
                    i += 1;
                } else {
                    break;
                }
            }
            i
        }
    }

    /// ------------------- BTree Implementation -------------------
    impl<K: Key + 'static, V: Val + 'static> fmt::Display for BTree<K, V> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let vecs = self.traverse();
            for (l, ids) in vecs.iter().enumerate() {
                f.write_fmt(format_args!("LEVEL {:?}\n", l))?;

                for &id in ids.iter() {
                    f.write_fmt(format_args!("\t{:?}\n", self.pager.read_page(id).unwrap()))?;
                }
            }
            Ok(())
        }
    }

    impl<K: Key + 'static, V: Val + 'static> BTree<K, V> {
        pub fn new(b: usize, is_unique: bool) -> BTree<K, V> {
            assert_eq!(b % 2, 1);
            assert!(b > 2);

            let pager = Box::new(MemPager {
                pages: vec![Page {
                    id: 0,
                    keys: vec![],
                    vals: vec![],
                    children: vec![],
                    deleted: vec![],
                    is_leaf: true,
                    sibling: None,
                }],
            });

            BTree {
                b,
                is_unique,
                pager,
                depth: 0,
                root_id: 0,
                next_id: 1,
            }
        }

        // Rebuild the tree to remove soft deleted keys.
        // pub fn rebuild(&mut self) {
        //     let mut id = self.root_id;
        //     for _ in 0..self.depth {
        //         let page = &self.pager.read_page(id).unwrap();
        //         id = page.children[0];
        //     }

        //     // traverse leaf pages to collect kv's
        //     let mut keys: Vec<K> = Vec::new();
        //     let mut vals: Vec<V> = Vec::new();
        //     let max_pages = self.next_id;
        //     for _ in 0..max_pages {
        //         let page = self.pager.read_page(id).unwrap().clone();

        //         // copy keys and vals that aren't marked deleted
        //         let mut del = page.deleted.iter();
        //         let mut keep_keys: Vec<K> = page.keys.drain(0..).collect();
        //         keep_keys.retain(|_| !*del.next().unwrap());
        //         keys.extend(keep_keys.into_iter());

        //         del = page.deleted.iter();
        //         let mut keep_vals: Vec<V> = page.vals.drain(0..).collect();
        //         keep_vals.retain(|_| !*del.next().unwrap());
        //         vals.extend(keep_vals.into_iter());

        //         match page.sibling {
        //             Some(sid) => {
        //                 id = sid;
        //             }
        //             None => {
        //                 break;
        //             }
        //         }
        //     }

        //     // reset tree
        //     self.pages = Self::new(self.b, self.is_unique).pages;
        //     self.depth = 0;
        //     self.root_id = 0;
        //     self.next_id = 1;
        //     for (k, v) in keys.into_iter().zip(vals.into_iter()) {
        //         let _ = self.insert(k, v);
        //     }
        // }

        // Return the value associated with key, or None if it doesn't exist.
        // If there are multiple values associated with the key, any can be returned.
        pub fn find(&self, key: &K) -> Option<V> {
            let id = self.find_leaf(key);
            let leaf = self.pager.read_page(id).unwrap();
            match leaf.keys.binary_search(key) {
                Ok(idx) => {
                    if leaf.deleted[idx] {
                        None
                    } else {
                        Some(leaf.vals[idx].clone())
                    }
                }
                Err(_) => None,
            }
        }

        // Find key-value pairs where the min <= key <= max.
        pub fn find_range(&self, min: &K, max: &K) -> Vec<(K, V)> {
            let mut kvs = vec![];
            let mut id = self.find_leaf(min);
            let mut leaf = self.pager.read_page(id).unwrap();
            let mut idx = match leaf.keys.binary_search(min) {
                Ok(i) => i,
                Err(i) => i,
            };
            'outer: loop {
                for i in idx..leaf.vals.len() {
                    if leaf.keys[i] > *max {
                        break 'outer;
                    }
                    if !leaf.deleted[i] {
                        kvs.push((leaf.keys[i].clone(), leaf.vals[i].clone()));
                    }
                }
                match leaf.sibling {
                    Some(i) => {
                        id = i;
                    }
                    None => {
                        break;
                    }
                }
                leaf = self.pager.read_page(id).unwrap();
                idx = 0;
            }
            kvs
        }

        // Insert a key-val pair into the tree.
        pub fn insert(&mut self, key: K, val: V) -> Result<(), DuplicateKeyError> {
            let mut id = self.root_id;
            let mut visited = vec![];
            for _ in 0..self.depth {
                visited.push(id);
                let page = self.pager.read_page(id).unwrap();
                let idx = page.find(&key);
                id = page.children[idx];
            }

            // attempt insert key-val in the leaf page
            let mut page = self.pager.read_page(id).unwrap().clone();
            let search = page.keys.binary_search(&key);
            let idx = search.unwrap_or_else(|x| x);
            if search.is_err() || (search.is_ok() && !self.is_unique) {
                // key is not present, or duplicates are allowed:
                // OK to insert
                page.keys.insert(idx, key.clone());
                page.vals.insert(idx, val);
                page.deleted.insert(idx, false);
            } else {
                // duplicate key found on a unique tree:
                // try to replace a deleted entry, otherwise error
                if page.deleted[idx] {
                    page.vals[idx] = val;
                    page.deleted[idx] = false;
                    self.pager.write_page(&page);
                    return Ok(());
                } else {
                    return Err(DuplicateKeyError);
                }
            }

            // since we inserted one entry, we can garbage collect one entry
            let mut del_idx = page
                .deleted
                .iter()
                .rev()
                .enumerate()
                .filter(|&(_, &b)| b)
                .map(|(i, _)| i);
            if let Some(i) = del_idx.next() {
                page.deleted.remove(i);
                page.keys.remove(i);
                page.vals.remove(i);
            }

            let mut needs_split = page.keys.len() >= self.b;
            if !needs_split {
                self.pager.write_page(&page);
                Ok(())
            } else {
                let mut par_id_opt = visited.pop();
                // TODO: OVERFLOW BORKEN
                // try to overflow to sibling first
                // if page.sibling.is_some() && par_id_opt.is_some() {
                //     let par_id = par_id_opt.unwrap();
                //     let sib_id = page.sibling.unwrap();
                //     let mut parent = self.pager.read_page(par_id).unwrap().clone();
                //     let mut sibling = self.pager.read_page(sib_id).unwrap().clone();

                //     if let Ok(()) = self.overflow_to_sibling(&mut page, &mut sibling, &mut parent) {
                //         self.pager.write_page(&page);
                //         self.pager.write_page(&sibling);
                //         self.pager.write_page(&parent);
                //         return Ok(());
                //     }
                // }
                // split page and propagate split upward if necessary
                let max_splits = self.depth + 1;
                for _ in 0..max_splits {
                    match par_id_opt {
                        Some(par_id) => {
                            let mut parent = self.pager.read_page(par_id).unwrap().clone();
                            let sibling = self.split_page(&mut page, &mut parent);
                            self.pager.write_page(&page);
                            self.pager.write_page(&sibling);

                            needs_split = parent.keys.len() >= self.b;
                            if !needs_split {
                                self.pager.write_page(&parent);
                                break;
                            } else {
                                // loop
                                page = parent;
                                par_id_opt = visited.pop();
                            }
                        }
                        None => {
                            // split root
                            assert_eq!(self.root_id, page.id);
                            let (sibling, root) = self.split_root(&mut page);

                            self.pager.write_page(&page);
                            self.pager.write_page(&sibling);
                            self.pager.write_page(&root);
                            break;
                        }
                    }
                }
                Ok(())
            }
        }

        // Attempt to overflow keys & vals of the leaf page to its right sibling.
        // Returns Err if overflow not possible.
        // fn overflow_to_sibling(
        //     &mut self,
        //     page: &mut Page<K, V>,
        //     sibling: &mut Page<K, V>,
        //     parent: &mut Page<K, V>,
        // ) -> Result<(), &'static str> {
        //     let p = page.keys.len();
        //     let s = sibling.keys.len();
        //     // how many keys to overflow from page?
        //     let mov = {
        //         // p > s + 1 ensures we can move at least one key to sibling
        //         // p + s < b ensures we don't underflow
        //         if s >= self.b || p <= s + 1 || p + s < self.b {
        //             return Err("not attempting overflow");
        //         }
        //         (p - s) / 2
        //     };

        //     // move data to sibling
        //     // TODO: insert(0,-) is inefficient
        //     for k in page.keys.drain((p - 1 - mov)..) {
        //         sibling.keys.insert(0, k);
        //     }
        //     for v in page.vals.drain((p - 1 - mov)..) {
        //         sibling.vals.insert(0, v);
        //     }
        //     for d in page.deleted.drain((p - 1 - mov)..) {
        //         sibling.deleted.insert(0, d);
        //     }

        //     // update parent key
        //     let max_key = page.keys.last().unwrap().clone();
        //     let idx = parent.find(&max_key);
        //     parent.keys[idx] = max_key;

        //     Ok(())
        // }

        // Split the given page into two and promote a key its parent page.
        // Mutates the page and parent and return the new right sibling.
        fn split_page(&mut self, page: &mut Page<K, V>, parent: &mut Page<K, V>) -> Page<K, V> {
            //println!("{} {}", page.keys.len(), self.b);
            assert!(page.keys.len() >= self.b);
            let split_idx = self.b / 2;
            let split_key = page.keys[split_idx].clone();
            // allocate right child page. the current page becomes left child page
            let sibling = self.divide_page(page);

            // insert left and right as parent's children
            let idx = parent.find(&split_key);
            parent.keys.insert(idx, split_key);
            parent.children.insert(idx, page.id);
            parent.children[idx + 1] = sibling.id;

            sibling
        }

        // Splits a page without a parent i.e. the root page.
        // In this case a new root page is created along with the right sibling page.
        // Returns (sibling , new root).
        fn split_root(&mut self, page: &mut Page<K, V>) -> (Page<K, V>, Page<K, V>) {
            let split_idx = self.b / 2;
            let split_key = page.keys[split_idx].clone();
            let sibling = self.divide_page(page);
            // current page was the root page; create a new root
            let new_root: Page<K, V> = Page {
                id: self.next_id as u32,
                keys: vec![split_key],
                children: vec![page.id, sibling.id],
                vals: Vec::new(),
                is_leaf: false,
                sibling: None,
                deleted: vec![],
            };
            self.next_id += 1;
            self.root_id = new_root.id;
            self.depth += 1;
            (sibling, new_root)
        }

        // Helper function for page splitting: divide upper half of page into
        // a new (right) sibling and returns the sibling.
        fn divide_page(&mut self, page: &mut Page<K, V>) -> Page<K, V> {
            let split_idx = self.b / 2;

            // allocate right child page. the current page becomes left child page
            let mut r_page = Page {
                id: self.next_id as u32,
                keys: Vec::with_capacity(split_idx),
                vals: Vec::with_capacity(split_idx),
                deleted: Vec::with_capacity(split_idx),
                children: vec![],
                is_leaf: page.is_leaf.clone(),
                sibling: page.sibling,
            };
            self.next_id += 1;
            r_page.keys = page.keys.drain((split_idx + 1)..).collect();

            if page.is_leaf {
                r_page.vals = page.vals.drain((split_idx + 1)..).collect();
                r_page.deleted = page.deleted.drain((split_idx + 1)..).collect();
                page.sibling = Some(r_page.id);
            } else {
                r_page.children = page.children.drain((split_idx + 1)..).collect();
            }
            r_page
        }

        fn find_leaf(&self, key: &K) -> u32 {
            let mut id = self.root_id;
            for _ in 0..self.depth {
                let page = self.pager.read_page(id).unwrap();
                let idx = page.find(key);
                id = page.children[idx];
            }
            id
        }

        // Mark entries associatied with key as deleted
        pub fn delete(&mut self, key: &K) -> Result<usize, KeyNotFoundError> {
            let mut id = self.find_leaf(key);
            let mut n_deleted = 0;

            'outer: loop {
                let mut leaf = self.pager.read_page(id).unwrap().clone();
                let idx = leaf.find(key);
                for i in idx..leaf.deleted.len() {
                    if leaf.keys[i] != *key {
                        break 'outer;
                    }
                    leaf.deleted[i] = true;
                    n_deleted += 1;
                    self.pager.write_page(&leaf);
                }
                // we may have to search the siblings
                match leaf.sibling {
                    Some(sid) => {
                        id = sid;
                    }
                    None => {
                        break;
                    }
                }
            }
            if n_deleted > 0 {
                Ok(n_deleted)
            } else {
                Err(KeyNotFoundError)
            }
        }

        // traverse page IDs in level order
        fn traverse(&self) -> Vec<Vec<u32>> {
            let mut lvl = 0;
            let mut ids = vec![vec![self.root_id]];
            let mut q = VecDeque::from([(0, self.root_id)]);
            let max_loop = self.next_id;
            for _ in 0..=max_loop {
                match q.pop_front() {
                    Some((l, id)) => {
                        if l >= lvl {
                            lvl += 1;
                            ids.push(vec![]);
                        }
                        let page = self.pager.read_page(id).unwrap();
                        if page.is_leaf {
                            continue;
                        }
                        for cid in page.children.iter() {
                            ids[lvl].push(cid.clone());
                            q.push_back((l + 1, cid.clone()));
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
            ids.pop();
            ids
        }
    }
}

#[cfg(test)]
mod tests {
    use super::btree::*;
    use rand::prelude::*;

    #[test]
    fn test_insert_no_split() {
        let mut bt: BTree<i32, i32> = BTree::new(27, false);
        let kvs = [(5, 50), (6, 60), (7, -70), (7, 70), (8, 80)];
        for (k, v) in kvs.into_iter() {
            let err = bt.insert(k, v);
            assert!(err.is_ok());
        }
        assert_eq!(bt.find(&5), Some(50));
        assert_eq!(bt.find(&6), Some(60));
        assert_eq!(bt.find(&8), Some(80));
    }

    #[test]
    fn test_insert_split() {
        let mut bt: BTree<i32, i32> = BTree::new(3, true);
        let kvs = [(5, 55), (6, 66), (7, 77), (8, 88), (9, 99), (10, 100)];
        for (k, v) in kvs.into_iter() {
            let err = bt.insert(k, v);
            assert!(err.is_ok());
        }

        assert_eq!(bt.find(&5), Some(55));
        assert_eq!(bt.find(&6), Some(66));
        assert_eq!(bt.find(&7), Some(77));
        assert_eq!(bt.find(&9), Some(99));
        assert_eq!(bt.find(&8), Some(88));
        assert_eq!(bt.find(&666), None);
    }

    #[test]
    fn test_insert_rand() {
        let mut rng = rand::thread_rng();
        let sizes = [33, 101, 179, 213, 303];
        for size in sizes.into_iter() {
            println!("size={}", size);
            let mut bt: BTree<i32, i32> = BTree::new(size, true);
            let mut keys: Vec<i32> = (0..50000)
                .map(|_| {
                    let k = rng.gen::<i32>();
                    return k;
                })
                .collect();
            keys.sort();
            keys.dedup();
            let mut vals = vec![];
            for &k in keys.iter() {
                let val: i32 = rng.gen();
                vals.push(val);
                let err = bt.insert(k, val);
                assert!(err.is_ok());
            }
            for (k, v) in keys.iter().zip(vals) {
                assert_eq!(bt.find(k), Some(v));
            }
        }
    }

    #[test]
    fn test_delete_rand() {
        let sizes = [71, 155, 191, 211, 301];
        for size in sizes.into_iter() {
            println!("size={}", size);
            let mut rng = rand::thread_rng();
            let mut bt: BTree<i32, i32> = BTree::new(size, true);
            let n = 50000;
            let mut keys: Vec<i32> = (0..n)
                .map(|_| {
                    let k = rng.gen::<i32>();
                    return k;
                })
                .collect();
            keys.sort();
            keys.dedup();
            let mut vals = vec![];
            for &k in keys.iter() {
                let val: i32 = rng.gen();
                vals.push(val);
                let err = bt.insert(k, val);
                assert!(err.is_ok());
            }
            for k in keys[n / 2..].iter() {
                let err = bt.delete(k);
                assert!(err.is_ok());
            }
            for (i, k) in keys[0..n / 2].iter().enumerate() {
                assert_eq!(bt.find(k), Some(vals[i]));
            }
        }
    }

    #[test]
    fn test_delete_one() {
        let mut bt: BTree<i32, i32> = BTree::new(3, true);
        let kvs = [(3, 333), (5, 55), (6, 66), (7, 77), (9, 99), (10, 100)];
        for (k, v) in kvs.into_iter() {
            let err = bt.insert(k, v);
            assert!(err.is_ok());
        }

        assert_eq!(bt.find(&5), Some(55));
        let err = bt.delete(&5);
        assert!(err.is_ok());
        assert_eq!(err.unwrap(), 1 as usize);
        assert_eq!(bt.find(&5), None);
    }

    #[test]
    fn test_duplicate_key() {
        let mut bt: BTree<i32, i32> = BTree::new(5, true);
        assert!(bt.insert(5, 55).is_ok());
        let err = bt.insert(5, 555);
        assert_eq!(err, Err(DuplicateKeyError));

        assert!(bt.delete(&5).is_ok());
        assert!(bt.insert(5, 555).is_ok());
    }

    // #[test]
    // fn test_rebuild() {
    //     let mut bt: BTree<i32, i32> = BTree::new(3, false);
    //     let kvs = [
    //         (5, 55),
    //         (6, 66),
    //         (7, 77),
    //         (9, 99),
    //         (9, 999),
    //         (9, 9999),
    //         (10, 100),
    //         (3, 333),
    //     ];
    //     for (k, v) in kvs.into_iter() {
    //         let err = bt.insert(k, v);
    //         assert!(err.is_ok());
    //     }
    //     assert!(bt.delete(&5).is_ok());
    //     assert!(bt.delete(&9).is_ok());
    //     assert!(bt.delete(&7).is_ok());

    //     bt.rebuild();

    //     assert_eq!(bt.find(&9), None);
    //     assert_eq!(bt.find(&5), None);
    //     assert_eq!(bt.find(&7), None);
    //     assert_eq!(bt.find(&10), Some(100));
    //     assert_eq!(bt.find(&3), Some(333));
    //     assert_eq!(bt.find(&6), Some(66));
    // }

    #[test]
    fn test_find_range() {
        let mut bt: BTree<i32, i32> = BTree::new(33, true);
        for i in (0..10000).step_by(3) {
            let err = bt.insert(i as i32, 3 * i as i32);
            assert!(err.is_ok());
        }

        let min = 51;
        let max = 300;
        let kvs = bt.find_range(&min, &max);
        assert_eq!(51, kvs.first().unwrap().0);
        assert_eq!(300, kvs.last().unwrap().0);
        assert_eq!(kvs.len(), ((max - min) / 3 + 1) as usize);

        for (k, v) in kvs.into_iter() {
            assert!(k >= 51);
            assert!(300 >= k);
            assert_eq!(v, 3 * k);
        }
    }
}
