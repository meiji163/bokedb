pub mod btree {
    use std::cmp::Ord;
    use std::collections::VecDeque;
    use std::fmt;
    use std::io;
    use std::marker::Sized;

    // const PAGE_SIZE: usize = 16384;
    // B = floor( (PAGE_SIZE - header_size)/max_row_size )

    // Key and Val are trait aliases
    pub trait Key: Ord + Clone + Copy + fmt::Debug
    where
        Self: Sized,
    {
    }
    pub trait Val: Clone + Copy + fmt::Debug
    where
        Self: Sized,
    {
    }
    impl<T> Key for T where T: Ord + Clone + Copy + fmt::Debug {}
    impl<T> Val for T where T: Clone + Copy + fmt::Debug {}

    // TODO: use enum for leaf page and interior page
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

    // BTree implements an in-memory B+Tree.
    // Each page has at most b children, where b is odd.
    #[derive(Debug, Clone)]
    pub struct BTree<K: Key, V: Val> {
        b: usize,
        depth: usize,
        root_id: u32,
        next_id: u32,
        pages: Vec<Page<K, V>>,
        n_deleted: usize,
        n_entries: usize,
    }

    impl<K: Key, V: Val> fmt::Display for BTree<K, V> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let vecs = self.traverse();
            for (l, ids) in vecs.iter().enumerate() {
                f.write_fmt(format_args!("LEVEL {:?}\n", l))?;

                for &id in ids.iter() {
                    f.write_fmt(format_args!("\t{:?}\n", self.pages[id as usize]))?;
                }
            }
            Ok(())
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

    impl<K: Key, V: Val> BTree<K, V> {
        pub fn new(b: usize) -> BTree<K, V> {
            assert_eq!(b % 2, 1);
            assert!(b > 2);
            let pages = vec![Page {
                id: 0,
                keys: vec![],
                vals: vec![],
                children: vec![],
                deleted: vec![],
                is_leaf: true,
                sibling: None,
            }];
            BTree {
                b: b,
                pages: pages,
                depth: 0,
                root_id: 0,
                next_id: 1,
                n_deleted: 0,
                n_entries: 0,
            }
        }

        pub fn from_sorted(&mut self, vals: &Vec<V>) {
            // TODO: efficient bulk loading
        }

        // Rebuild the tree to remove soft deleted keys.
        pub fn rebuild(&mut self) {
            // find min
            let mut id = self.root_id;
            for _ in 0..self.depth {
                let page = &self.pages[id as usize];
                id = page.children[0];
            }
            let mut keys: Vec<K> = Vec::with_capacity(self.n_entries);
            let mut vals: Vec<V> = Vec::with_capacity(self.n_entries);
            let max_pages = self.pages.len();
            for _ in 0..max_pages {
                let page = &mut self.pages[id as usize];

                // copy keys and vals that aren't marked deleted
                let mut del = page.deleted.iter();
                let mut keep_keys: Vec<K> = page.keys.drain(0..).collect();
                keep_keys.retain(|_| !*del.next().unwrap());
                keys.extend(keep_keys.into_iter());

                del = page.deleted.iter();
                let mut keep_vals: Vec<V> = page.vals.drain(0..).collect();
                keep_vals.retain(|_| !*del.next().unwrap());
                vals.extend(keep_vals.into_iter());

                match page.sibling {
                    Some(sid) => {
                        id = sid;
                    }
                    None => {
                        break;
                    }
                }
            }

            // reset tree
            self.pages = Self::new(self.b).pages;
            self.depth = 0;
            self.root_id = 0;
            self.next_id = 1;
            self.n_deleted = 0;
            self.n_entries = 0;
            for (k, v) in keys.into_iter().zip(vals.into_iter()) {
                self.insert(k, v);
            }
        }

        // Return the value associated with key, or None if it doesn't exist.
        // If there are multiple values associated with the key, any can be returned.
        pub fn find(&self, key: &K) -> Option<V> where {
            let id = self.find_leaf(key);
            let leaf = &self.pages[id as usize];
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

        pub fn insert(&mut self, key: K, val: V) {
            self.n_entries += 1;
            let mut id = self.root_id;
            let mut visited = vec![];
            for _ in 0..self.depth {
                visited.push(id.clone());
                let page = &self.pages[id as usize];
                let idx = page.find(&key);
                id = page.children[idx];
            }

            let mut needs_split = false;
            // insert key-val in the leaf page
            {
                let leaf = &mut self.pages[id as usize];

                // duplicate keys are allowed
                let idx = leaf.keys.binary_search(&key).unwrap_or_else(|x| x);
                leaf.keys.insert(idx, key);
                leaf.vals.insert(idx, val);
                leaf.deleted.insert(idx, false);
                if leaf.keys.len() >= self.b {
                    needs_split = true;
                }
            }

            // propagate the split if necessary
            if needs_split {
                let max_splits = self.depth.clone() + 1;
                for _ in 0..max_splits {
                    let par = visited.pop();
                    let split_more = self.split_page(id, par);
                    if !split_more {
                        break;
                    } else {
                        // par != None here since a new root page
                        // is constructed when par == None.
                        id = par.unwrap();
                    }
                }
            }
        }

        // Split the given page by promoting a key to the next level of the tree,
        // The key is promoted to the parent page if it exists, or a new root page otherwise.
        // Returns true if a further split of the parent page is necessary.
        fn split_page(&mut self, page_id: u32, parent_id: Option<u32>) -> bool {
            let page = &mut self.pages[page_id as usize];
            if page.keys.len() < self.b {
                return false;
            }
            let split_idx = self.b / 2;
            let split_key = page.keys[split_idx].clone();

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
            let r_page_id = r_page.id.clone();
            r_page.keys = page.keys.drain((split_idx + 1)..).collect();

            if page.is_leaf {
                r_page.vals = page.vals.drain((split_idx + 1)..).collect();
                r_page.deleted = page.deleted.drain((split_idx + 1)..).collect();
                page.sibling = Some(r_page_id);
            } else {
                r_page.children = page.children.drain((split_idx + 1)..).collect();
            }
            self.pages.push(r_page);

            // insert left and right as parent's children
            match parent_id {
                Some(id) => {
                    let parent_page = &mut self.pages[id as usize];
                    let idx = parent_page.find(&split_key);
                    parent_page.keys.insert(idx, split_key);
                    parent_page.children.insert(idx, page_id);
                    parent_page.children[idx + 1] = r_page_id;

                    parent_page.keys.len() >= self.b
                }
                None => {
                    // current page was the root page; create a new root
                    let new_root: Page<K, V> = Page {
                        id: self.next_id as u32,
                        keys: vec![split_key],
                        children: vec![page_id, r_page_id],
                        vals: Vec::new(),
                        is_leaf: false,
                        sibling: None,
                        deleted: vec![],
                    };
                    self.next_id += 1;
                    self.root_id = new_root.id;
                    self.depth += 1;
                    self.pages.push(new_root);

                    false
                }
            }
        }

        fn find_leaf(&self, key: &K) -> u32 {
            let mut id = self.root_id;
            for _ in 0..self.depth {
                let page = &self.pages[id as usize];
                let idx = page.find(key);
                id = page.children[idx];
            }
            id
        }

        // delete marks entries associatied with key as deleted
        pub fn delete(&mut self, key: &K) -> io::Result<usize> {
            let mut id = self.find_leaf(key);
            let mut n_deleted = 0;

            'outer: loop {
                let leaf = &mut self.pages[id as usize];
                let idx = leaf.find(key);
                for i in idx..leaf.deleted.len() {
                    if leaf.keys[i] != *key {
                        break 'outer;
                    }
                    leaf.deleted[i] = true;
                    n_deleted += 1;
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

            self.n_deleted += n_deleted;
            if n_deleted > 0 {
                Ok(n_deleted)
            } else {
                let err =
                    io::Error::new(io::ErrorKind::NotFound, format!("key {:?} not found", key));
                Err(err)
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
                        let page = &self.pages[id as usize];
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
        let mut bt: BTree<i32, i32> = BTree::new(27);
        bt.insert(5, 50);
        bt.insert(6, 60);
        bt.insert(7, -70);
        bt.insert(7, 70);
        bt.insert(8, 80);
        assert_eq!(bt.find(&5), Some(50));
        assert_eq!(bt.find(&6), Some(60));
        assert_eq!(bt.find(&8), Some(80));
    }

    #[test]
    fn test_insert_split() {
        let mut bt: BTree<i32, i32> = BTree::new(3);
        bt.insert(5, 55);
        bt.insert(6, 66);
        bt.insert(7, 77);
        bt.insert(9, 99);
        bt.insert(10, 100);
        bt.insert(8, 88);
        println!("{}", bt);

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
        let mut bt: BTree<i32, i32> = BTree::new(133);

        let mut keys: Vec<i32> = (0..10000)
            .map(|_| {
                let k = rng.gen::<i32>();
                return k;
            })
            .collect();
        keys.dedup();
        let mut vals = vec![];
        for &k in keys.iter() {
            let val: i32 = rng.gen();
            vals.push(val);
            bt.insert(k, val);
        }

        for (k, v) in keys.iter().zip(vals) {
            assert_eq!(bt.find(k), Some(v));
        }
    }

    #[test]
    fn test_delete_one() {
        let mut bt: BTree<i32, i32> = BTree::new(3);
        bt.insert(5, 55);
        bt.insert(6, 66);
        bt.insert(7, 77);
        bt.insert(9, 99);
        bt.insert(10, 100);
        bt.insert(3, 333);

        assert_eq!(bt.find(&5), Some(55));
        let err = bt.delete(&5);
        assert!(err.is_ok());
        assert_eq!(err.unwrap(), 1 as usize);
        assert_eq!(bt.find(&5), None);
    }

    #[test]
    fn test_rebuild() {
        let mut bt: BTree<i32, i32> = BTree::new(3);
        bt.insert(5, 55);
        bt.insert(6, 66);
        bt.insert(7, 77);
        bt.insert(9, 99);
        bt.insert(9, 999);
        bt.insert(9, 9999);
        bt.insert(10, 100);
        bt.insert(3, 333);

        assert!(bt.delete(&5).is_ok());
        assert!(bt.delete(&9).is_ok());
        assert!(bt.delete(&7).is_ok());
        println!("{}", bt);

        bt.rebuild();

        assert_eq!(bt.find(&9), None);
        assert_eq!(bt.find(&5), None);
        assert_eq!(bt.find(&7), None);
        assert_eq!(bt.find(&10), Some(100));
        assert_eq!(bt.find(&3), Some(333));
        assert_eq!(bt.find(&6), Some(66));
    }
}
