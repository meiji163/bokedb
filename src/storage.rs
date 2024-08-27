pub mod btree {
    use std::cmp::Ord;
    use std::collections::VecDeque;
    use std::fmt;
    use std::marker::Sized;

    // const PAGE_SIZE: usize = 16384;
    // B = floor( (PAGE_SIZE - header_size)/max_row_size )

    // Key and Val are trait aliases
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

    // TODO: use enum for leaf page and interior page
    #[derive(Debug)]
    pub struct Page<K: Key, V: Val> {
        id: u32,
        keys: Vec<K>,
        vals: Vec<V>,
        children: Vec<u32>, // for interior nodes, children.len() == keys.len() + 1
        is_leaf: bool,
    }

    // BTree implements an in-memory B+Tree.
    // Each page has at most b children, where b is odd.
    #[derive(Debug)]
    pub struct BTree<K: Key, V: Val> {
        b: usize,
        depth: usize,
        root_id: u32,
        next_id: u32,
        pages: Vec<Page<K, V>>,
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
        // Return the index
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
                is_leaf: true,
            }];
            BTree {
                b: b,
                pages: pages,
                depth: 0,
                root_id: 0,
                next_id: 1,
            }
        }

        // Return the value associated with key, or None if it doesn't exist.
        // If there are multiple values associated with the key, any can be returned.
        pub fn find(&self, key: &K) -> Option<V> {
            let mut id = self.root_id;
            for _ in 0..self.depth {
                let page = &self.pages[id as usize];
                let idx = page.find(key);
                id = page.children[idx];
            }
            let leaf = &self.pages[id as usize];
            match leaf.keys.binary_search(key) {
                Ok(idx) => Some(leaf.vals[idx].clone()),
                Err(_) => None,
            }
        }

        pub fn insert(&mut self, key: K, val: V) {
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
                        // par is not None here since
                        // a new root page is constructed in that case.
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
                children: vec![],
                is_leaf: page.is_leaf.clone(),
            };
            self.next_id += 1;
            let r_page_id = r_page.id.clone();
            r_page.keys = page.keys.drain((split_idx + 1)..).collect();

            if page.is_leaf {
                r_page.vals = page.vals.drain((split_idx + 1)..).collect();
            } else {
                r_page.children = page.children.drain((split_idx + 1)..).collect();
            }
            self.pages.push(r_page);

            match parent_id {
                Some(id) => {
                    let parent_page = &mut self.pages[id as usize];
                    let idx = parent_page.find(&split_key);

                    parent_page.keys.insert(idx, split_key);
                    parent_page.children.insert(idx, page_id.clone());
                    parent_page.children[idx + 1] = r_page_id.clone();

                    parent_page.keys.len() >= self.b
                }
                None => {
                    // current page was the root page; create a new root
                    let new_root: Page<K, V> = Page {
                        id: self.next_id as u32,
                        keys: vec![split_key],
                        children: vec![page_id.clone(), r_page_id.clone()],
                        vals: Vec::new(),
                        is_leaf: false,
                    };
                    self.next_id += 1;
                    self.root_id = new_root.id;
                    self.depth += 1;
                    self.pages.push(new_root);

                    false
                }
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
    fn test_large_rand() {
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
}
