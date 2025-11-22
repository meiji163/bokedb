pub mod btree {
    use crate::types::values::Serializable;
    use crate::types::values::SerializeError;

    use std::cmp::Ord;
    use std::collections::VecDeque;
    use std::convert::TryFrom;
    use std::fmt;
    use std::fs::File;
    use std::io::{prelude::*, Seek, SeekFrom};
    use std::marker::Sized;
    use std::mem::size_of;

    /// Key and Val are trait aliases for BTree key-val types.
    pub trait Key: Ord + Clone + Serializable + fmt::Debug
    where
        Self: Sized,
    {
    }
    pub trait Val: Clone + fmt::Debug + Serializable
    where
        Self: Sized,
    {
    }
    impl<T> Key for T where T: Ord + Clone + Serializable + fmt::Debug {}
    impl<T> Val for T where T: Clone + fmt::Debug + Serializable {}

    //
    // ===================== Error Types =====================

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

    // ===================== BTree Pages =====================
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    pub enum PageType {
        Leaf = 0,
        Interior = 1,
    }

    /// Size in bytes of each BTree page.
    pub const PAGE_SIZE: usize = 1024 * 16;

    /// Page is a BTree page, which can hold keys or key-vals
    #[derive(Debug, Clone)]
    pub struct Page<K: Key, V: Val> {
        pub id: u32,
        pub ptype: PageType,
        /// Soft delete info for leaf pages
        pub deleted: Vec<bool>,
        /// Keys for interior and leaf pages
        pub keys: Vec<K>,
        /// Vals corresponding to keys for leaf pages
        pub vals: Vec<V>,
        /// Child page IDs for interior pages
        pub children: Vec<u32>,
        /// Page ID of the right sibling for leaf pages
        pub sibling: Option<u32>,
    }

    /// Pager is an interface for reading and writing pages.
    pub trait Pager<K: Key, V: Val>: fmt::Debug {
        fn read_page(&mut self, id: u32) -> Result<&Page<K, V>, PageNotFoundError>;
        fn write_page(&mut self, page: &Page<K, V>) -> std::io::Result<()>;
        fn commit(&mut self) -> std::io::Result<()>;
    }

    /// MemPager is a simple in-memory page store.
    #[derive(Debug)]
    pub struct MemPager<K: Key, V: Val> {
        pages: Vec<Page<K, V>>,
        wal: Vec<Page<K, V>>,
    }

    impl<K: Key, V: Val> Pager<K, V> for MemPager<K, V> {
        fn read_page(&mut self, id: u32) -> Result<&Page<K, V>, PageNotFoundError> {
            for p in self.wal.iter().rev() {
                if p.id == id {
                    return Ok(p);
                }
            }
            let res = self.pages.binary_search_by_key(&id, |p| p.id);
            match res {
                Ok(idx) => Ok(&self.pages[idx]),
                Err(_) => Err(PageNotFoundError),
            }
        }

        fn write_page(&mut self, page: &Page<K, V>) -> std::io::Result<()> {
            self.wal.push(page.clone());
            Ok(())
        }

        fn commit(&mut self) -> std::io::Result<()> {
            for page in self.wal.iter() {
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
            self.wal.clear();
            Ok(())
        }
    }

    /// FilePager stores pages in a file.
    /// It maintains an index that maps page IDs to the byte offset.
    #[derive(Debug)]
    pub struct FilePager<K: Key, V: Val> {
        /// Pairs of (`id`, `idx`), where `idx` is the index of page where id=`id` in the file.
        p_index: Vec<(u32, u32)>,
        /// The next page index to be assigned.
        next_index: u32,
        /// Global byte offset in the file.
        global_offset: u64,
        pub file: File,
        /// Cache stores pages read from disk.
        cache: Vec<Page<K, V>>,
        wal: Vec<Page<K, V>>,
    }

    impl<K: Key, V: Val> Pager<K, V> for FilePager<K, V> {
        fn read_page(&mut self, id: u32) -> Result<&Page<K, V>, PageNotFoundError> {
            for p in self.wal.iter().rev() {
                if p.id == id {
                    return Ok(p);
                }
            }

            // check cache
            let cached = self.cache.binary_search_by_key(&id, |p| p.id);
            if let Ok(idx) = cached {
                return Ok(&self.cache[idx]);
            }
            let cache_idx = cached.unwrap_err();

            // read from file
            let res = self.p_index.binary_search_by_key(&id, |&(p, _)| p);
            if let Ok(idx) = res {
                let ofs = (PAGE_INDEX_SIZE as u64) * (self.p_index[idx].1 as u64);
                self.file
                    .seek(SeekFrom::Start(ofs + self.global_offset))
                    .unwrap();
                let mut buf = [0x0; PAGE_INDEX_SIZE];
                let _ = self.file.read_exact(&mut buf);
                let page: Page<K, V> = Page::from_bytes(&buf).unwrap().1;

                self.cache.insert(cache_idx, page);
                Ok(&self.cache[cache_idx])
            } else {
                Err(PageNotFoundError)
            }
        }

        fn write_page(&mut self, page: &Page<K, V>) -> std::io::Result<()> {
            self.wal.push(page.clone());
            Ok(())
        }

        fn commit(&mut self) -> std::io::Result<()> {
            for page in self.wal.iter() {
                let id = page.id;
                let res = self.p_index.binary_search_by_key(&id, |&(p, _)| p);
                let buf = page.to_bytes();
                match res {
                    Ok(idx) => {
                        // update page
                        let ofs = (PAGE_INDEX_SIZE as u64) * (self.p_index[idx].1 as u64);
                        self.file.seek(SeekFrom::Start(ofs + self.global_offset))?;
                        self.file.write_all(&buf)?;
                    }
                    Err(idx) => {
                        // create page
                        let ofs = (PAGE_INDEX_SIZE as u64) * (self.next_index as u64);
                        self.file.seek(SeekFrom::Start(ofs + self.global_offset))?;
                        self.file.write_all(&buf)?;
                        self.p_index.insert(idx, (id, self.next_index));
                        self.next_index += 1;
                    }
                }
                // update cache
                match self.cache.binary_search_by_key(&id, |p| p.id) {
                    Ok(i) => {
                        self.cache[i] = page.clone();
                    }
                    Err(i) => {
                        self.cache.insert(i, page.clone());
                    }
                }
            }
            self.wal.clear();
            self.write_index()
        }
    }

    // TODO: the page index should grow as needed.
    // For now the size is constant, able to store (PAGE_INDEX_SIZE/8) - 1 pages.
    pub const PAGE_INDEX_SIZE: usize = 2 * PAGE_SIZE;

    impl<K: Key, V: Val> FilePager<K, V> {
        pub fn read_index(&mut self) -> std::io::Result<Vec<(u32, u32)>> {
            let mut buf = [0; PAGE_INDEX_SIZE];
            self.file.seek(SeekFrom::Start(self.global_offset))?;
            self.file.read_exact(&mut buf).unwrap();
            let mut p_index = vec![];

            let len_bytes: [u8; 4] = buf[0..4].try_into().unwrap();
            let len = u32::from_le_bytes(len_bytes);
            let mut n = 4;
            for _ in 0..len {
                let id_bytes: [u8; 4] = buf[n..n + 4].try_into().unwrap();
                let id = u32::from_le_bytes(id_bytes);
                n += 4;

                let ofs_bytes: [u8; 4] = buf[n..n + 4].try_into().unwrap();
                let ofs = u32::from_le_bytes(ofs_bytes);
                n += 4;
                p_index.push((id, ofs));
            }
            Ok(p_index)
        }

        pub fn write_index(&mut self) -> std::io::Result<()> {
            let mut bytes = [0; PAGE_INDEX_SIZE];
            let len: u32 = self.p_index.len().try_into().unwrap();
            let len_bytes: [u8; 4] = len.to_le_bytes();

            bytes[0..4].copy_from_slice(&len_bytes);
            let mut n = 4;
            for (id, ofs) in self.p_index.iter() {
                let id_bytes: [u8; 4] = id.to_le_bytes();
                bytes[n..n + 4].copy_from_slice(&id_bytes);
                n += 4;

                let ofs_bytes: [u8; 4] = ofs.to_le_bytes();
                bytes[n..n + 4].copy_from_slice(&ofs_bytes);
                n += 4;
            }
            self.file.seek(SeekFrom::Start(self.global_offset))?;
            self.file.write_all(&bytes)
        }

        pub fn new(file: File) -> FilePager<K, V> {
            FilePager {
                next_index: 1,
                p_index: vec![],
                cache: vec![],
                wal: vec![],
                file: file,
                global_offset: 0,
            }
        }
    }

    /// Pack a vector of bits into bytes with padding in litte-endian order
    pub fn pack_bits(bits: &Vec<bool>) -> Vec<u8> {
        let len = (bits.len() + 7) / 8;
        let mut bs = vec![0x0; len];
        for i in 0..len {
            for j in 0..8 {
                if 8 * i + j >= bits.len() {
                    break;
                }
                bs[i] ^= (bits[8 * i + j] as u8) << (7 - j);
            }
        }
        bs
    }

    /// Inverse of `pack_bits`
    pub fn unpack_bits(len: usize, bytes: &[u8]) -> Vec<bool> {
        let mut bits: Vec<bool> = vec![];
        for b in bytes.iter() {
            for j in (0..8).rev() {
                let bit = ((*b >> j) & 1) == 1;
                bits.push(bit);
            }
        }
        bits.truncate(len);
        bits
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

        /// The byte layout of a page is as follows:
        ///
        /// Interior Page
        ///  0-3   4     5-8       9-12      13-16
        /// +----+------+---------+---------+--------+
        /// | id | type | sibling | keysize | keylen |
        /// +----+------+---+-----+---------+--------+
        /// |  keys...  |      children...           |
        /// +-----------+----------------------------+
        ///
        ///
        /// Leaf Page
        ///  0-3   4     5-8       9-12      13-16
        /// +----+------+---------+---------+--------+
        /// | id | type | sibling | keysize | keylen |
        /// +----+------+---+-----+---------+--------+
        /// | keys...   | deleted... | vals...       |
        /// +-----------+------------+---------------+
        pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
            let mut bytes = [0; PAGE_SIZE];
            let id_bytes: [u8; 4] = self.id.to_le_bytes();
            bytes[0..4].copy_from_slice(&id_bytes);
            bytes[4] = self.ptype as u8;

            // sibling
            let sib = self.sibling.unwrap_or(0);
            bytes[5..9].copy_from_slice(&sib.to_le_bytes());

            // keys
            let key_size = u32::try_from(size_of::<K>()).unwrap();
            bytes[9..13].copy_from_slice(&key_size.to_le_bytes());
            let keys_len = u32::try_from(self.keys.len()).unwrap();
            bytes[13..17].copy_from_slice(&keys_len.to_le_bytes());
            let key_usize = key_size as usize;
            let mut i = 17;
            for k in self.keys.iter() {
                bytes[i..(i + key_usize)].copy_from_slice(&k.to_bytes());
                i += key_usize;
            }

            match self.ptype {
                PageType::Interior => {
                    for c in self.children.iter() {
                        bytes[i..(i + 4)].copy_from_slice(&c.to_le_bytes());
                        i += 4;
                    }
                }
                PageType::Leaf => {
                    assert_eq!(self.deleted.len(), self.vals.len());
                    assert_eq!(self.vals.len(), self.keys.len());

                    let del_bytes = pack_bits(&self.deleted);
                    let del_len = del_bytes.len();
                    bytes[i..(i + del_len)].copy_from_slice(&del_bytes);
                    i += del_len;

                    for v in self.vals.iter() {
                        let v_bytes = v.to_bytes();
                        let v_len = v_bytes.len();
                        bytes[i..(i + v_len)].copy_from_slice(&v_bytes);
                        i += v_len;
                    }
                }
            }
            bytes
        }

        pub fn from_bytes(bs: &[u8]) -> Result<(usize, Self), SerializeError> {
            let id_bytes: [u8; 4] = bs[0..4].try_into().unwrap();
            let id = u32::from_le_bytes(id_bytes);

            // type
            let ptype = if bs[4] == PageType::Interior as u8 {
                PageType::Interior
            } else {
                PageType::Leaf
            };

            // sibling
            let sib_bytes: [u8; 4] = bs[5..9].try_into().unwrap();
            let sib_id = u32::from_le_bytes(sib_bytes);
            let sibling = if sib_id == 0 { None } else { Some(sib_id) };

            // keys
            let key_size_bytes: [u8; 4] = bs[9..13].try_into().unwrap();
            let key_size = u32::from_le_bytes(key_size_bytes);
            let keys_len_bytes: [u8; 4] = bs[13..17].try_into().unwrap();
            let keys_len = u32::from_le_bytes(keys_len_bytes);

            let key_usize = key_size as usize;
            let keys_len_usize = keys_len as usize;
            let mut keys = Vec::with_capacity(keys_len_usize);
            let mut i = 17;
            for _ in 0..keys_len_usize {
                let (size, key) = K::from_bytes(&bs[i..(i + key_usize)])?;
                keys.push(key);
                i += size;
            }

            match ptype {
                PageType::Interior => {
                    let mut children = Vec::with_capacity(keys_len_usize);
                    for _ in 0..keys_len_usize {
                        let cbytes: [u8; 4] = bs[i..(i + 4)].try_into().unwrap();
                        let c = u32::from_le_bytes(cbytes);
                        children.push(c);
                        i += 4;
                    }

                    Ok((
                        i,
                        Page {
                            id,
                            ptype,
                            keys,
                            children,
                            sibling,
                            vals: vec![],
                            deleted: vec![],
                        },
                    ))
                }
                PageType::Leaf => {
                    let del_len = (keys_len_usize + 7) / 8;
                    let deleted = unpack_bits(keys_len_usize, &bs[i..(i + del_len)]);
                    i += del_len;

                    let mut vals = Vec::with_capacity(keys_len_usize);
                    for _ in 0..keys_len_usize {
                        let (size, val) = V::from_bytes(&bs[i..])?;
                        vals.push(val);
                        i += size;
                    }

                    Ok((
                        i,
                        Page {
                            id,
                            ptype,
                            keys,
                            deleted,
                            sibling,
                            vals,
                            children: vec![],
                        },
                    ))
                }
            }
        }
    }

    /// ------------------- BTree Implementation -------------------

    /// The ID of the BTree root stays constant.
    pub const ROOT_PAGE_ID: u32 = 0;

    /// BTree implements a B+Tree.
    /// Each page has at most b children, where b is odd.
    #[derive(Debug)]
    pub struct BTree<K: Key, V: Val> {
        b: usize,
        is_unique: bool,
        pub depth: usize,
        next_id: u32,
        pub pager: Box<dyn Pager<K, V>>,
    }

    impl<K: Key + 'static, V: Val + 'static> BTree<K, V> {
        pub fn new_in_mem(b: usize, is_unique: bool) -> BTree<K, V> {
            assert!(b > 2);
            assert_eq!(b % 2, 1);

            let pager = Box::new(MemPager {
                pages: vec![Page {
                    id: 0,
                    keys: vec![],
                    vals: vec![],
                    children: vec![],
                    deleted: vec![],
                    ptype: PageType::Leaf,
                    sibling: None,
                }],
                wal: vec![],
            });

            BTree {
                b,
                is_unique,
                pager,
                depth: 0,
                next_id: 1,
            }
        }

        pub fn new(b: usize, is_unique: bool, pager: Box<dyn Pager<K, V>>) -> BTree<K, V> {
            assert_eq!(b % 2, 1);
            assert!(b > 2);

            BTree {
                b,
                is_unique,
                pager,
                depth: 0,
                next_id: 1,
            }
        }

        pub fn print(&mut self) {
            let vecs = self.traverse();
            for (l, ids) in vecs.iter().enumerate() {
                print!("LEVEL {:?}\n", l);
                for &id in ids.iter() {
                    print!("\t{:?}\n", self.pager.read_page(id).unwrap());
                }
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
        //         let res = self.pager.read_page(id);
        //         if res.is_err() {
        //             continue
        //         }
        //         let mut page = res.unwrap().clone();

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
        //     self.depth = 0;
        //     self.root_id = 0;
        //     self.next_id = 1;
        //     for (k, v) in keys.into_iter().zip(vals.into_iter()) {
        //         let _ = self.insert(k, v);
        //     }
        // }

        // Return the value associated with key, or None if it doesn't exist.
        // If there are multiple values associated with the key, any can be returned.
        pub fn find(&mut self, key: &K) -> Option<V> {
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
        pub fn find_range(&mut self, min: &K, max: &K) -> Vec<(K, V)> {
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

        /// Insert a key-val pair into the tree.
        /// Splits the tree in case insert causes page overflow.
        pub fn insert(&mut self, key: K, val: V) -> Result<(), DuplicateKeyError> {
            let mut id = ROOT_PAGE_ID;
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
                // key is not present, or duplicates are allowed: OK to insert
                page.keys.insert(idx, key.clone());
                page.vals.insert(idx, val);
                page.deleted.insert(idx, false);
            } else {
                // duplicate key found on a unique tree:
                // try to replace a deleted entry, otherwise error
                if page.deleted[idx] {
                    page.vals[idx] = val;
                    page.deleted[idx] = false;
                    let _ = self.pager.write_page(&page);
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
                let _ = self.pager.write_page(&page);
                Ok(())
            } else {
                let mut par_id_opt = visited.pop();
                // try to overflow to sibling first
                if page.sibling.is_some() && par_id_opt.is_some() {
                    let par_id = par_id_opt.unwrap();
                    let sib_id = page.sibling.unwrap();
                    let mut parent = self.pager.read_page(par_id).unwrap().clone();
                    let mut sibling = self.pager.read_page(sib_id).unwrap().clone();

                    if let Ok(()) = self.overflow_to_sibling(&mut page, &mut sibling, &mut parent) {
                        self.pager.write_page(&page).unwrap();
                        self.pager.write_page(&sibling).unwrap();
                        self.pager.write_page(&parent).unwrap();
                        return Ok(());
                    }
                }

                // split page and propagate split upward if necessary
                let max_splits = self.depth + 1;
                for _ in 0..max_splits {
                    match par_id_opt {
                        Some(par_id) => {
                            let mut parent = self.pager.read_page(par_id).unwrap().clone();
                            let sibling = self.split_page(&mut page, &mut parent);
                            // TODO: handle error
                            self.pager.write_page(&page).unwrap();
                            self.pager.write_page(&sibling).unwrap();

                            needs_split = parent.keys.len() >= self.b;
                            if !needs_split {
                                self.pager.write_page(&parent).unwrap();
                                break;
                            } else {
                                // loop
                                page = parent;
                                par_id_opt = visited.pop();
                            }
                        }
                        None => {
                            // split root
                            assert_eq!(ROOT_PAGE_ID, page.id);
                            let (sibling, root) = self.split_root(&mut page);

                            self.pager.write_page(&root).unwrap();
                            self.pager.write_page(&page).unwrap();
                            self.pager.write_page(&sibling).unwrap();
                            break;
                        }
                    }
                }
                Ok(())
            }
        }

        /// Attempt to overflow keys & vals of the leaf page to its right sibling.
        /// Returns Err if overflow not possible.
        fn overflow_to_sibling(
            &mut self,
            page: &mut Page<K, V>,
            sibling: &mut Page<K, V>,
            parent: &mut Page<K, V>,
        ) -> Result<(), &'static str> {
            let p = page.keys.len();
            let s = sibling.keys.len();
            // how many keys to overflow from page?
            let mov = {
                // p > s + 1 ensures we can move at least one key to sibling
                // p + s < b ensures we don't underflow
                if s >= self.b || p <= s + 1 || p + s < self.b {
                    return Err("not attempting overflow");
                }
                (p - s) / 2
            };

            // move data to sibling
            // TODO: insert(0,-) is inefficient
            for k in page.keys.drain((p - 1 - mov)..) {
                sibling.keys.insert(0, k);
            }
            for v in page.vals.drain((p - 1 - mov)..) {
                sibling.vals.insert(0, v);
            }
            for d in page.deleted.drain((p - 1 - mov)..) {
                sibling.deleted.insert(0, d);
            }

            // update parent key
            let max_key = page.keys.last().unwrap().clone();
            let idx = parent.find(&max_key);
            parent.keys[idx] = max_key;

            Ok(())
        }

        /// Splits the given page into two and promotes a key its parent page.
        /// Mutates `page` and `parent` and returns the new right sibling page.
        fn split_page(&mut self, page: &mut Page<K, V>, parent: &mut Page<K, V>) -> Page<K, V> {
            assert!(page.keys.len() >= self.b);
            let split_idx = self.b / 2;
            let split_key = page.keys[split_idx].clone();
            let sibling = self.divide_page(page);
            /*
             *                 `parent`
             *  `page` -->      /    \
             *              `page`  `sibling`
             */

            let idx = parent.find(&split_key);
            parent.keys.insert(idx, split_key);
            parent.children.insert(idx, page.id);
            parent.children[idx + 1] = sibling.id;

            sibling
        }

        /// Splits the root node. Returns (sibling, new root).
        fn split_root(&mut self, root: &mut Page<K, V>) -> (Page<K, V>, Page<K, V>) {
            let split_idx = self.b / 2;
            let split_key = root.keys[split_idx].clone();
            let sibling = self.divide_page(root);
            /*
             *                 `new_root`
             *  `root` -->      /    \
             *              `root`  `sibling`
             */
            root.id = self.next_id as u32;
            self.next_id += 1;

            let new_root: Page<K, V> = Page {
                id: 0,
                keys: vec![split_key],
                children: vec![root.id, sibling.id],
                vals: Vec::new(),
                ptype: PageType::Interior,
                sibling: None,
                deleted: vec![],
            };
            self.depth += 1;
            (sibling, new_root)
        }

        /// Helper function for page splitting: divides upper half of page into
        /// a new (right) sibling page and returns the sibling.
        fn divide_page(&mut self, page: &mut Page<K, V>) -> Page<K, V> {
            let split_idx = self.b / 2;

            // Allocate the right child page.
            // The current page becomes left child page.
            let mut r_page = Page {
                id: self.next_id as u32,
                keys: Vec::with_capacity(split_idx),
                vals: Vec::with_capacity(split_idx),
                deleted: Vec::with_capacity(split_idx),
                children: vec![],
                ptype: page.ptype.clone(),
                sibling: page.sibling,
            };
            self.next_id += 1;
            r_page.keys = page.keys.drain((split_idx + 1)..).collect();
            page.sibling = Some(r_page.id);

            match page.ptype {
                PageType::Leaf => {
                    r_page.vals = page.vals.drain((split_idx + 1)..).collect();
                    r_page.deleted = page.deleted.drain((split_idx + 1)..).collect();
                }
                PageType::Interior => {
                    r_page.children = page.children.drain((split_idx + 1)..).collect();
                }
            }
            r_page
        }

        fn find_leaf(&mut self, key: &K) -> u32 {
            let mut id = ROOT_PAGE_ID;
            for _ in 0..self.depth {
                let page = self.pager.read_page(id).unwrap();
                let idx = page.find(key);
                id = page.children[idx];
            }
            id
        }

        /// Mark entries associatied with key as deleted
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
                    let _ = self.pager.write_page(&leaf);
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

        /// Traverse page IDs in level order
        fn traverse(&mut self) -> Vec<Vec<u32>> {
            let mut lvl = 0;
            let mut ids = vec![vec![ROOT_PAGE_ID]];
            let mut q = VecDeque::from([(0, ROOT_PAGE_ID)]);
            let max_loop = self.next_id;
            for _ in 0..=max_loop {
                match q.pop_front() {
                    Some((l, id)) => {
                        if l >= lvl {
                            lvl += 1;
                            ids.push(vec![]);
                        }
                        let page = self.pager.read_page(id).unwrap();
                        if page.ptype == PageType::Leaf {
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

        pub fn assert_integrity(&mut self) {
            let levels = self.traverse();
            for lvl in levels.iter() {
                for i in 0..(lvl.len() - 1) {
                    let id = lvl[i];
                    let id_next = lvl[i + 1];
                    let page = self.pager.read_page(id).unwrap();
                    assert_eq!(page.sibling, Some(id_next));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::btree::*;
    use crate::types::values::*;
    use rand::rngs::StdRng;
    use rand::Rng;
    use rand::SeedableRng;
    use std::fs::File;

    const RNG_SEED: u64 = 163;

    fn new_test_file_pager() -> FilePager<i32, i32> {
        let file: File = tempfile::tempfile().unwrap();
        let mut pager = FilePager::new(file);
        let root = Page {
            id: 0,
            keys: vec![],
            vals: vec![],
            children: vec![],
            deleted: vec![],
            ptype: PageType::Leaf,
            sibling: None,
        };
        pager.write_page(&root).unwrap();
        pager.commit().unwrap();

        pager
    }

    fn new_test_pages() -> Vec<Page<i32, i32>> {
        vec![
            Page {
                id: 0,
                ptype: PageType::Interior,
                keys: vec![61, 68],
                vals: vec![],
                deleted: vec![],
                sibling: None,
                children: vec![111, 222, 333],
            },
            Page {
                id: 111,
                ptype: PageType::Leaf,
                keys: vec![44, 45, 49, 50, 55, 60],
                vals: vec![45, 46, 50, 51, 56, 61],
                deleted: vec![false; 6],
                sibling: Some(222),
                children: vec![],
            },
            Page {
                id: 222,
                ptype: PageType::Leaf,
                keys: vec![61, 62, 63, 64, 65, 67],
                vals: vec![45, 46, 50, 51, 56, 61],
                deleted: vec![false; 6],
                sibling: Some(333),
                children: vec![],
            },
            Page {
                id: 333,
                ptype: PageType::Leaf,
                keys: vec![69, 70, 70, 71, 72, 74],
                vals: vec![45, 46, 50, 51, 56, 61],
                deleted: vec![false; 6],
                sibling: None,
                children: vec![],
            },
        ]
    }

    fn _insert_no_split(bt: &mut BTree<i32, i32>) {
        let kvs = [(5, 50), (6, 60), (7, -70), (7, 70), (8, 80)];
        for (k, v) in kvs.into_iter() {
            let err = bt.insert(k, v);
            assert!(err.is_ok());
        }
        let res = bt.pager.commit();
        assert!(res.is_ok());
        assert_eq!(bt.find(&5), Some(50));
        assert_eq!(bt.find(&6), Some(60));
        assert_eq!(bt.find(&8), Some(80));
    }
    #[test]
    fn in_mem_insert_no_split() {
        let mut bt: BTree<i32, i32> = BTree::new_in_mem(27, false);
        _insert_no_split(&mut bt);
    }

    #[test]
    fn file_insert_no_split() {
        let pager = Box::new(new_test_file_pager());
        let mut bt = BTree::new(27, false, pager);
        _insert_no_split(&mut bt);
    }

    #[test]
    fn insert_overflow_to_sibling() {
        let pager = Box::new(new_test_file_pager());
        let mut bt: BTree<i32, i32> = BTree::new(5, false, pager);
        for k in 1..=5 {
            let err = bt.insert(k, k * 2);
            assert!(err.is_ok());
        }
        // root is now split at k=3. Next inserts trigger an overflow
        assert_eq!(bt.insert(0, 0), Ok(()));
        assert_eq!(bt.insert(-1, -2), Ok(()));

        let res = bt.pager.commit();
        assert!(res.is_ok());

        let p0 = bt.pager.read_page(0).unwrap();
        assert_eq!(p0.keys, vec![1]);
        let p1 = bt.pager.read_page(1).unwrap();
        assert_eq!(p1.keys, vec![3, 2, 4, 5]);
        let p2 = bt.pager.read_page(2).unwrap();
        assert_eq!(p2.keys, vec![-1, 0, 1]);
        assert_eq!(p2.sibling, Some(1));

        bt.assert_integrity();
    }

    fn _insert_split(bt: &mut BTree<i32, i32>) {
        let kvs = [(5, 55), (6, 66), (7, 77), (8, 88), (9, 99), (10, 100)];
        for (k, v) in kvs.into_iter() {
            let err = bt.insert(k, v);
            assert!(err.is_ok());
        }

        let res = bt.pager.commit();
        assert!(res.is_ok());

        assert_eq!(bt.find(&5), Some(55));
        assert_eq!(bt.find(&6), Some(66));
        assert_eq!(bt.find(&7), Some(77));
        assert_eq!(bt.find(&9), Some(99));
        assert_eq!(bt.find(&8), Some(88));
        assert_eq!(bt.find(&666), None);

        bt.assert_integrity();
    }

    #[test]
    fn in_mem_insert_split() {
        let mut bt: BTree<i32, i32> = BTree::new_in_mem(3, true);
        _insert_split(&mut bt);
    }

    #[test]
    fn file_insert_split() {
        let pager = Box::new(new_test_file_pager());
        let mut bt = BTree::new(3, true, pager);
        _insert_split(&mut bt);
    }

    fn _insert_rand(rng: &mut StdRng, bt: &mut BTree<i32, i32>, n: usize) {
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
        let res = bt.pager.commit();
        assert!(res.is_ok());

        for (k, v) in keys.iter().zip(vals) {
            assert_eq!(bt.find(k), Some(v));
        }
    }

    #[test]
    fn in_mem_insert_rand() {
        let mut rng = StdRng::seed_from_u64(RNG_SEED);
        //let sizes = [21, 101, 179, 213, 303];
        let sizes = [21, 101, 179];
        for size in sizes.into_iter() {
            println!("size={}", size);
            let mut bt: BTree<i32, i32> = BTree::new_in_mem(size, true);
            _insert_rand(&mut rng, &mut bt, 10000);
            bt.print();
            bt.assert_integrity();
        }
    }

    #[test]
    fn file_insert_rand() {
        let mut rng = StdRng::seed_from_u64(RNG_SEED);
        let sizes = [101, 179, 213, 303];
        for size in sizes.into_iter() {
            println!("size={}", size);
            let pager = Box::new(new_test_file_pager());
            let mut bt = BTree::new(size, true, pager);
            _insert_rand(&mut rng, &mut bt, 10000);
        }
    }

    fn _delete_rand(rng: &mut StdRng, bt: &mut BTree<i32, i32>, n: usize) {
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
        let res = bt.pager.commit();
        assert!(res.is_ok());

        for k in keys[n / 2..].iter() {
            let err = bt.delete(k);
            assert!(err.is_ok());
        }
        let res = bt.pager.commit();
        assert!(res.is_ok());
        for (i, k) in keys[0..n / 2].iter().enumerate() {
            assert_eq!(bt.find(k), Some(vals[i]));
        }
    }

    #[test]
    fn in_mem_delete_rand() {
        let mut rng = StdRng::seed_from_u64(RNG_SEED);
        let sizes = [101, 179, 213, 303];
        for size in sizes.into_iter() {
            println!("size={}", size);
            let mut bt: BTree<i32, i32> = BTree::new_in_mem(size, true);
            _delete_rand(&mut rng, &mut bt, 5000);
        }
    }

    #[test]
    fn file_delete_rand() {
        let mut rng = StdRng::seed_from_u64(RNG_SEED);
        let sizes = [101, 179, 213, 303];
        for size in sizes.into_iter() {
            println!("size={}", size);
            let pager = Box::new(new_test_file_pager());
            let mut bt = BTree::new(size, true, pager);
            _delete_rand(&mut rng, &mut bt, 5000);
        }
    }

    fn _delete_one(bt: &mut BTree<i32, i32>) {
        let kvs = [(3, 333), (5, 55), (6, 66), (7, 77), (9, 99), (10, 100)];
        for (k, v) in kvs.into_iter() {
            let err = bt.insert(k, v);
            assert!(err.is_ok());
        }
        let res = bt.pager.commit();
        assert!(res.is_ok());

        assert_eq!(bt.find(&5), Some(55));
        let err = bt.delete(&5);
        assert!(err.is_ok());
        assert_eq!(err.unwrap(), 1 as usize);
        assert_eq!(bt.find(&5), None);
    }

    #[test]
    fn in_mem_delete_one() {
        let mut bt: BTree<i32, i32> = BTree::new_in_mem(3, true);
        _delete_one(&mut bt);
    }

    #[test]
    fn file_delete_one() {
        let pager = Box::new(new_test_file_pager());
        let mut bt = BTree::new(3, true, pager);
        _delete_one(&mut bt);
    }

    fn _find_range(bt: &mut BTree<i32, i32>, n: usize) {
        for i in (0..n).step_by(3) {
            let err = bt.insert(i as i32, 3 * i as i32);
            assert!(err.is_ok());
        }
        let res = bt.pager.commit();
        assert!(res.is_ok());

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

        bt.print();
        bt.assert_integrity();
    }

    #[test]
    fn in_mem_find_range() {
        let mut bt: BTree<i32, i32> = BTree::new_in_mem(23, true);
        _find_range(&mut bt, 50000);
    }

    #[test]
    fn file_find_range() {
        let pager = Box::new(new_test_file_pager());
        let mut bt = BTree::new(101, true, pager);
        _find_range(&mut bt, 10000);
    }

    #[test]
    fn in_mem_duplicate_key() {
        let mut bt: BTree<i32, i32> = BTree::new_in_mem(5, true);

        assert!(bt.insert(6, 66).is_ok());
        assert!(bt.insert(1, 11).is_ok());

        assert!(bt.insert(5, 55).is_ok());
        let res = bt.pager.commit();
        assert!(res.is_ok());

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
    fn serialize_page() {
        let pages = new_test_pages();
        for page in pages.iter() {
            let bytes = page.to_bytes();
            let res: Result<(usize, Page<i32, i32>), SerializeError> = Page::from_bytes(&bytes);
            assert!(res.is_ok());
            let (_, got_page) = res.unwrap();

            assert_eq!(page.id, got_page.id);
            assert_eq!(page.ptype, got_page.ptype);
            assert_eq!(page.keys, got_page.keys);
            assert_eq!(page.vals, got_page.vals);
            assert_eq!(page.deleted, got_page.deleted);
            assert_eq!(page.sibling, got_page.sibling);
        }
    }

    #[test]
    fn file_pager_rw() {
        let mut pager = new_test_file_pager();
        let mut pages = new_test_pages();

        for p in pages.iter() {
            assert!(pager.write_page(p).is_ok());
        }
        let res = pager.commit();
        assert!(res.is_ok());

        let res = pager.read_index();
        assert!(res.is_ok());
        let p_index = res.unwrap();
        assert_eq!(p_index, vec![(0, 1), (111, 2), (222, 3), (333, 4)]);
        // update page
        pages[1].deleted[0] = true;
        assert!(pager.write_page(&pages[1]).is_ok());

        let res = pager.read_page(111);
        assert!(res.is_ok());
        let page_111 = res.unwrap();
        assert_eq!(page_111.id, pages[1].id);
        assert_eq!(page_111.deleted, pages[1].deleted);
    }

    #[test]
    fn test_pack_bits() {
        // 01101001 11000001 10110100 0011
        let bits = vec![
            false, true, true, false, true, false, false, true, true, true, false, false, false,
            false, false, true, true, false, true, true, false, true, false, false, false, false,
            true, true,
        ];
        let len = bits.len();
        let bytes = pack_bits(&bits);

        let unpacked = unpack_bits(len, &bytes[..]);
        for i in 0..bits.len() {
            assert_eq!(bits[i], unpacked[i]);
        }
    }
}
