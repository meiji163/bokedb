pub mod sql {
    use crate::storage::btree;
    use crate::types::values::*;
    use lazy_static::lazy_static;
    use regex::Regex;

    #[derive(Debug, Clone)]
    pub enum Statement<K: btree::Key, V: btree::Val> {
        SelectOne(K),
        SelectAll,
        Delete(K),
        Insert((K, V)),
    }

    // HARDCODED TABLE
    // id:       int
    // username: varchar(32)
    // email:    varchar(255)

    lazy_static! {
        static ref INSERT_RE: Regex = Regex::new(r"^insert\s+(-?\d+)\s+'(.*)'\s+'(.*)'$").unwrap();
        static ref SELECT_RE: Regex = Regex::new(r"^select\s+(-?\d+|\*)$").unwrap();
        static ref DELETE_RE: Regex = Regex::new(r"^delete\s+(-?\d+)$").unwrap();
    }

    pub fn parse_statement(s: &str) -> Option<Statement<i32, [Value; 2]>> {
        let mut itr = s.split_whitespace();
        let cmd = itr.next()?.to_lowercase();
        match cmd.as_str() {
            "insert" => {
                let cap = INSERT_RE.captures(s)?;
                let id = cap.get(1)?.as_str().parse::<i32>().unwrap();
                let vals = [
                    new_varchar(cap.get(2)?.as_str()),
                    new_varchar(cap.get(3)?.as_str()),
                ];
                Some(Statement::Insert((id, vals)))
            }
            "select" => {
                let cap = SELECT_RE.captures(s)?;
                let id_str = cap.get(1)?.as_str();
                if id_str == "*" {
                    Some(Statement::SelectAll)
                } else {
                    let id = id_str.parse::<i32>().unwrap();
                    Some(Statement::SelectOne(id))
                }
            }
            "delete" => {
                let cap = DELETE_RE.captures(s)?;
                let id = cap.get(1)?.as_str().parse::<i32>().unwrap();
                Some(Statement::Delete(id))
            }
            _ => None,
        }
    }
}
