pub mod values {
    use anyhow;
    use std::fmt;
    //use std::cmp::{self, Ordering};
    //const MAX_COLUMNS: usize = 255;

    #[derive(Debug, Eq, PartialEq, Clone)]
    pub enum Value {
        Int(i32),
        VarChar(VarChar),
    }

    #[derive(Debug, PartialEq, Eq, Default, Clone)]
    pub struct VarChar {
        pub val: String,
        pub len: u8, // max bytes = 255
    }

    pub fn new_varchar(s: &str) -> Value {
        let len = s.len();
        Value::VarChar(VarChar {
            val: s.to_string(),
            len: len as u8,
        })
    }

    impl Value {
        pub fn to_bin(&self) -> Vec<u8> {
            match &self {
                Value::Int(n) => n.to_le_bytes().to_vec(),
                Value::VarChar(vc) => {
                    let size = (vc.len as usize) + 1;
                    let mut bytes = vec![0; size];
                    bytes[0] = vc.len;
                    bytes.splice(1..size, vc.val.clone().into_bytes());
                    bytes
                }
            }
        }
        // Attempt to read Value from binary format and store in self.
        // Returns number of bytes read if successful.
        pub fn from_bin(&mut self, b: &[u8]) -> anyhow::Result<usize> {
            match self {
                Value::Int(ref mut n) => {
                    let int_bytes: [u8; 4] = b[..4].try_into()?;
                    let val = i32::from_le_bytes(int_bytes);
                    *n = val;
                    Ok(4)
                }
                Value::VarChar(ref mut vc) => {
                    let len: u8 = b[0];
                    vc.len = len;
                    let val_rng = 1..(len as usize) + 1;
                    let val = String::from_utf8(b[val_rng].to_vec())?;
                    vc.val = val;
                    Ok((len + 1) as usize)
                }
            }
        }
    }

    impl fmt::Display for Value {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Value::Int(n) => n.fmt(f),
                Value::VarChar(vc) => vc.val.fmt(f),
            }
        }
    }

    pub type Row = Vec<Value>;

    pub fn row_to_bin(row: &Row) -> Vec<u8> {
        row.iter().flat_map(|v| v.to_bin()).collect()
    }

    pub fn bin_to_row(row: &mut Row, b: &[u8]) -> anyhow::Result<usize> {
        let mut idx = 0;
        for v in row.iter_mut() {
            let n_read = v.from_bin(&b[idx..])?;
            idx += n_read;
        }
        Ok(idx)
    }

    // pub struct TableMeta {
    //     col_names: Vec<String>,
    //     row_meta: Row, // reference row
    // }
}

#[cfg(test)]
mod tests {
    use super::values::*;
    #[test]
    fn test_int_serialize() {
        let int1 = Value::Int(2345087);
        let mut int2 = Value::Int(0);
        let b = int1.to_bin();
        let err = int2.from_bin(&b);
        assert!(err.is_ok());
        assert_eq!(int1, int2)
    }
    #[test]
    fn test_varchar_serialize() {
        let val = "ラウトは難しいです！".to_string();
        let len = val.len() as u8;
        let vc1 = Value::VarChar(VarChar { val, len });
        let mut vc2 = Value::VarChar(VarChar {
            val: String::new(),
            len: 0,
        });
        let b = vc1.to_bin();
        let err = vc2.from_bin(&b);
        assert!(err.is_ok());
        assert_eq!(vc1, vc2);
    }
    #[test]
    fn test_row_serialize() {
        let row1 = vec![
            new_varchar("abcdefg"),
            Value::Int(435098),
            new_varchar("5-20: 季文子三思而後行。子聞之、曰。再、斯可矣。"),
            Value::Int(230956989),
        ];
        let mut row2 = vec![
            new_varchar(""),
            Value::Int(0),
            new_varchar(""),
            Value::Int(0),
        ];
        let b = row_to_bin(&row1);
        let err = bin_to_row(&mut row2, &b);
        assert!(err.is_ok());
        for i in 0..row1.len() {
            assert_eq!(row1[i], row2[i]);
        }
    }
}
