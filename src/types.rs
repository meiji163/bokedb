pub mod values {
    use std::fmt;
    use thiserror::Error;

    //use std::cmp::{self, Ordering};

    #[derive(Debug, Error, Clone, PartialEq, Eq)]
    pub enum SerializeError {
        #[error("could not deserialize utf8 string")]
        InvalidUtf8(#[from] std::string::FromUtf8Error),
        #[error("invalid byte length")]
        InvalidByteLen,
    }

    pub trait Serializable {
        fn to_bytes(&self) -> Vec<u8>;
        fn from_bytes(bs: &[u8]) -> Result<(usize, Self), SerializeError>
        where
            Self: Sized;
        // the size in bytes when serialized
        fn size(&self) -> usize;
    }

    // Type provides the type information for columns.
    #[derive(Debug, PartialEq, Eq, Clone)]
    pub enum Type {
        Int = 0,
        VarChar = 1,
        DateTime = 2,
    }

    // Value type wraps the primitive storage structs.
    #[derive(Debug, Eq, PartialEq, Clone)]
    pub enum Value {
        Int(i32),
        VarChar(VarChar),
        DateTime(DateTime),
    }

    #[derive(Debug, Eq, PartialEq, Clone)]
    pub struct VarChar {
        pub val: String,
        max_len: u32,
    }

    #[derive(Debug, Eq, PartialEq, Clone)]
    pub struct DateTime {
        pub year: u32,
        pub month: u32,
        pub day: u32,
        pub hour: u32,
        pub minute: u32,
        pub second: u32,
    }

    impl fmt::Display for DateTime {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "{}-{}-{} {}:{}:{}",
                self.year, self.month, self.day, self.hour, self.minute, self.second
            )
        }
    }

    impl Value {
        fn vtype(&self) -> Type {
            match self {
                Value::Int(_) => Type::Int,
                Value::DateTime(_) => Type::DateTime,
                Value::VarChar(_) => Type::VarChar,
            }
        }
    }

    pub const VARCHAR_MAX_LEN: u32 = 8192;
    impl VarChar {
        pub fn new(s: &str) -> Self {
            VarChar {
                val: s.to_string(),
                max_len: VARCHAR_MAX_LEN,
            }
        }
    }

    impl Serializable for i32 {
        fn to_bytes(&self) -> Vec<u8> {
            self.to_le_bytes().to_vec()
        }
        fn from_bytes(bs: &[u8]) -> Result<(usize, Self), SerializeError> {
            if bs.len() < 4 {
                Err(SerializeError::InvalidByteLen)
            } else {
                let int_bytes: [u8; 4] = bs[..4].try_into().unwrap();
                let val = i32::from_le_bytes(int_bytes);
                Ok((4, val))
            }
        }
        fn size(&self) -> usize {
            4
        }
    }

    impl Serializable for VarChar {
        fn to_bytes(&self) -> Vec<u8> {
            let l = u32::try_from(self.val.len()).unwrap();
            let mut bs = l.to_le_bytes().to_vec();
            bs.extend(self.val.clone().into_bytes());
            bs
        }
        fn from_bytes(bs: &[u8]) -> Result<(usize, Self), SerializeError> {
            let len_bytes: [u8; 4] = bs[0..4].try_into().unwrap();
            let len = u32::from_le_bytes(len_bytes);
            let size = 4 + (len as usize);
            let val = String::from_utf8(bs[4..size].to_vec())?;
            Ok((
                size,
                VarChar {
                    val,
                    max_len: VARCHAR_MAX_LEN,
                },
            ))
        }
        fn size(&self) -> usize {
            self.val.len() + 4
        }
    }

    impl Serializable for DateTime {
        fn to_bytes(&self) -> Vec<u8> {
            let date_enc = 10000 * self.year + 100 * self.month + self.day;
            let time_enc = self.hour * 10000 + self.minute * 100 + self.second;
            let mut bs = u32::try_from(date_enc).unwrap().to_le_bytes().to_vec();
            bs.extend(u32::try_from(time_enc).unwrap().to_le_bytes());
            bs
        }
        fn from_bytes(bs: &[u8]) -> Result<(usize, Self), SerializeError> {
            if bs.len() < 8 {
                Err(SerializeError::InvalidByteLen)
            } else {
                let (year, month, day) = {
                    let bytes: [u8; 4] = bs[0..4].try_into().unwrap();
                    let enc = u32::from_le_bytes(bytes);
                    let year = enc / 10000;
                    let rem = enc % 10000;
                    (year, rem / 100, rem % 100)
                };
                let (hour, minute, second) = {
                    let bytes: [u8; 4] = bs[4..8].try_into().unwrap();
                    let enc = u32::from_le_bytes(bytes);
                    let hour = enc / 10000;
                    let rem = enc % 10000;
                    (hour, rem / 100, rem % 100)
                };
                Ok((
                    8,
                    DateTime {
                        year,
                        month,
                        day,
                        hour,
                        minute,
                        second,
                    },
                ))
            }
        }
        fn size(&self) -> usize {
            8
        }
    }

    impl From<usize> for Type {
        fn from(value: usize) -> Self {
            match value {
                _ if value == Type::Int as usize => Type::Int,
                _ if value == Type::VarChar as usize => Type::VarChar,
                _ if value == Type::DateTime as usize => Type::DateTime,
                _ => {
                    panic!("invalid type")
                }
            }
        }
    }

    // have to dispatch the enum type... annoying
    impl Serializable for Value {
        fn to_bytes(&self) -> Vec<u8> {
            let type_id = self.vtype() as usize;
            let mut v = match self {
                Value::Int(n) => n.to_bytes(),
                Value::DateTime(dt) => dt.to_bytes(),
                Value::VarChar(vc) => vc.to_bytes(),
            };
            v.insert(0, type_id as u8);
            v
        }
        fn from_bytes(bs: &[u8]) -> Result<(usize, Self), SerializeError> {
            let type_id = bs[0] as usize;
            let vtype = Type::try_from(type_id).unwrap();
            let (size, val) = match vtype {
                Type::Int => {
                    let (size, n) = i32::from_bytes(&bs[1..])?;
                    (size, Value::Int(n))
                }
                Type::VarChar => {
                    let (size, vc) = VarChar::from_bytes(&bs[1..])?;
                    (size, Value::VarChar(vc))
                }
                Type::DateTime => {
                    let (size, dt) = DateTime::from_bytes(&bs[1..])?;
                    (size, Value::DateTime(dt))
                }
            };
            Ok((size + 1, val))
        }
        fn size(&self) -> usize {
            match self {
                Value::Int(n) => n.size(),
                Value::VarChar(vc) => vc.size(),
                Value::DateTime(dt) => dt.size(),
            }
        }
    }

    impl Serializable for Vec<Value> {
        fn to_bytes(&self) -> Vec<u8> {
            let len = u32::try_from(self.len()).unwrap();
            let mut bs = len.to_le_bytes().to_vec();
            for v in self.iter() {
                bs.extend(v.to_bytes());
            }
            bs
        }
        fn from_bytes(bs: &[u8]) -> Result<(usize, Self), SerializeError> {
            let len_bytes: [u8; 4] = bs[0..4].try_into().unwrap();
            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut vs = Vec::with_capacity(len);
            let mut i = 4;
            for _ in 0..len {
                let (size, val) = Value::from_bytes(&bs[i..])?;
                vs.push(val);
                i += size;
            }
            Ok((i, vs))
        }
        fn size(&self) -> usize {
            self.to_bytes().len()
        }
    }

    impl fmt::Display for Value {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Value::Int(n) => n.fmt(f),
                Value::VarChar(vc) => vc.val.fmt(f),
                Value::DateTime(dt) => dt.fmt(f),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::values::*;
    #[test]
    fn test_serialize_row() {
        let row = vec![
            Value::Int(163),
            Value::VarChar(VarChar::new("季文子三思而後行。子聞之、曰。再、斯可矣。")),
            Value::DateTime(DateTime {
                year: 2024,
                month: 8,
                day: 13,
                hour: 21,
                minute: 6,
                second: 0,
            }),
        ];
        let bytes = row.to_bytes();
        let deser = Vec::from_bytes(&bytes);
        assert!(deser.is_ok());
        let (_, got_row) = deser.unwrap();
        assert_eq!(got_row.len(), 3);
        assert_eq!(row[0], got_row[0]);
        assert_eq!(row[1], got_row[1]);
        assert_eq!(row[2], got_row[2]);
    }

    // #[test]
    // fn test_int_serialize() {
    //     let int1 = Value::Int(2345087);
    //     let mut int2 = Value::Int(0);
    //     let b = int1.to_bin();
    //     let err = int2.from_bin(&b);
    //     assert!(err.is_ok());
    //     assert_eq!(int1, int2)
    // }
    // #[test]
    // fn test_varchar_serialize() {
    //     let val = "ラウトは難しいです！".to_string();
    //     let len = val.len() as u8;
    //     let vc1 = Value::VarChar(VarChar { val, len });
    //     let mut vc2 = Value::VarChar(VarChar {
    //         val: String::new(),
    //         len: 0,
    //     });
    //     let b = vc1.to_bin();
    //     let err = vc2.from_bin(&b);
    //     assert!(err.is_ok());
    //     assert_eq!(vc1, vc2);
    // }
    // #[test]
    // fn test_row_serialize() {
    //     let row1 = vec![
    //         new_varchar("abcdefg"),
    //         Value::Int(435098),
    //         new_varchar("5-20: 季文子三思而後行。子聞之、曰。再、斯可矣。"),
    //         Value::Int(230956989),
    //     ];
    //     let mut row2 = vec![
    //         new_varchar(""),
    //         Value::Int(0),
    //         new_varchar(""),
    //         Value::Int(0),
    //     ];
    //     let b = row_to_bin(&row1);
    //     let err = bin_to_row(&mut row2, &b);
    //     assert!(err.is_ok());
    //     for i in 0..row1.len() {
    //         assert_eq!(row1[i], row2[i]);
    //     }
    // }
}
