use super::{extract_args, validate_command, CommandExecutor, HGet, HGetAll, HSet, RESP_OK};
// use crate::{cmd::CommandError, RespArray, RespFrame, RespMap};
use crate::{cmd::CommandError, BulkString, RespArray, RespFrame};

impl CommandExecutor for HGet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(crate::RespNull),
        }
    }
}

// impl CommandExecutor for HGetAll {
//     fn execute(self, backend: &crate::Backend) -> RespFrame {
//         let hmap = backend.hmap.get(&self.key);

//         match hmap {
//             Some(hmap) => {
//                 let mut map = RespMap::new();
//                 for v in hmap.iter() {
//                     let key = v.key().to_owned();
//                     map.insert(key, v.value().clone());
//                 }
//                 map.into()
//             }
//             None => RespArray::new([]).into(),
//         }
//     }
// }

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let hmap = backend.hmap.get(&self.key);

        match hmap {
            Some(hmap) => {
                let mut data = Vec::with_capacity(hmap.len());
                for v in hmap.iter() {
                    let key = v.key().to_owned();
                    data.push((key, v.value().clone()));
                }

                // sort_by is a method provided by Rust's Vec type.
                // It sorts the elements of the vector in place (modifies the vector directly).
                // You provide a closure (a function) to sort_by that defines how two elements should be compared.
                if self.sort {
                    data.sort_by(|a, b| a.0.cmp(&b.0));
                    // No, you cannot replace &b.0 with b.0 in this case because b.0 is not a reference itself, even though b is a reference type.
                    // Why &b.0 is Required
                    // b.0 is also a &String, but .cmp requires a reference to the other parameter.
                    // To pass b.0 as the other parameter, you need to explicitly borrow it with &b.0.
                    // When you write &b.0, you are explicitly borrowing b.0
                    // So, even though b is a reference (&(String, RespFrame)), b.0 is directly a &String, not a &&String.
                }
                let ret = data
                    .into_iter()
                    .flat_map(|(k, v)| vec![BulkString::from(k).into(), v]) // impl From<String> for BulkString
                    .collect::<Vec<RespFrame>>();

                // The flat_map method in Rust is used to transform each element of an iterator into another iterator
                // and then flatten the resulting iterators into a single iterator.
                // It combines the functionality of map (transforming elements) and flatten (flattening nested iterators).

                // How flat_map Works
                // Transform Each Element:
                // For each element in the original iterator, flat_map applies a closure that returns an iterator (or something that can be converted into an iterator).
                // Flatten the Result:
                // The resulting iterators are flattened into a single iterator, so you get a sequence of all the elements from the nested iterators.

                RespArray::new(ret).into()
            }
            None => RespArray::new([]).into(),
        }
    }
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value);
        RESP_OK.clone()
    }
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hget"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field))) => Ok(HGet {
                key: String::from_utf8(key.0)?,
                field: String::from_utf8(field.0)?,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or field".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hgetall"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(HGetAll {
                key: String::from_utf8(key.0)?,
                sort: false,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hset"], 3)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field)), Some(value)) => {
                Ok(HSet {
                    key: String::from_utf8(key.0)?,
                    field: String::from_utf8(field.0)?,
                    value,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "Invalid key, field or value".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::RespDecode;

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_hget_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$4\r\nhget\r\n$3\r\nmap\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: HGet = frame.try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.field, "hello");

        Ok(())
    }

    #[test]
    fn test_hgetall_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$7\r\nhgetall\r\n$3\r\nmap\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: HGetAll = frame.try_into()?;
        assert_eq!(result.key, "map");

        Ok(())
    }

    #[test]
    fn test_hset_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*4\r\n$4\r\nhset\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: HSet = frame.try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.field, "hello");
        assert_eq!(result.value, RespFrame::BulkString(b"world".into()));

        Ok(())
    }

    #[test]
    fn test_hset_hget_hgetall_commands() -> Result<()> {
        let backend = crate::Backend::new();
        let cmd = HSet {
            key: "map".to_string(),
            field: "hello".to_string(),
            value: RespFrame::BulkString(b"world".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = HSet {
            key: "map".to_string(),
            field: "hello1".to_string(),
            value: RespFrame::BulkString(b"world1".into()),
        };
        cmd.execute(&backend);

        let cmd = HGet {
            key: "map".to_string(),
            field: "hello".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RespFrame::BulkString(b"world".into()));

        let cmd = HGetAll {
            key: "map".to_string(),
            sort: true,
        };
        let result = cmd.execute(&backend);
        // let mut expected = RespMap::new();
        // expected.insert("hello".to_string(), RespFrame::BulkString(b"world".into()));
        // expected.insert(
        //     "hello1".to_string(),
        //     RespFrame::BulkString(b"world1".into()),
        // );
        let expected = RespArray::new([
            BulkString::from("hello").into(),
            BulkString::from("world").into(),
            BulkString::from("hello1").into(),
            BulkString::from("world1").into(),
        ]);
        assert_eq!(result, expected.into());
        Ok(())
    }
}
