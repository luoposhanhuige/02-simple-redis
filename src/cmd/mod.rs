// This file defines the command handling system for your Redis-like application.
// It includes the logic for parsing, validating, and executing commands,
// as well as the data structures and traits required to represent commands.

mod hmap;
mod map;

use crate::{Backend, RespArray, RespError, RespFrame, SimpleString};
use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;
use thiserror::Error;

// lazy_static! Macro:

// The lazy_static! macro is provided by the lazy_static crate.
// It allows you to define static variables that are initialized lazily (i.e., only when they are accessed for the first time).
// This is useful for initializing complex or non-const values that cannot be determined at compile time.
lazy_static! {
    static ref RESP_OK: RespFrame = SimpleString::new("OK").into();
}

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("{0}")]
    RespError(#[from] RespError),
    #[error("Utf8 error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

// Defines a common interface for all commands, requiring an execute method that takes a Backend and returns a RespFrame.
#[enum_dispatch]
pub trait CommandExecutor {
    fn execute(self, backend: &Backend) -> RespFrame;
}

// Represents all possible commands (Get, Set, HGet, etc.).
// Uses enum_dispatch to automatically dispatch calls to the appropriate CommandExecutor implementation.
#[enum_dispatch(CommandExecutor)]
pub enum Command {
    Get(Get),
    Set(Set),
    HGet(HGet),
    HSet(HSet),
    HGetAll(HGetAll),
}

// Each struct is designed to encapsulate the semantics of a specific Redis command.
// For example:
// Get represents a read operation, so it only needs a key.
// Set represents a write operation, so it needs both a key and a value.
// HGet and HSet operate on hash maps, so they need additional fields (field and value for HSet).

// Alignment with RESP Protocol:
// The structs align with the Redis Serialization Protocol (RESP), which defines how commands and their arguments are serialized and deserialized.
// For example:
// The GET command is serialized as *2\r\n$3\r\nget\r\n$5\r\nhello\r\n, which corresponds to the Get struct with a single key.
// The SET command is serialized as *3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n, which corresponds to the Set struct with a key and a value.

// Redis Hash Map Concepts
// In Redis, a hash is a mapping between fields and values, stored under a key.

// key => {
//     field1 => value1,
//     field2 => value2,
//     ...
// }

// Definitions:
// Key: The main identifier of the entire hash (i.e., the name of the hash).
// Field: A sub-key within the hash.
// Value: The value stored for a specific field.

// HSET user:1001 name "Alice"
// HSET user:1001 age "30"

// "user:1001": {
//     "name": "Alice",
//     "age": "30"
// }

// HGET user:1001 name
// "Alice"

// HGETALL user:1001
// 1) "name"
// 2) "Alice"
// 3) "age"
// 4) "30"

// In a serialized form (e.g., RESP – Redis Serialization Protocol), it might look like:
// *4$4name$5Alice$3age$230

// use std::collections::HashMap;

// type Field = String;
// type Value = String;
// type Hash = HashMap<Field, Value>;

// let mut store: HashMap<String, Hash> = HashMap::new();

// Purpose:
// Each struct represents a specific Redis command.
// Encapsulates the arguments required for that command.
// Examples:
// Get: Requires only a key.
// Set: Requires a key and a value.
// HGet and HSet: Operate on hash maps, requiring a key, field, and (for HSet) a value.
// Get only needs a key because the GET command retrieves the value of a single key.
#[derive(Debug)]
pub struct Get {
    key: String, // Only the key is needed for the GET command
}
// Set requires both a key and a value because the SET command stores a value for a given key.
#[derive(Debug)]
pub struct Set {
    key: String,      // The key to set
    value: RespFrame, // The value to associate with the key
}
// HGet and HSet operate on hash maps, so they require both a key (the hash map's name) and a field (the specific field within the hash map). HSet also requires a value to store in the field.
#[derive(Debug)]
pub struct HGet {
    key: String,   // The hash map's name
    field: String, // The specific field to retrieve
}
// HSet also requires a value to store in the field.
#[derive(Debug)]
pub struct HSet {
    key: String,      // The hash map's name
    field: String,    // The specific field to set
    value: RespFrame, // The value to associate with the field
}
// HGetAll only needs a key because it retrieves all fields and values from a hash map.
#[derive(Debug)]
pub struct HGetAll {
    key: String, // The hash map's name
}

// Yes, you are absolutely correct! The purpose of implementing TryFrom<RespArray> for Command is to dispatch the conversion logic to the appropriate real command (e.g., Get, Set, HGet, etc.) based on the first element of the RespArray.
// This implementation acts as a command parser that determines which specific command struct to create and return.

// Purpose:
// Converts a RespArray (representing a serialized Redis command) into a Command.
// How It Works:
// Extracts the first element of the RespArray (the command name).
// Matches the command name (get, set, etc.) and converts the RespArray into the corresponding command struct.
// Returns an error if the command is invalid or the arguments are malformed.

// you can call .first() on a RespArray because it forwards the call to the inner Vec<RespFrame>.
// The .first() Method:
// The .first() method is defined on Vec<T> in Rust's standard library.
// It returns an Option<&T> representing the first element of the vector, or None if the vector is empty.

// v is of type RespArray.
// The Deref implementation for RespArray allows it to behave like a Vec<RespFrame>.
// When you call v.first(), Rust automatically dereferences v to access the inner Vec<RespFrame>.
// The .first() method is then called on the Vec<RespFrame>.

// Without deref coercion, you would need to write:
// v.deref().first()
// or:
// (*v).first()

// let vec = vec![1, 2, 3];
// let first = vec.first(); // Some(&1)

// You’re actually matching Some(&RespFrame::BulkString(...)),
// but Rust allows you to write it as Some(RespFrame::BulkString(...)) for convenience.

// 如果把 Some(RespFrame::BulkString(ref cmd)) 写成 Some(&RespFrame::BulkString(ref cmd))，这样就可以与 v.first() 的返回类型 Option<&RespFrame> 匹配，但这种写法太冗余，编译器还要多做一层 dereference 转化；毕竟 ref cmd 已经返回一个 &BulkString 了。
// RespFrame::BulkString(ref cmd) is accessing (or "calling out") the associated data of the BulkString variant by reference.
// It’s not “calling” in the function sense, but it’s matching and extracting the inner value of the enum variant.

// 这里的 ref cmd 是一个引用，表示我们只想借用这个数据，而不是获取它的所有权。
// 这样做的好处是，我们可以在不消耗 cmd 的情况下，使用它的值。
// 这段代码的作用是从 RespArray 中提取第一个元素，并将其转换为 RespFrame::BulkString 变体。

// It acts as a command parser that:
// Extracts the command name.
// Matches it against known commands.
// Delegates the conversion to the corresponding command struct.
// Returns meaningful errors for invalid or malformed commands.
// This design makes the code modular and extensible, as new commands can be added by implementing TryFrom<RespArray> for their respective structs.
impl TryFrom<RespArray> for Command {
    type Error = CommandError;
    fn try_from(v: RespArray) -> Result<Self, Self::Error> {
        // While matching, you are also deconstructing the RespFrame::BulkString variant to extract the associated data (cmd) for further use, such as performing submatching or additional operations.
        match v.first() {
            // Option<&T> -> Option<&RespFrame>，因为 &RespFrame， 所以 &RespFrame::BulkString(ref cmd)，但简化为 RespFrame::BulkString(ref cmd)，至于 ref cmd，是因为整个 &RespFrame::BulkString 是一个引用，所以，对应的 associated data 也需要是借用。
            // The AsRef trait is used to provide a lightweight, explicit, and flexible way to convert a type into a reference of another type.
            // In this case, converts cmd (a wrapper around Vec<u8>) into a &[u8] (a byte slice).
            // This is useful because many operations (e.g., comparisons, pattern matching) require working with slices rather than owned vectors.
            // b"get" is of type &[u8]
            Some(RespFrame::BulkString(ref cmd)) => match cmd.as_ref() {
                // BulkString is a wrapper of vec<u8>，所以二级 match 语句，进一步通过 AsRef<u8>，来比对 b"get"，b"set" 之类的 byte string literal，也就是 byte slice。
                b"get" => Ok(Get::try_from(v)?.into()),
                b"set" => Ok(Set::try_from(v)?.into()),
                b"hget" => Ok(HGet::try_from(v)?.into()),
                b"hset" => Ok(HSet::try_from(v)?.into()),
                b"hgetall" => Ok(HGetAll::try_from(v)?.into()),
                _ => Err(CommandError::InvalidCommand(format!(
                    "Invalid command: {}",
                    String::from_utf8_lossy(cmd.as_ref())
                ))),
            },
            _ => Err(CommandError::InvalidCommand(
                "Command must have a BulkString as the first argument".to_string(),
            )),
        }
    }
}

fn validate_command(
    value: &RespArray,
    names: &[&'static str],
    n_args: usize,
) -> Result<(), CommandError> {
    if value.len() != n_args + names.len() {
        return Err(CommandError::InvalidArgument(format!(
            "{} command must have exactly {} argument",
            names.join(" "),
            n_args
        )));
    }

    // .to_ascii_lowercase(): Makes sure command matching is case-insensitive.
    // "!= name.as_bytes()": Converts the expected command name (e.g., "set") to a byte slice
    // Then compares it to the received command, in lowercase.
    // This means "SET", "Set", or "set" are all valid for name = "set".

    // What is .enumerate()?
    // The .enumerate() method is an iterator adaptor in Rust that transforms an iterator into a new iterator that yields pairs of:
    // The index of each element in the original iterator.
    // A reference to the element itself.

    // The key reason this works is that Rust allows you to match a reference to an enum (&RespFrame) directly against its variants.
    // This is because Rust automatically dereferences the reference during pattern matching.
    // Without Automatic Dereferencing:
    // If Rust didn’t automatically dereference, you would need to write:
    // match *value[i] {
    //     RespFrame::BulkString(ref cmd) => { ... }
    //     _ => { ... }
    // }

    // match value[i] 之所以能够 match RespFrame::BulkString(ref cmd)，是因为后者是 enum RespFrame 的一个 variant
    // value[i] 是一个 RespFrame 的引用，Rust 会自动解引用这个引用，所以你可以直接匹配它的 variant。

    for (i, name) in names.iter().enumerate() {
        // 如何成对取出数据
        match value[i] {
            RespFrame::BulkString(ref cmd) => {
                if cmd.as_ref().to_ascii_lowercase() != name.as_bytes() {
                    // 转换为 u8 比较
                    return Err(CommandError::InvalidCommand(format!(
                        "Invalid command: expected {}, got {}",
                        name,
                        String::from_utf8_lossy(cmd.as_ref())
                    )));
                }
            }
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Command must have a BulkString as the first argument".to_string(),
                ))
            }
        }
    }
    Ok(())
}

fn extract_args(value: RespArray, start: usize) -> Result<Vec<RespFrame>, CommandError> {
    Ok(value.0.into_iter().skip(start).collect::<Vec<RespFrame>>()) // 充分利用了 iterator 的级联操作
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RespDecode, RespNull};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_command() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"); // &[u8; 24]

        let frame = RespArray::decode(&mut buf)?;
        // After decoding, frame will be a RespArray containing two RespFrame elements:
        // RespArray(vec![
        //     RespFrame::BulkString("get".into()),
        //     RespFrame::BulkString("hello".into()),
        // ])
        // The first element is RespFrame::BulkString("get".into()), representing the bulk string "get".
        // The second element is RespFrame::BulkString("hello".into()), representing the bulk string "hello".

        // When you call frame.try_into()?, the following happens:
        // Rust checks if RespArray implements TryInto<Command>.
        // Since TryFrom<RespArray> for Command is implemented, RespArray automatically implements TryInto<Command>.
        // The try_into() method calls the TryFrom<RespArray> for Command implementation.

        let cmd: Command = frame.try_into()?;

        let backend = Backend::new();

        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Null(RespNull));

        Ok(())
    }
}

// Covers the end-to-end process:
// Decoding a RESP command into a RespArray.
// Parsing the RespArray into a Command (e.g., Command::Get or Command::Set).
// Executing the command on the backend.
// Verifying the result.
