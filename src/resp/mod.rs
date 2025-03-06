// mod decode;
mod encode;

use enum_dispatch::enum_dispatch;
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

#[enum_dispatch]
pub trait RespEncode {
    fn encode(self) -> Vec<u8>;
}

pub trait RespDecode {
    fn decode(buf: Self) -> Result<RespFrame, String>;
}

#[enum_dispatch(RespEncode)]
#[derive(Debug, PartialEq, PartialOrd)]
pub enum RespFrame {
    SimpleString(SimpleString),
    Error(SimpleError),
    Integer(i64),
    BulkString(BulkString),
    NullBulkString(RespNullBulkString),
    Array(RespArray),
    NullArray(RespNullArray),
    Null(RespNull),
    Boolean(bool),
    Double(f64),
    Map(RespMap),
    Set(RespSet),
}
// RespFrame is like a container for all the types that implement the RespEncode trait.

// each variants of tuple struct is a wrapper of the inner type
// with Deref trait implementation, we can access the inner type directly
// without unwrapping the outer type

// RespArray is a wrapper of inner Vec<RespFrame>
// RespMap is a wrapper of inner BTreeMap<String, RespFrame>
// RespSet is a wrapper of inner Vec<RespFrame>

// difference between RespArray with RespSet is that RespArray is ordered with elements of the same type,
// while RespSet is unordered with elements of different types

// #[enum_dispatch(RespEncode)] 简化了类似如下的步骤：
// impl RespEncode for RespFrame {
// fn encode(&self) -> Vec<u8> {
//     match self {
//         RespFrame::SimpleString(s) => s.encode(),
//         RespFrame::SimpleError(e) => e.encode(),
//         RespFrame::Integer(i) => i.encode(),
//         RespFrame::BulkString(b) => b.encode(),
//         RespFrame::Array(a) => a.encode(),
//         RespFrame::Null => Null.encode(), // Assuming Null has an implementation
//     }
// }

#[derive(Debug, PartialEq, Eq, PartialOrd)]
pub struct SimpleString(String);
#[derive(Debug, PartialEq, Eq, PartialOrd)]
pub struct SimpleError(String);
#[derive(Debug, PartialEq, Eq, PartialOrd)]
pub struct BulkString(Vec<u8>);
#[derive(Debug, PartialEq, Eq, PartialOrd)]
pub struct RespNull;
#[derive(Debug, PartialEq, PartialOrd)]
pub struct RespArray(Vec<RespFrame>);
#[derive(Debug, PartialEq, Eq, PartialOrd)]
pub struct RespNullArray;
#[derive(Debug, PartialEq, Eq, PartialOrd)]
pub struct RespNullBulkString;
#[derive(Debug, PartialEq, PartialOrd)]
pub struct RespMap(BTreeMap<String, RespFrame>);
#[derive(Debug, PartialEq, PartialOrd)]
pub struct RespSet(Vec<RespFrame>);

impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for SimpleError {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for BulkString {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for RespArray {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for RespMap {
    type Target = BTreeMap<String, RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for RespSet {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleString(s.into())
    }
}

impl SimpleError {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleError(s.into())
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        BulkString(s.into())
    }
}

impl RespArray {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        RespArray(s.into())
    }
}

impl RespMap {
    pub fn new() -> Self {
        RespMap(BTreeMap::new())
    }
}

// 因为 RespMap::new() 没有带参数，所以才用 Default trait 来实现初始化。
impl Default for RespMap {
    fn default() -> Self {
        RespMap::new()
    }
}

impl RespSet {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        RespSet(s.into())
    }
}

// Why Eq is Needed for Some Structs
// SimpleString, SimpleError, BulkString, RespNull, RespNullArray, RespNullBulkString:

// These types have a clear and total equivalence relation.
// Deriving Eq makes sense because their instances can be compared for total equality.
// Example: Two SimpleString instances with the same inner String are considered equal.
// RespArray, RespMap, RespSet:

// These types involve collections or complex structures where total equivalence might not be straightforward or meaningful.
// Deriving Eq might not be necessary or appropriate if the type does not have a clear total equivalence relation.
// Example: Comparing two RespArray instances might involve complex logic that does not guarantee total equivalence.
