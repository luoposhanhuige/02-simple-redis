/*
- 如何解析 Frame
    - simple string: "+OK\r\n"
    - error: "-Error message\r\n"
    - bulk error: "!<length>\r\n<error>\r\n"
    - integer: ":[<+|->]<value>\r\n"
    - bulk string: "$<length>\r\n<data>\r\n"
    - null bulk string: "$-1\r\n"
    - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
        - "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
    - null array: "*-1\r\n"
    - null: "_\r\n"
    - boolean: "#<t|f>\r\n"
    - double: ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
    - map: "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
    - set: "~<number-of-elements>\r\n<element-1>...<element-n>"
 */

use crate::{
    BulkString, RespArray, RespDecode, RespError, RespFrame, RespMap, RespNull, RespNullArray,
    RespNullBulkString, RespSet, SimpleError, SimpleString,
};
use bytes::{Buf, BytesMut};

const CRLF: &[u8] = b"\r\n";
const CRLF_LEN: usize = CRLF.len();

// Prefix Matching:
// The decode method examines the first byte of the buffer to determine the type of frame.
// It uses a match statement to check the prefix byte and decide which variant's decode method to call.

// How These Methods Work Together
// .iter() creates an iterator over a collection, which can be used to iterate through elements one by one.
// .peekable() takes that iterator and wraps it in a new iterator that supports the ability to "peek" at the next item without consuming it.
// .peek() provides a way to look ahead at the next element of the iterator (if available) without advancing the iterator.

// let mut buf = BytesMut::from(&b"$5\r\nhello\r\n"[..]);
// let expected_length = BulkString::expect_length(&buf)?;
// println!("Expected length: {}", expected_length); // Output: Expected length: 10
// let frame = BulkString::decode(&mut buf)?;
// println!("Decoded frame: {:?}", frame); // Output: Decoded frame: BulkString { data: [104, 101, 108, 108, 111] }
// Yes, you can understand BytesMut as a type that provides a mutable, growable buffer for byte data, which can be referenced as &mut BytesMut for mutable operations or as &[u8] for read-only operations. BytesMut is part of the bytes crate and is designed to efficiently manage byte buffers, providing both mutable and immutable views of the data.

impl RespDecode for RespFrame {
    const PREFIX: &'static str = "";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => {
                let frame = SimpleString::decode(buf)?;
                Ok(frame.into()) // This is equivalent to Ok(RespFrame::Variant(frame)) which is Ok(RespFrame::SimpleString(frame))
            }
            Some(b'-') => {
                let frame = SimpleError::decode(buf)?;
                Ok(frame.into())
            }
            Some(b':') => {
                let frame = i64::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'$') => {
                // When the prefix byte is $, it indicates that the frame could be either a Null Bulk String or a regular Bulk String.
                // try null bulk string first, $-1\r\n
                match RespNullBulkString::decode(buf) {
                    Ok(frame) => Ok(frame.into()),
                    Err(RespError::NotComplete) => Err(RespError::NotComplete), // If the error is RespError::NotComplete, it means the buffer does not yet contain enough data to determine the frame type, so the error is propagated.
                    Err(_) => {
                        // If the error is any other type, it means the buffer does not contain a Null Bulk String, so the method proceeds to decode it as a regular Bulk String using BulkString::decode.
                        let frame = BulkString::decode(buf)?;
                        Ok(frame.into())
                    }
                }
            }
            Some(b'*') => {
                // try null array first
                match RespNullArray::decode(buf) {
                    Ok(frame) => Ok(frame.into()),
                    Err(RespError::NotComplete) => Err(RespError::NotComplete),
                    Err(_) => {
                        let frame = RespArray::decode(buf)?;
                        Ok(frame.into())
                    }
                }
            }
            Some(b'_') => {
                let frame = RespNull::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'#') => {
                let frame = bool::decode(buf)?;
                Ok(frame.into())
            }
            Some(b',') => {
                let frame = f64::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'%') => {
                let frame = RespMap::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'~') => {
                let frame = RespSet::decode(buf)?;
                Ok(frame.into())
            }
            None => Err(RespError::NotComplete),
            _ => Err(RespError::InvalidFrameType(format!(
                "expect_length: unknown frame type: {:?}",
                buf
            ))),
        }
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'*') => RespArray::expect_length(buf),
            Some(b'~') => RespSet::expect_length(buf),
            Some(b'%') => RespMap::expect_length(buf),
            Some(b'$') => BulkString::expect_length(buf),
            Some(b':') => i64::expect_length(buf),
            Some(b'+') => SimpleString::expect_length(buf),
            Some(b'-') => SimpleError::expect_length(buf),
            Some(b'#') => bool::expect_length(buf),
            Some(b',') => f64::expect_length(buf),
            Some(b'_') => RespNull::expect_length(buf),
            _ => Err(RespError::NotComplete),
        }
    }
}

// can b"+OK\r\n" be converted into &mut BytesMut automatically?
// No, the byte string b"+OK\r\n" cannot be directly converted into a &mut BytesMut automatically.
// However, you can create a BytesMut buffer from a byte slice and then pass it as a mutable reference to functions that require &mut BytesMut.

// Creating a BytesMut Buffer
// To create a BytesMut buffer from a byte slice, you can use the BytesMut::from method. Here is how you can do it:
// use bytes::BytesMut;
// let mut buf = BytesMut::from(&b"+OK\r\n"[..]);

// - simple string: "+OK\r\n"
impl RespDecode for SimpleString {
    const PREFIX: &'static str = "+";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?; // for "+OK\r\n", end is 3 which is the index of the first \r character in the \r\n sequence.

        // Splits the buffer into two at the given index.
        // Afterwards self contains elements [at, len), and the returned BytesMut contains elements [0, at).
        // This is an O(1) operation that just increases the reference count and sets a few indices.

        // 重要提醒 .split_to 中包含 advance_unchecked，所以不需要额外的 buf.advance(end + CRLF_LEN);
        // 经过 buf.split_to 之后，被切走的部分给了 data，然后剩下的后半部分仍然在 buf 中，
        // 以 "+OK\r\n" 为例，buf 中剩下的部分是 ""，data 中是 "+OK\r\n"，
        // 所以在 test_simple_string_decode，每次给 buf 重新赋予新的 b"...." 之前，buf 内部已经被清空了，所以不需要额外的 buf.advance(end + CRLF_LEN);
        // The split_to method internally advances the buffer by the specified number of bytes, effectively consuming those bytes.
        let data = buf.split_to(end + CRLF_LEN); // end 之前是 "+OK", end 之后是 "\r\n" whose length is CRLF_LEN，两者结合，相当于把 "+OK\r\n" 都拿走了
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]); // 把 "OK" 从 "+OK" 中剥离出来
        Ok(SimpleString::new(s.to_string()))
    }
    // The String::from_utf8_lossy function itself does not return an error. Instead,
    // it converts any invalid UTF-8 sequences in the byte slice to the Unicode replacement character � (U+FFFD).
    // This means that String::from_utf8_lossy will always succeed

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// - error: "-Error message\r\n"
impl RespDecode for SimpleError {
    const PREFIX: &'static str = "-";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        // split the buffer
        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(SimpleError::new(s.to_string()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// - null: "_\r\n"
impl RespDecode for RespNull {
    const PREFIX: &'static str = "_";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "_\r\n", "Null")?;
        // 经过 extract_fixed_data，buf 中的数据头部被砍掉 "_\r\n"，所以 buf.is_empty() 为 true，不为空就报错。
        // check if buf is not empty after extract_fixed_data, then give an error of InvalidFrameLength
        if !buf.is_empty() {
            return Err(RespError::InvalidFrameLength(buf.len() as isize));
        }
        // 不过其实可以做一些更简单的处理，就是默认 "_\r\nkkkkkop" 之类的也是合法的 RespNull，只要头部是 "_\r\n" 就行了，后面的数据不管。
        // 这样的话，就不需要检查 buf.is_empty() 了，直接返回 Ok(RespNull) 就行了。测试case中也不必那么麻烦。

        Ok(RespNull)
    }

    // In Rust, a variable name starting with an underscore (e.g., _buf) is a convention to indicate that the variable is intentionally unused. This can be useful to avoid compiler warnings about unused variables.
    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(3)
    }
}

// - null array: "*-1\r\n"
impl RespDecode for RespNullArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "*-1\r\n", "NullArray")?;

        if !buf.is_empty() {
            return Err(RespError::InvalidFrameLength(buf.len() as isize));
        }
        // 不过其实可以做一些更简单的处理，就是默认 "*-1\r\nkkkkkop" 之类的也是合法的 RespNull，只要头部是 "*-1\r\n" 就行了，后面的数据不管。
        // 这样的话，就不需要检查 buf.is_empty() 了，直接返回 Ok(RespNullArray) 就行了。测试case中也不必那么麻烦。

        Ok(RespNullArray)
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(4)
    }
}

// - null bulk string: "$-1\r\n"
impl RespDecode for RespNullBulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "$-1\r\n", "NullBulkString")?;

        if !buf.is_empty() {
            return Err(RespError::InvalidFrameLength(buf.len() as isize));
        }
        // 不过其实可以做一些更简单的处理，就是默认 "$-1\r\nkkkkkop" 之类的也是合法的 RespNull，只要头部是 "$-1\r\n" 就行了，后面的数据不管。
        // 这样的话，就不需要检查 buf.is_empty() 了，直接返回 Ok(RespNullBulkString) 就行了。测试case中也不必那么麻烦。

        Ok(RespNullBulkString)
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(5)
    }
}

// - integer: ":[<+|->]<value>\r\n"
impl RespDecode for i64 {
    const PREFIX: &'static str = ":";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        // split the buffer
        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(s.parse()?)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// - boolean: "#<t|f>\r\n"
impl RespDecode for bool {
    const PREFIX: &'static str = "#";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        match extract_fixed_data(buf, "#t\r\n", "Bool") {
            Ok(_) => Ok(true),
            Err(RespError::NotComplete) => Err(RespError::NotComplete),
            Err(_) => match extract_fixed_data(buf, "#f\r\n", "Bool") {
                Ok(_) => Ok(false),
                Err(e) => Err(e),
            },
        }
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(4)
    }
}

// - bulk string: "$<length>\r\n<data>\r\n"
impl RespDecode for BulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        // step 1: 获得第一次出现的 \r 的位置，也就是第一个 CRLF 的位置，以及 "$<length>\r\n<data>\r\n" 中的 <length>
        let (end, len) = parse_length(buf, Self::PREFIX)?; // parse_length > extract_simple_frame_data > find_crlf > return Some(i) > Ok(i)

        // step 2: 根据 <length>，验证 <data> 部分是否有与length匹配长度的数据，如果不够长，返回 NotComplete
        let remained = &buf[end + CRLF_LEN..];
        if remained.len() < len + CRLF_LEN {
            return Err(RespError::NotComplete);
        }

        // step 3: 把 "$<length>\r\n" 从 buf 中剥离掉
        buf.advance(end + CRLF_LEN);

        // step 4: 把 "<data>\r\n" 从 buf 中剥离出来
        let data = buf.split_to(len + CRLF_LEN);

        // step 5: 把 "<data>" 从 "<data>\r\n" 中剥离出来
        Ok(BulkString::new(data[..len].to_vec()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN + len + CRLF_LEN)
    }
}

// - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
// - "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
impl RespDecode for RespArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;

        // 因为类似 BulkString::expect_length(buf) 在计算长度时，调用 expect_length > extract_simple_frame_data > find_crlf 的过程中，只是提取了的 <length>，并没有把后续所有的数据都逐一计算在内，所以这里需要再次验证 buf 中的数据是否足够长
        // 比如 b"*2\r\n$3\r\nset\r\n$6\r\ncan"，虽然第2个 BulkString 的长度是 5，但是 can 不够长，正确的字符串是 cannon\r\n ，所以这里需要再次验证 buf 中的数据是否足够长
        if buf.len() < total_len {
            return Err(RespError::NotComplete);
        }

        buf.advance(end + CRLF_LEN);

        let mut frames = Vec::with_capacity(len);
        for _ in 0..len {
            frames.push(RespFrame::decode(buf)?);
        }
        // 以 "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n" 为例，frames 中的内容是 [BulkString { data: [103, 101, 116] }, BulkString { data: [104, 101, 108, 108, 111] }]
        // 每一次调用 RespFrame::decode(buf)?， buf 中的数据会被逐渐剥离，因为每一次的 decode 都会调用 buf.advance(end + CRLF_LEN)，所以 buf 中的数据会逐渐减少

        Ok(RespArray::new(frames))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

// - double: ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
// (123.456).into(), b",+123.456\r\n"
// (-123.456).into(), b",-123.456\r\n"
// (1.23456e+8).into(), b",+1.23456e8\r\n"
// (-1.23456e-9).into(), b",-1.23456e-9\r\n"
impl RespDecode for f64 {
    const PREFIX: &'static str = ",";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(s.parse()?)
        // what is the difference between s.parse::<f64>()? and s.parse()? in Rust?
        // The difference between s.parse::<f64>()? and s.parse()? in Rust is that s.parse::<f64>()? explicitly specifies the type to parse the string into (f64), while s.parse()? infers the type based on the context. Both methods are used to parse a string into a specific type, but the former is more explicit about the target type.
        // s is -1.23456e-9, what would get from s.parse::<f64>()? and s.parse()?
        // The result of s.parse::<f64>()? and s.parse()? would be the same in this case, as the string "-1.23456e-9" can be unambiguously parsed as a floating-point number. Both methods would successfully parse the string into the f64 value -1.23456e-9.
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = extract_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// - map: "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
// b"%2\r\n +foo\r\n -> -123456.789\r\n  +hello\r\n -> $5\r\nworld\r\n"
impl RespDecode for RespMap {
    const PREFIX: &'static str = "%";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;

        if buf.len() < total_len {
            return Err(RespError::NotComplete);
        }

        buf.advance(end + CRLF_LEN);

        let mut frames = RespMap::new();
        for _ in 0..len {
            let key = SimpleString::decode(buf)?;
            let value = RespFrame::decode(buf)?;
            frames.insert(key.0, value); // The key of a RespMap is of type String, not SimpleString. This is why key.0 is used to access the inner String value of the SimpleString instance before inserting it into the RespMap
        }

        Ok(frames)
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}

// - set: "~<number-of-elements>\r\n<element-1>...<element-n>"
// b"~2\r\n*2\r\n:+1234\r\n#t\r\n$5\r\nworld\r\n"
impl RespDecode for RespSet {
    const PREFIX: &'static str = "~";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;

        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;

        if buf.len() < total_len {
            return Err(RespError::NotComplete);
        }

        buf.advance(end + CRLF_LEN);

        let mut frames = Vec::new();
        for _ in 0..len {
            frames.push(RespFrame::decode(buf)?);
        }

        Ok(RespSet::new(frames))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}
// the implementations of RespDecode for RespArray and RespSet are very similar.
// Both implementations follow a similar structure to decode their respective types from a buffer.
// The main differences are in the prefixes they use and the types they return.

// the buf.advance(expect.len()) call inside the extract_fixed_data function does affect the buffer (buf) outside the function.
// This is because buf is passed as a mutable reference (&mut BytesMut), and any modifications to it within the function will persist after the function returns.
// 本函数的目的是验证 buf 是否以 expect 开头，如果是，则把 expect 从 buf 中剥离出来，该步骤通过 buf.advance(expect.len())，这样 原始 buf 就被砍头了。
// 本函数影响的是传递进来的 buf: &mut BytesMut，所以在函数外部，buf 会被改变，因为 buf 是可变引用，因此，函数的返回值是 Result<(), RespError>，而不是 Result<BytesMut, RespError>。
fn extract_fixed_data(
    buf: &mut BytesMut,
    expect: &str,
    expect_type: &str,
) -> Result<(), RespError> {
    if buf.len() < expect.len() {
        return Err(RespError::NotComplete);
    }

    // 这里Err提示信息与check的内容不一致，因为check的是是否含有“expect内容”的前缀，但给出的error信息是“expect类型”错误的反馈
    // 建议修改为“纯粹的内容之间的对比”：
    //      "Expected prefix: {:?}, got: {:?}",
    //      expect, &buf[..expect.len()]
    if !buf.starts_with(expect.as_bytes()) {
        // Converts a string slice to a byte slice.
        return Err(RespError::InvalidFrameType(format!(
            "expect: {}, got: {:?}",
            expect_type, buf
        )));
    }

    // The advance method is used to move the internal cursor of a buffer forward by a specified number of bytes. This effectively "consumes" the specified number of bytes from the buffer, making them no longer available for future operations.
    // for example:
    // let mut buf = BytesMut::from(&b"hello world"[..]);
    // buf.advance(6);
    // assert_eq!(&buf[..], b"world");
    buf.advance(expect.len()); // 这个就是本函数的核心，把想要裁掉的头部裁掉了，然后影响到函数之外的 buf
    Ok(())
}

// The name extract_simple_frame_data might be misleading because it suggests that the function extracts and returns the data itself, whereas it actually returns the index of the \r character in the \r\n sequence.
// To make the function name more accurate and reflective of its purpose, we can rename it to something like find_frame_end_index.

// 这个函数具有三合一作用：两步验证是否是指定类型的frame，一步找到第一个CRLF的位置
// find_frame_end_index
// 本函数是否改变了 buf 的内容？没有，因为函数以及 find_crlf，都没有调用 advance。
fn extract_simple_frame_data(buf: &[u8], prefix: &str) -> Result<usize, RespError> {
    if buf.len() < 3 {
        return Err(RespError::NotComplete);
    }

    if !buf.starts_with(prefix.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "expect: SimpleString({}), got: {:?}", // The error message is not always accurate because it assumes that the expected type is always SimpleString, which is not the case. The function is used for various types, so the error message should be more generic.
            prefix, buf
        )));
    }

    // to find the first CRLF sequence to determine the end of the first line/frame. This is why find_crlf is called with nth: 1.
    // In the find_crlf function, the nth parameter specifies the occurrence of the \r\n sequence you are looking for, and the function returns the index of the \r character in that sequence.
    let end = find_crlf(buf, 1).ok_or(RespError::NotComplete)?; // Transforms the Option<T> into a Result<T, E>, mapping [Some(v)] to [Ok(v)] and None to [Err(err)]

    Ok(end) // return the index of the first occurrence of \r in the buffer since nth is 1
            // for b"$5\r\nhello\r\n", the end is 2, since the first \r\n sequence appears at index 2-3
}

// find nth CRLF in the buffer

// b"$5\r\nhello\r\n" has 11 bytes in total.
// The \r\n sequences appear at index 2-3 and index 9-10.
// Loop Bounds (1..buf.len() - 1)

// buf.len() is 11, so buf.len() - 1 is 10.
// 1..10 means the loop starts at index 1 and stops at 9 (before 10).
// Why Start at 1?

// The loop starts at 1 because buf[i] should not be the very first byte ($ at index 0), which can't be part of \r\n.
// Why Stop at buf.len() - 1 (Index 9)?

// The function checks buf[i] and buf[i + 1] for \r\n.
// If i were allowed to reach buf.len() - 1 (index 10), then buf[i + 1] would be out of bounds.

// This for loop iterates over the indices of the buffer from 1 to buf.len() - 2（包括）.
// For each index i, it checks if buf[i] and buf[i + 1] are \r and \n, respectively.
// The reason for starting at 1 and ending at buf.len() - 2 is to ensure that we can safely check both buf[i] and buf[i + 1] without going out of bounds.

// In the find_crlf function, the nth parameter specifies the occurrence of the \r\n sequence you are looking for, and the function returns the index of the \r character in that sequence.
// the find_crlf is called with nth to find the nth occurrence of the \r\n sequence in the buffer.
// The function returns the index of the \r character in that sequence, which is used to determine the end of the frame.

// find_crlf 为什么不用判断 buf 的长度是否大于 2 等？因为调用 find_crlf 之前，extract_simple_frame_data 已经判断了 buf 的长度是否大于 3，所以这里不需要再判断了。
// 如果想把 find_crlf 做成 public api 供更多函数调用，那么需要在 find_crlf 中加入对 buf 长度的判断。
fn find_crlf(buf: &[u8], nth: usize) -> Option<usize> {
    let mut count = 0;
    for i in 1..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            count += 1;
            if count == nth {
                // count equals 1, then return the index of the first occurrence of \r character in the sequence; count equals 2, then return the index of the second occurrence of \r character in the sequence, and etc.
                return Some(i);
            }
        }
    }

    None
}

// - For the input `b"$5\r\nhello\r\n"`, the prefix length is `1` (length of `"$"`), and [end] is `2`.
// - The length string is `&buf[1..2]`, which is `"5"`.
// &buf[1..2] only take the byte at index 1, which is `5`, since the end is exclusive.

// - The length string is then converted to a `String` using `String::from_utf8_lossy`.
// Converts a slice of bytes to a string, including invalid characters.
// Strings are made of bytes (u8), and a slice of bytes (&[u8]) is made of bytes, so this function converts between the two.
// Not all byte slices are valid strings, however: strings are required to be valid UTF-8.
// During this conversion, from_utf8_lossy() will replace any invalid UTF-8 sequences with U+FFFD REPLACEMENT CHARACTER,
// which looks like this: �

// The term "lossy" in this context means that if the byte slice contains any invalid UTF-8 sequences, they will be replaced with the Unicode replacement character � (U+FFFD).

// String::from_utf8_lossy:
// Converts a slice of bytes to a Cow<str>.
// If the byte slice is valid UTF-8, it returns a borrowed &str.
// If the byte slice contains invalid UTF-8, it returns an owned String with invalid sequences replaced by �.

// The String::from_utf8_lossy function returns a Cow<str>, which stands for "Clone on Write".
// This type can either be a borrowed string slice (&str) or an owned String.
// The Cow type provides a way to work with borrowed data that can be converted to owned data if necessary.
// The Cow<str> type implements the Deref trait, which allows it to be used as if it were a &str.
// The parse method is available on &str, and because Cow<str> can be dereferenced to &str, you can call parse directly on a Cow<str>.

// the function is to extract the length of the frame from the buffer based on the prefix.
// for b"$5\r\nhello\r\n", the length is 5, 被包含中字符串中，我们需要写一个函数，把5剥离出来，并从 &str 转换为 usize
// for b"$5\r\nhello\r\n", prefix is "$", end is 2, and s is "5"
// 其实，函数名称改为 extract_length 更合适，因为它的作用是从 buffer 中提取出长度，而不是提取出数据。
fn parse_length(buf: &[u8], prefix: &str) -> Result<(usize, usize), RespError> {
    let end = extract_simple_frame_data(buf, prefix)?;
    let s = String::from_utf8_lossy(&buf[prefix.len()..end]); // Cow<str> can be either a &str (borrowed) or a String (owned).
    Ok((end, s.parse()?)) // Parses this string slice into another type. in this case, it parses the string slice into a usize.
}
// In Rust, the parse method is a generic method that can parse a string slice (&str) into various types that implement the FromStr trait.
// The specific type to parse into is determined by the context in which parse is called.
// since Result<(usize, usize), RespError>, the compiler can infers that the result of s.parse()? should be either usize or RespError.
// if the result is of RespError, which is RespError::ParseIntError, then it will be automatically converted to RespError.

// 对于 RespArray，len 是指数组中元素的个数，
// 对于 RespMap，len 是指键值对的个数，
// 对于 RespSet，len 是指集合中元素的个数，
// 对于 BulkString，len 是指字符串的长度，
// 对于 SimpleString，len 是指字符串的长度...
fn calc_total_length(buf: &[u8], end: usize, len: usize, prefix: &str) -> Result<usize, RespError> {
    let mut total = end + CRLF_LEN;
    let mut data = &buf[total..];
    match prefix {
        "*" | "~" => {
            // find nth CRLF in the buffer, for array and set, we need to find 1 CRLF for each element
            for _ in 0..len {
                let len = RespFrame::expect_length(data)?;
                data = &data[len..];
                total += len;
            }
            Ok(total)
        }
        "%" => {
            // find nth CRLF in the buffer. For map, we need to find 2 CRLF for each key-value pair
            // b"%2\r\n  +hello\r\n -> $5\r\nworld\r\n  +foo\r\n -> $3\r\nbar\r\n"
            for _ in 0..len {
                // len is the number of key-value pairs，对于每一个 key-value pair，需要找到两个 CRLF
                // step 1: 计算第_个 key 的长度
                let len = SimpleString::expect_length(data)?; // 调用了 extract_simple_frame_data，但并没有改变 buf 的内容
                                                              // step 2: 从第_个 key 之后，引用 buf 的剩余部分，以便计算第_个 value 的长度
                data = &data[len..];
                total += len;
                // step 3: 计算第_个 value 的长度
                let len = RespFrame::expect_length(data)?;
                data = &data[len..];
                total += len;
            }
            Ok(total)
        }
        _ => Ok(len + CRLF_LEN), // 因为 RespMap, RespArray, RespSet 可以嵌套包含其他类型的 frame，所以需要递归计算总长度，递归到最后，就是其他类型的 frame 不需要递归计算总长度
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use bytes::BufMut;

    // In the `test_simple_string_decode` function, the `buf.advance()` method is implicitly called within the `SimpleString::decode` method.
    // This method processes the buffer, advances its internal cursor, and consumes the bytes that have been processed.
    // This is why you don't see an explicit call to `buf.advance()` in the test function itself.
    // The split_to method internally advances the buffer by the specified number of bytes, effectively consuming those bytes.
    #[test]
    fn test_simple_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"+OK\r\n");

        let frame = SimpleString::decode(&mut buf)?;
        assert_eq!(frame, SimpleString::new("OK".to_string()));

        buf.extend_from_slice(b"+hello\r");

        let ret = SimpleString::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        buf.put_u8(b'\n');
        let frame = SimpleString::decode(&mut buf)?;
        assert_eq!(frame, SimpleString::new("hello".to_string()));
        // Writes an unsigned 8 bit integer to self. The current position is advanced by 1.
        // The put_u8 method advances the position to ensure that subsequent write operations do not overwrite the byte that was just written. This is a common behavior for methods that write data to buffers, as it maintains the integrity of the data being written.

        Ok(())
    }

    #[test]
    fn test_simple_error_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"-Error message\r\n");

        let frame = SimpleError::decode(&mut buf)?;
        assert_eq!(frame, SimpleError::new("Error message".to_string()));

        Ok(())
    }

    #[test]
    fn test_integer_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b":+123\r\n");

        let frame = i64::decode(&mut buf)?;
        assert_eq!(frame, 123);

        buf.extend_from_slice(b":-123\r\n");

        let frame = i64::decode(&mut buf)?;
        assert_eq!(frame, -123);

        Ok(())
    }

    #[test]
    fn test_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$5\r\nhello\r\n");

        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"hello"));

        buf.extend_from_slice(b"$5\r\nhello");
        let ret = BulkString::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        buf.extend_from_slice(b"\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"hello"));

        Ok(())
    }

    #[test]
    fn test_null_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$-1\r\n");

        let frame = RespNullBulkString::decode(&mut buf)?;
        assert_eq!(frame, RespNullBulkString);

        Ok(())
    }

    #[test]
    fn test_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*-1\r\n");

        let frame = RespNullArray::decode(&mut buf)?;
        assert_eq!(frame, RespNullArray);

        buf.extend_from_slice(b"*-1\r\nkkk");
        let ret = RespNullArray::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespError::InvalidFrameLength(3));

        Ok(())
    }

    #[test]
    fn test_null_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"_\r\n");

        let frame = RespNull::decode(&mut buf)?;
        assert_eq!(frame, RespNull);

        buf.extend_from_slice(b"_\r\nkkk");

        // the resulf of RespNull::decode(&mut buf)? would be RespError::InvalidFrameLength(buf.len() as isize)
        let ret = RespNull::decode(&mut buf); // 经过 advance 之后，buf 剩下 "kkk"
        assert_eq!(ret.unwrap_err(), RespError::InvalidFrameLength(3));
        buf.clear();

        buf.extend_from_slice(b"_\r");

        // let ret = RespNull::decode(&mut buf); does not use the ? operator because it is not necessary to propagate the error in this context.
        // Instead, the error is handled explicitly using unwrap_err().
        let ret = RespNull::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);
        buf.clear();

        Ok(())
    }

    #[test]
    fn test_boolean_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"#t\r\n");

        let frame = bool::decode(&mut buf)?;
        assert!(frame);

        buf.extend_from_slice(b"#f\r\n");

        let frame = bool::decode(&mut buf)?;
        assert!(!frame);

        buf.extend_from_slice(b"#f\r");
        let ret = bool::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        buf.put_u8(b'\n');
        let frame = bool::decode(&mut buf)?;
        assert!(!frame);

        Ok(())
    }

    #[test]
    fn test_array_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nset\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(frame, RespArray::new([b"set".into(), b"hello".into()]));

        buf.extend_from_slice(b"*2\r\n$3\r\nset\r\n");
        let ret = RespArray::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        buf.extend_from_slice(b"$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(frame, RespArray::new([b"set".into(), b"hello".into()]));

        Ok(())
    }

    #[test]
    fn test_double_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b",123.45\r\n");

        let frame = f64::decode(&mut buf)?;
        assert_eq!(frame, 123.45);

        buf.extend_from_slice(b",+1.23456e-9\r\n");
        let frame = f64::decode(&mut buf)?;
        assert_eq!(frame, 1.23456e-9);

        Ok(())
    }

    #[test]
    fn test_map_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"%2\r\n+hello\r\n$5\r\nworld\r\n+foo\r\n$3\r\nbar\r\n");

        let frame = RespMap::decode(&mut buf)?;
        let mut map = RespMap::new();
        map.insert(
            "hello".to_string(),
            BulkString::new(b"world".to_vec()).into(),
        );
        map.insert("foo".to_string(), BulkString::new(b"bar".to_vec()).into());
        assert_eq!(frame, map);

        Ok(())
    }

    #[test]
    fn test_set_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"~2\r\n$3\r\nset\r\n$5\r\nhello\r\n");

        let frame = RespSet::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespSet::new(vec![
                BulkString::new(b"set".to_vec()).into(),
                BulkString::new(b"hello".to_vec()).into()
            ])
        );

        Ok(())
    }

    #[test]
    fn test_calc_array_length() -> Result<()> {
        let buf = b"*2\r\n$3\r\nset\r\n$5\r\nhello\r\n";
        let (end, len) = parse_length(buf, "*")?;
        let total_len = calc_total_length(buf, end, len, "*")?;
        assert_eq!(total_len, buf.len());

        let buf = b"*2\r\n$3\r\nset\r\n";
        let (end, len) = parse_length(buf, "*")?;
        let ret = calc_total_length(buf, end, len, "*");
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        Ok(())
    }
}
