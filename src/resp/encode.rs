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
    BulkString, RespArray, RespEncode, RespMap, RespNull, RespNullArray, RespNullBulkString,
    RespSet, SimpleError, SimpleString,
};

const BUF_CAP: usize = 4096; // is this the size of bytes or bits?  4096 bytes

// 原则上，都是把字符串变成 u8 字节流，然后用 vec 存储，并返回。

// - simple string: "+OK\r\n"
impl RespEncode for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes() // b"+{}\r\n", equals a vector of u8.
    }
}

// - error: "-Error message\r\n"
impl RespEncode for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

// - integer: ":[<+|->]<value>\r\n"
impl RespEncode for i64 {
    fn encode(self) -> Vec<u8> {
        let sign = if self < 0 { "" } else { "+" }; // because the negative number already has a sign of '-'
        format!(":{}{}\r\n", sign, self).into_bytes()
    }
}

// - bulk string: "$<length>\r\n<data>\r\n"
// "$<length>\r\n" + <data> + "\r\n"，三块数据都是 u8 字节流，然后通过.extend_from_slice() 方法拼接到一起。
impl RespEncode for BulkString {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.len() + 16); // 预留 addtioanl 16 bytes for the prefix and suffix which is "$<length>\r\n" + "\r\n"，\r or \n is a single byte
        buf.extend_from_slice(&format!("${}\r\n", self.len()).into_bytes());
        buf.extend_from_slice(&self); // pub struct BulkString(Vec<u8>), BulkString::new(b"hello".to_vec())
        buf.extend_from_slice(b"\r\n");
        buf
    }
    // what is the length of "$5\r\nhello\r\n"?
    // 11
    // because \r or \n is a single byte, so the length of "$5\r\nhello\r\n" is 11
}

// Vec::with_capacity
// Initializing buf with a specified capacity using Vec::with_capacity(self.len() + 16) can improve performance by reducing the number of reallocations and copying of data.
// Initializing buf without specifying a capacity using Vec::new() will still work correctly, but it may be less efficient due to potential reallocations and copying of data.
// If the final size of the Vec exceeds its initial capacity specified by with_capacity(), the Vec will automatically reallocate memory to accommodate the additional elements. This reallocation involves allocating a new memory block with a larger capacity, copying the existing elements to the new block, and then freeing the old memory block.

// - null bulk string: "$-1\r\n"
impl RespEncode for RespNullBulkString {
    fn encode(self) -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }
}

// - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
impl RespEncode for RespArray {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("*{}\r\n", self.0.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

// - null array: "*-1\r\n"
impl RespEncode for RespNullArray {
    fn encode(self) -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}

// - null: "_\r\n"
impl RespEncode for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

// - boolean: "#<t|f>\r\n"
impl RespEncode for bool {
    fn encode(self) -> Vec<u8> {
        format!("#{}\r\n", if self { "t" } else { "f" }).into_bytes()
    }
}

// - double: ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
impl RespEncode for f64 {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);

        // Determine the format based on the value of self
        // ret: return value
        let ret = if self.abs() > 1e+8 || self.abs() < 1e-8 {
            // Format in scientific notation if the number is very large or very small
            format!(",{:+e}\r\n", self)
        } else {
            // Format with a sign if the number is within the normal range
            let sign = if self < 0.0 { "" } else { "+" };
            format!(",{}{}\r\n", sign, self)
        };

        buf.extend_from_slice(&ret.into_bytes());
        buf
    }
}

// {:+e} 的解释的核心，此处的e的含义：

// ### Explanation of the Code

// - **Scientific Notation**:
//   - If the absolute value of the number is greater than `1e+8` or less than `1e-8`, it is formatted in scientific notation with a sign using `format!(",{:+e}\r\n", self)`.
//   - This ensures that very large or very small numbers are represented in a concise and readable format.

// - **Normal Range**:
//   - If the number is within the normal range, it is formatted with a sign using `format!(",{}{}\r\n", sign, self)`.
//   - The `sign` variable is determined based on whether the number is negative or positive.

// ### Summary

// - The `{:+e}` format specifier in Rust's `format!` macro is used to format floating-point numbers in scientific notation with a sign.
// - The `+` flag ensures that the output always includes a sign, even for positive numbers.
// - The `e` specifier indicates that the number should be formatted in scientific notation.
// - In your code, `{:+e}` is used to format very large or very small numbers in a concise and readable format.

// By understanding the `{:+e}` format specifier, you can see how it is used to format floating-point numbers in scientific notation with a sign, ensuring that the output is clear and consistent.

// - map: "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
// we only support string key which encode to SimpleString
impl RespEncode for RespMap {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("%{}\r\n", self.len()).into_bytes());
        for (key, value) in self.0 {
            buf.extend_from_slice(&SimpleString::new(key).encode());
            buf.extend_from_slice(&value.encode());
        }
        buf
    }
}

// - set: "~<number-of-elements>\r\n<element-1>...<element-n>"
impl RespEncode for RespSet {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("~{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use crate::RespFrame;

    use super::*;

    #[test]
    fn test_simple_string_encode() {
        let frame: RespFrame = SimpleString::new("OK").into();
        // 这里的 into() 转换，是因为 frame:: RespFrame 显式标注，所以编译器强行转换
        // 虽然 frame 是 RespFrame 类型，但编译器了解并关联该 instance 到 SimpleString 类型
        // 这样，当调用 frame.encode() 时，编译器会调用 SimpleString 的 encode 方法
        // This creates an instance of RespFrame with the SimpleString variant.
        // When frame.encode() is called, the enum_dispatch crate dispatches the call to the encode method of the SimpleString variant.

        assert_eq!(frame.encode(), b"+OK\r\n");
        // what is b"+OK\r\n"?
        // it is a byte string. is it equal a vector of u8?
        // yes, it is equal to a vector of u8
        // how does compiler infer frame.encode to RespFrame::SimpleError(e) => e.encode()?
    }

    #[test]
    fn test_error_encode() {
        let frame: RespFrame = SimpleError::new("Error message").into();

        assert_eq!(frame.encode(), b"-Error message\r\n");
    }

    #[test]
    fn test_integer_encode() {
        let frame: RespFrame = 123.into();
        assert_eq!(frame.encode(), b":+123\r\n");

        let frame: RespFrame = (-123).into();
        assert_eq!(frame.encode(), b":-123\r\n");
    }

    #[test]
    fn test_bulk_string_encode() {
        let frame: RespFrame = BulkString::new(b"hello").into();
        assert_eq!(frame.encode(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_null_bulk_string_encode() {
        let frame: RespFrame = RespNullBulkString.into();
        assert_eq!(frame.encode(), b"$-1\r\n");
    }

    #[test]
    fn test_array_encode() {
        let frame: RespFrame = RespArray::new(vec![
            // 也可以直接用 [] 代替 vec![]，因为 array implements Into<Vec<RespFrame>>
            BulkString::new("set".to_string()).into(),
            BulkString::new("hello".to_string()).into(),
            BulkString::new("world".to_string()).into(),
        ])
        .into();
        assert_eq!(
            &frame.encode(),
            b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
        );
    }

    #[test]
    fn test_null_array_encode() {
        let frame: RespFrame = RespNullArray.into();
        assert_eq!(frame.encode(), b"*-1\r\n");
    }

    #[test]
    fn test_null_encode() {
        let frame: RespFrame = RespNull.into();
        assert_eq!(frame.encode(), b"_\r\n");
    }

    #[test]
    fn test_boolean_encode() {
        let frame: RespFrame = true.into();
        assert_eq!(frame.encode(), b"#t\r\n");

        let frame: RespFrame = false.into();
        assert_eq!(frame.encode(), b"#f\r\n");
    }

    #[test]
    fn test_double_encode() {
        let frame: RespFrame = 123.456.into();
        assert_eq!(frame.encode(), b",+123.456\r\n");

        let frame: RespFrame = (-123.456).into();
        assert_eq!(frame.encode(), b",-123.456\r\n");

        let frame: RespFrame = 1.23456e+8.into();
        assert_eq!(frame.encode(), b",+1.23456e8\r\n");

        let frame: RespFrame = (-1.23456e-9).into();
        assert_eq!(&frame.encode(), b",-1.23456e-9\r\n");
    }

    #[test]
    fn test_map_encode() {
        let mut map = RespMap::new();
        map.insert("hello".to_string(), BulkString::new("world").into());
        map.insert("foo".to_string(), (-123456.789).into());

        let frame: RespFrame = map.into();
        assert_eq!(
            &frame.encode(),
            b"%2\r\n+foo\r\n,-123456.789\r\n+hello\r\n$5\r\nworld\r\n"
        );
    }

    #[test]
    fn test_set_encode() {
        let frame: RespFrame = RespSet::new([
            // 这里有偷懒的成分，
            // 更切实的做法是，先把 1234 和 true 分别通过 RespFrame::Integer 和 RespFrame::Boolean 转换成 RespFrame 类型，然后再放入 RespArray 中。
            RespArray::new([1234.into(), true.into()]).into(),
            BulkString::new("world").into(),
        ])
        .into();
        assert_eq!(
            frame.encode(),
            b"~2\r\n*2\r\n:+1234\r\n#t\r\n$5\r\nworld\r\n"
        );
    }
    // RespSet 与 RespArray 的区别在于，两者虽然都是用 Vec 存储，但 RespSet 特意用于与 把不同类型的元素通过 enum 统一封装为统一类型的 RespFrame，而 RespArray 则是用于存储相同类型的元素。
    // If you want a collection of values (not just unique values) of different types with only values and no keys, you can use a Vec in combination with an enum to encapsulate the different types. This allows you to store a heterogeneous collection of values in a single vector.
    // 当然，这只是刻意为之，rust 原生库中有 BTreeSet 和 HashSet 用于存储相同类型的元素，而 BTreeMap 和 HashMap 用于存储不同类型的元素。
}
