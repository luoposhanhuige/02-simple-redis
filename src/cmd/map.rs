use super::{extract_args, validate_command, CommandExecutor, Get, Set, RESP_OK};
use crate::{cmd::CommandError, RespArray, RespFrame, RespNull};

// Key Takeaway
// The connection between impl TryFrom<RespArray> for Get and impl CommandExecutor for Get is that:

// TryFrom<RespArray> converts the raw RESP command into a structured Get command.
// CommandExecutor executes the Get command by interacting with the backend and returning the result.
// Together, they form a pipeline that processes the GET command from the client and produces the appropriate response.

// Yes, exactly! The impl TryFrom<RespArray> for Get (and similarly for Set) acts as a parser that converts a RespArray (representing the raw RESP command sent by the client) into a structured command object (like Get or Set).
// This is a critical step in the execution flow because it validates and extracts the necessary arguments from the raw command before executing it.

// Yes, that's correct! In the request_handler function in network.rs,
// the execution flow first calls TryFrom to parse the raw RESP frame into a structured Command,
// and then it calls CommandExecutor to execute the parsed command.

impl CommandExecutor for Get {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        match backend.get(&self.key) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for Set {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        backend.set(self.key, self.value);
        RESP_OK.clone() // Since RESP_OK is a static variable, it is immutable and shared across the entire program. To return a new instance of RespFrame from the execute method, you need to create a copy of the value stored in RESP_OK.
    }
}

impl TryFrom<RespArray> for Get {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["get"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(Get {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for Set {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["set"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(Set {
                key: String::from_utf8(key.0)?,
                value,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or value".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Backend, RespDecode};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_get_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: Get = frame.try_into()?;
        assert_eq!(result.key, "hello");

        Ok(())
    }
    // Focuses only on the parsing phase:
    // Decoding a RESP command into a RespArray.
    // Parsing the RespArray into a Get struct.
    // Verifying that the Get struct is correctly created.

    #[test]
    fn test_set_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: Set = frame.try_into()?;
        assert_eq!(result.key, "hello");
        assert_eq!(result.value, RespFrame::BulkString(b"world".into()));

        Ok(())
    }

    #[test]
    fn test_set_get_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = Set {
            key: "hello".to_string(),
            value: RespFrame::BulkString(b"world".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = Get {
            key: "hello".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RespFrame::BulkString(b"world".into()));

        Ok(())
    }
}
