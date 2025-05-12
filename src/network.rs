// This module likely contains the logic for handling individual client connections.
// It processes incoming commands, interacts with the backend, and sends responses back to the client.

// cmd: Contains the Command enum and CommandExecutor trait for parsing and executing commands.
use crate::{
    cmd::{Command, CommandExecutor},
    Backend, RespDecode, RespEncode, RespError, RespFrame,
};
use anyhow::Result;
use futures::SinkExt;
// tokio and tokio_util:
// Used for asynchronous networking and framing (splitting streams into frames).
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::info;

// RespFrameCodec:
// A codec for encoding and decoding RESP frames.
// Used with tokio_util::codec::Framed to handle streams of RESP frames.
#[derive(Debug)]
struct RespFrameCodec; // The term codec is short for "coder-decoder"
                       // It refers to a system or component that:
                       // Encodes structured data into a specific format (e.g., raw bytes for transmission).
                       // Decodes data from that format back into structured data.
                       // In the context of networking, a codec is used to handle the serialization and deserialization of data as it is sent and received over a network connection.

// RedisRequest:
// Represents a client request.
// Contains:
// frame: The parsed RESP frame from the client.
// backend: A reference to the backend storage system.
#[derive(Debug)]
struct RedisRequest {
    frame: RespFrame,
    backend: Backend,
}

// RedisResponse:
// Represents a server response.
// Contains:
// frame: The RESP frame to send back to the client.
#[derive(Debug)]
struct RedisResponse {
    frame: RespFrame,
}

// Handles a single client connection.
// Reads data from the stream, processes commands, and writes responses back to the client.
pub async fn stream_handler(stream: TcpStream, backend: Backend) -> Result<()> {
    // how to get a frame from the stream?
    // Create a Framed Stream:
    // Wraps the TcpStream with RespFrameCodec to handle RESP frame encoding/decoding.

    // How Framed Works
    // 1. Input (Decoding)
    // Framed reads raw bytes from the underlying stream (e.g., TcpStream).
    // It uses the Decoder implementation of the codec (e.g., RespFrameCodec) to convert the raw bytes into structured frames (e.g., RespFrame).
    // 2. Output (Encoding)
    // When you send a frame (e.g., RespFrame) using Framed, it uses the Encoder implementation of the codec to serialize the frame into raw bytes.
    // These bytes are then written to the underlying stream.

    // By default, Framed's second parameter is a codec type that implements both the Decoder and Encoder traits.
    // This codec is responsible for decoding incoming data into structured frames and encoding outgoing frames into raw bytes.

    // The functionality of Framed is both a parser and a converter, depending on the context in which it is used.
    // It acts as a high-level abstraction for handling streams of data by combining a transport layer (e.g., TcpStream) with a codec (e.g., RespFrameCodec) to handle decoding (parsing) and encoding (converting).
    let mut framed = Framed::new(stream, RespFrameCodec); // The term codec is short for "coder-decoder"
    loop {
        // Uses framed.next().await to read the next frame from the client.
        match framed.next().await {
            // If a frame is received:
            // Logs the frame.
            // Creates a RedisRequest with the frame and backend.
            // Passes the request to request_handler to process it.
            // Sends the response back to the client.
            Some(Ok(frame)) => {
                info!("Received frame: {:?}", frame);
                let request = RedisRequest {
                    frame,
                    backend: backend.clone(),
                };
                let response = request_handler(request).await?;
                info!("Sending response: {:?}", response.frame);
                framed.send(response.frame).await?; // to send the response back to the client.
            }
            Some(Err(e)) => return Err(e),
            None => return Ok(()), // If the stream ends (None), exits the loop.
        }
    }
}

// Processes a single client request.
// Converts the RESP frame into a Command, executes it, and generates a response.

// Yes, that's correct! In the request_handler function,
// the execution flow first calls TryFrom to parse the raw RESP frame into a structured Command,
// and then it calls CommandExecutor to execute the parsed command.

async fn request_handler(request: RedisRequest) -> Result<RedisResponse> {
    let (frame, backend) = (request.frame, request.backend);
    let cmd = Command::try_from(frame)?;
    info!("Executing command: {:?}", cmd);
    let frame = cmd.execute(&backend);
    Ok(RedisResponse { frame })
}

// Implements encoding and decoding for RESP frames.
// Used by tokio_util::codec::Framed to handle streams of RESP frames.

// Encoder Implementation:
// Converts a RespFrame into bytes and writes them to the destination buffer (dst).
// Uses RespFrame::encode() to serialize the frame.

// The impl Encoder<RespFrame> for RespFrameCodec implementation is called internally by the Framed utility when you send a frame using the framed.send() method.
// Specifically, it is invoked whenever you need to encode a RespFrame into raw bytes to send it over the network.
// Where is it Called in Your Code?
// In your stream_handler function, the Encoder implementation is called here:

// framed.send(response.frame).await?;

// What Happens Here?
// framed.send(response.frame):

// This method is provided by the SinkExt trait (from the futures crate).
// It takes a RespFrame (the structured frame) and passes it to the encode method of the RespFrameCodec.
// RespFrameCodec::encode:

// The encode method serializes the RespFrame into raw bytes.
// These bytes are then written to the underlying TcpStream by the Framed utility.
impl Encoder<RespFrame> for RespFrameCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: RespFrame, dst: &mut bytes::BytesMut) -> Result<()> {
        let encoded = item.encode();
        dst.extend_from_slice(&encoded);
        Ok(())
    }
}

// Decoder Implementation:
// Converts bytes from the source buffer (src) into a RespFrame.
// Uses RespFrame::decode() to deserialize the frame.
// Handles incomplete frames by returning Ok(None).

// the impl Decoder<RespFrame> for RespFrameCodec implementation is called internally by the Framed utility when you attempt to read the next frame from the stream using the framed.next().await method. Specifically,
// it is invoked whenever you need to decode raw bytes from the stream into a structured RespFrame.

// What Happens Here?
// framed.next().await:

// This method is provided by the StreamExt trait (from the tokio-stream crate).
// It reads raw bytes from the underlying TcpStream and passes them to the decode method of the RespFrameCodec.
// RespFrameCodec::decode:

// The decode method attempts to parse the raw bytes into a RespFrame.
// If a complete frame is found, it returns Ok(Some(frame)).
// If the frame is incomplete, it returns Ok(None) and waits for more data.
// If there is an error during decoding, it returns Err(e).
impl Decoder for RespFrameCodec {
    type Item = RespFrame;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<RespFrame>> {
        match RespFrame::decode(src) {
            Ok(frame) => Ok(Some(frame)),
            Err(RespError::NotComplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

// Example Interaction

// Client Sends a Command:
// *2\r\n$3\r\nGET\r\n$5\r\nhello\r\n

// Flow:
// stream_handler reads the frame and logs it.
// request_handler parses the frame into a Command::Get { key: "hello" }.
// The backend is queried for the key "hello".
// A response frame is generated (e.g., $5\r\nworld\r\n).
// stream_handler sends the response back to the client.

// This modular design ensures that the server can handle multiple clients concurrently, process commands efficiently, and maintain clean separation of concerns.

// The magic of Framed lies in its ability to simplify the handling of streaming data by combining a transport layer (e.g., TcpStream) with a codec (e.g., RespFrameCodec) for encoding and decoding messages. It abstracts away the complexity of manually managing byte streams, allowing you to focus on higher-level logic like processing commands and sending responses.

// What is Framed?
// Framed is a utility provided by the tokio-util crate. It wraps a stream (e.g., TcpStream) and uses a codec (e.g., RespFrameCodec) to:

// Decode incoming byte streams into structured frames (e.g., RespFrame).
// Encode structured frames into byte streams for outgoing data.
// How Framed Works
// 1. Input (Decoding)
// Framed reads raw bytes from the underlying stream (e.g., TcpStream).
// It uses the Decoder implementation of the codec (e.g., RespFrameCodec) to convert the raw bytes into structured frames (e.g., RespFrame).
// 2. Output (Encoding)
// When you send a frame (e.g., RespFrame) using Framed, it uses the Encoder implementation of the codec to serialize the frame into raw bytes.
// These bytes are then written to the underlying stream.
// Why Use Framed?
// Without Framed
// If you were to handle the stream manually:

// You would need to read raw bytes from the stream.
// You would need to implement custom logic to:
// Detect message boundaries.
// Handle incomplete messages.
// Parse the bytes into structured data.
// You would need to serialize structured data back into bytes for outgoing messages.
// This is error-prone and tedious.

// With Framed
// Framed handles all of this for you:

// It reads raw bytes from the stream and decodes them into structured frames.
// It encodes structured frames into raw bytes and writes them to the stream.
// It abstracts away the complexity of managing byte streams and message boundaries.

// End-to-End Flow with Framed
// Client Sends Data:
// The client sends a RESP-encoded command (e.g., *2\r\n$3\r\nGET\r\n$5\r\nhello\r\n).

// Framed Reads Data:
// Framed reads raw bytes from the TcpStream.
// The Decoder implementation of RespFrameCodec parses the bytes into a RespFrame.

// Process the Frame:
// The stream_handler function processes the RespFrame (e.g., parses it into a Command and executes it).

// Send a Response:
// The stream_handler function sends a RespFrame response back to the client.
// The Encoder implementation of RespFrameCodec serializes the RespFrame into bytes.
// Framed writes the bytes to the TcpStream.
