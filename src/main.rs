// This is the entry point of the application.
// It initializes the server, listens for incoming TCP connections, and spawns tasks to handle each connection.

use anyhow::Result;
use simple_redis::{network, Backend};
use tokio::net::TcpListener;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";
    info!("Simple-Redis-Server is listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;

    // Initializes the backend storage system (e.g., a key-value store).
    // This backend will be shared across all client connections.
    let backend = Backend::new();
    loop {
        let (stream, raddr) = listener.accept().await?;
        info!("Accepted connection from: {}", raddr);

        // Clones the backend so that it can be shared with the task handling the connection.
        // The backend is likely implemented with a thread-safe data structure like DashMap.
        let cloned_backend = backend.clone();

        // Spawns a new asynchronous task to handle the connection.
        // This allows the server to continue accepting new connections while handling existing ones.
        tokio::spawn(async move {
            // network::stream_handler:
            // Handles the logic for processing client requests over the stream.
            // Likely includes parsing commands (e.g., SET, GET) and interacting with the backend.
            match network::stream_handler(stream, cloned_backend).await {
                Ok(_) => {
                    info!("Connection from {} exited", raddr);
                }
                Err(e) => {
                    warn!("handle error for {}: {:?}", raddr, e);
                }
            }
        });
    }
}

// step 1:
// RUST_LOG=debug cargo run
// 终端将显示：INFO simple_redis: Simple-Redis-Server is listening on 0.0.0.0:6379
// 处于监听状态

// step 2:
// 打开另外一个 terminal
// redis-cli
// set hello world
// get hello

// Execution Flow Example
// Client Sends a Command

// *2\r\n$3\r\nGET\r\n$5\r\nhello\r\n

// Step-by-Step Flow
// 1, network.rs:
// Reads the raw bytes from the client.

// RespFrame::Array(vec![
//     RespFrame::BulkString("GET".into()),
//     RespFrame::BulkString("hello".into()),
// ])

// 2, cmd/mod.rs:
// Converts the RespFrame into a Command:

// Command::Get { key: "hello".to_string() }

// Executes the command using the backend and generates a response:

// RespFrame::BulkString("world".into())

// 3, network.rs:
// Encodes the RespFrame response into raw bytes:

// $5\r\nworld\r\n

// Sends the response back to the client.
