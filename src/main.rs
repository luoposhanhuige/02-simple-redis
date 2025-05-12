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
// 核心：redis-cli 是安装 官方 redisserver 之后的一个命令行工具，其能够在交互中，把用户的输入 encode 成 redis 协议的格式，类似 *2\r\n$3\r\nGET\r\n$5\r\nhello\r\n，
// 然后，将其发送到 redis server；
// redis server 或者你自己编写的 SIMPLE_REDIS 解析这个协议，即 decode，并执行相应的命令，最后将结果通过 redis-cli 直接输出给用户终端。

// 127.0.0.1:6379> set hello world
// OK
// 127.0.0.1:6379> get hello
// "world"
// 127.0.0.1:6379> hgetall
// Error: Server closed the connection
// not connected> hset map hello world
// OK
// 127.0.0.1:6379> hset map goodbye lin
// OK
// 127.0.0.1:6379> hgetall map
// 1# goodbye => "lin"
// 2# hello => "world"

// 为 HGetAll 添加了 sort 属性，
// 并修改了 impl CommandExecutor for HGetAll 代码，增加排序逻辑
// hgetall map 输出排序过的结果
// ❯ redis-cli
// 127.0.0.1:6379> set hello world
// OK
// 127.0.0.1:6379> get hello
// "world"
// 127.0.0.1:6379> hset map hello world
// OK
// 127.0.0.1:6379> hset map goodbye linxh
// OK
// 127.0.0.1:6379> hgetall map
// 1) "hello"
// 2) "world"
// 3) "goodbye"
// 4) "linxh"
// 127.0.0.1:6379>

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
