use std::fs::{self, File};
#[cfg(target_os = "wasi")]
use std::os::wasi::prelude::OwnedFd;
use std::time::Duration;

use anyhow::{bail, Context};
use futures::{StreamExt, TryStreamExt};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_stream::wrappers::LinesStream;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind")]
pub enum Peer {
    #[serde(rename = "anonymous")]
    Anonymous,
    #[serde(rename = "local")]
    Local { digest: String },
    #[serde(rename = "keep")]
    Keep { workload: String, digest: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ConnectMetadata {
    pub peer: Peer,
}

async fn connect(addr: &str) -> std::net::TcpStream {
    let path = format!("/net/con/{addr}");
    loop {
        match File::options().read(true).write(true).open(&path) {
            Ok(stream) => return std::net::TcpStream::from(OwnedFd::from(stream)),
            Err(e) => {
                eprintln!(r#"Failed to connect to `{addr}`: {e}
Retrying in 1s"#);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut stdin = std::io::stdin().lines();

    eprint!("Server address: ");
    let addr = stdin
        .next()
        .context("server address must be specified")?
        .context("failed to read server address")?;

    let stream = connect(addr.trim()).await;
    let md = fs::read_to_string("/net/peer/con").context("failed to query peer metadata")?;
    let ConnectMetadata { peer } =
        serde_json::from_str(&md).context("failed to parse connection metadata")?;
    match peer {
        Peer::Anonymous => bail!("`{addr}` did not present a valid certificate, abort connection"),
        Peer::Local { digest } => match digest.as_str() {
              "Z9op739CbE+YbFZXv0XAXtlHc0Gayi1obyVy/Vpkxtc" // store.rvolosatovs.dev/chat/server:0.1.0
            | "dbObfl9U9XYGgD6rNxznJqZ2Ngt0C2gNowKKX/eoweE" // store.rvolosatovs.dev/chat/server:0.1.1
            | "W6QlNli9j5BWWl4OPE/L2jJz4g3dO/T3m3kDkx4dPiY"  // store.rvolosatovs.dev/chat/server:0.1.2
            => {
                eprintln!("Connected to `{addr}` running local workload with trusted digest `{digest}`")
            },
            digest => bail!("`{addr}` is running a local workload with unknown digest `{digest}`, refuse connection"),
        },
        Peer::Keep { workload, digest } => match workload.split_once(':') {
            Some(("store.rvolosatovs.dev/chat/server", "0.1.0" | "0.1.1" | "0.1.2")) => eprintln!("Connected to `{addr}` running `{workload}` with digest `{digest}`"),
            Some(("store.rvolosatovs.dev/chat/server", version)) => bail!("`{addr}` is running unexpected version `{version}` of `rvolosatovs/chat-server` with digest `{digest}`, abort connection"),
            _ => bail!("`{addr}` is running unexpected workload `{workload}` with digest `{digest}`, abort connection")
        },
    }
    stream
        .set_nonblocking(true)
        .context("failed to set stream to non-blocking mode")?;

    let mut stream = TcpStream::from_std(stream).context("failed to initialize Tokio stream")?;

    // TODO: Send and receive multiple messages concurrently once async reads from stdin are possible
    for line in stdin {
        let line = line.context("failed to read line from STDIN")?;
        stream
            .write_all(format!("{line}\n").as_bytes())
            .await
            .context("failed to send line")?;
    }
    LinesStream::new(BufReader::new(stream).lines())
        .try_for_each(|line| async move {
            println!("{line}");
            Ok(())
        })
        .await
        .context("failed to receive line")
}
