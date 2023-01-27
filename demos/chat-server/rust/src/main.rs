use std::error::Error;
use std::fmt::Display;
use std::fs::{self, File};
use std::net::SocketAddr;
#[cfg(target_os = "wasi")]
use std::os::wasi::prelude::OwnedFd;

use anyhow::{bail, Context};
use futures::{join, select, Stream, StreamExt};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tokio_stream::wrappers::{BroadcastStream, LinesStream, TcpListenerStream};
use ulid::Ulid;

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
pub struct AcceptMetadata {
    pub addr: SocketAddr,
    pub peer: Peer,
}

/// Handles the peer TCP stream I/O.
async fn handle_io(
    peer: &mut TcpStream,
    tx: &Sender<String>,
    rx: impl Stream<Item = Result<String, impl Error>> + Unpin,
    id: impl Display,
) -> anyhow::Result<()> {
    let (input, mut output) = peer.split();
    let mut input = LinesStream::new(BufReader::new(input).lines()).fuse();
    let mut rx = rx.fuse();
    loop {
        select! {
            line = input.next() => match line {
                None => return Ok(()),
                Some(Err(e)) => bail!("failed to receive line from {id}: {e}"),
                Some(Ok(line)) => if let Err(e) = tx.send(format!("{id}: {line}")) {
                    bail!("failed to send line from {id}: {e}")
                },
            },
            line = rx.next() => match line {
                None => return Ok(()),
                Some(Err(e)) => bail!("failed to receive line for {id}: {e}"),
                Some(Ok(line)) => if let Err(e) = output.write_all(format!("{line}\n").as_bytes()).await {
                    bail!("failed to send line to {id}: {e}")
                },
            },
            complete => return Ok(()),
        };
    }
}

/// Handles the peer TCP stream.
async fn handle(stream: &mut TcpStream, peer: Peer, tx: &Sender<String>) -> anyhow::Result<()> {
    let rx = BroadcastStream::new(tx.subscribe());

    let id = match peer {
        Peer::Anonymous => {
            bail!("client did not present a valid certificate, refuse connection")
        }
        Peer::Local { digest } => bail!("client is running a local workload with digest `{digest}`, abort connection"),
        Peer::Keep { workload, digest } => match workload.split_once(':') {
            Some(("store.rvolosatovs.dev/chat/client", "0.1.0" | "0.1.1" | "0.1.2")) => {
                let id = Ulid::new();
                tx.send(format!(
                    "`{id}` joined the chat running `{workload}` with digest `{digest}`"
                ))
                .with_context(|| format!("failed to send `{id}` join event to peers"))?;
                id
            }
            Some(("store.rvolosatovs.dev/chat/client", version)) => bail!("client is running unexpected version `{version}` of `rvolosatovs/chat-client`, refuse connection"),
            _ => bail!("client is running unexpected workload `{workload}`, refuse connection"),
        },
    };

    _ = handle_io(stream, tx, rx, id)
        .await
        .with_context(|| format!("failed to handle `{id}` peer I/O"))?;

    tx.send(format!("`{id}` left the chat"))
        .map(|_| ())
        .with_context(|| format!("failed to send `{id}` leave event to peers"))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let listener = File::options()
        .read(true)
        .write(true)
        .open("/net/lis/50000")
        .map(OwnedFd::from)
        .map(std::net::TcpListener::from)
        .context("failed to listen on port 50000")?;
    listener
        .set_nonblocking(true)
        .context("failed to set non-blocking flag on socket")?;
    let listener = TcpListener::from_std(listener)
        .context("failed to initialize Tokio listener")
        .map(TcpListenerStream::new)?;

    let (tx, rx) = broadcast::channel(128);
    join!(
        BroadcastStream::new(rx).for_each(|line| async {
            match line {
                Err(e) => eprintln!("failed to receive line: {e}"),
                Ok(line) => println!("> {line}"),
            }
        }),
        listener.for_each_concurrent(None, |stream| async {
            match stream {
                Err(e) => eprintln!("failed to accept connection: {e}"),
                Ok(mut stream) => {
                    let md =
                        fs::read_to_string("/net/peer/lis").expect("failed to query peer metadata");
                    let AcceptMetadata { addr, peer } =
                        serde_json::from_str(&md).expect("failed to decode peer metadata");
                    let _ = handle(&mut stream, peer, &tx).await.map_err(|e| {
                        eprintln!("failed to handle client connection from `{addr}`: {e}")
                    });
                }
            };
        })
    );
    Ok(())
}
