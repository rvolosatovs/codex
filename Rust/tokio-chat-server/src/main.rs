use std::env;
#[cfg(unix)]
use std::os::unix::io::FromRawFd;
#[cfg(target_os = "wasi")]
use std::os::wasi::io::FromRawFd;
use std::str::FromStr;

use anyhow::{bail, Context};
use futures::{select, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::{BroadcastStream, LinesStream, TcpListenerStream};
use ulid::Ulid;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let fd_count = env::var("FD_COUNT").context("failed to lookup `FD_COUNT`")?;
    let fd_count = usize::from_str(&fd_count).context("failed to parse `FD_COUNT`")?;
    assert_eq!(
        fd_count,
        4, // STDIN, STDOUT, STDERR and a socket
        "unexpected amount of file descriptors received"
    );
    let listener = match env::var("FD_NAMES")
        .context("failed to lookup `FD_NAMES`")?
        .splitn(fd_count, ':')
        .nth(3)
    {
        None => bail!("failed to parse `FD_NAMES`"),
        Some("ingest") => {
            let l = unsafe { std::net::TcpListener::from_raw_fd(3) };
            l.set_nonblocking(true)
                .context("failed to set non-blocking flag on ingest socket")?;
            TcpListener::from_std(l)
                .context("failed to initialize Tokio listener")
                .map(TcpListenerStream::new)?
        }
        Some(name) => bail!("unknown socket name `{name}`"),
    };

    let (tx, _) = broadcast::channel(128);
    listener
        .for_each_concurrent(None, |peer| async {
            match peer {
                Err(e) => {
                    eprintln!("Failed to accept connection: {e}");
                }
                Ok(mut peer) => {
                    let id = Ulid::new();

                    eprintln!("{id} peer joined");
                    let (r, mut w) = peer.split();
                    let mut input = LinesStream::new(BufReader::new(r).lines()).fuse();
                    let mut rx = BroadcastStream::new(tx.subscribe()).fuse();

                    if let Err(e) = tx.send(format!("{id} joined the chat")) {
                        eprintln!("failed to send {id} join event to peers: {e}");
                        return
                    }
                    loop {
                        select! {
                            line = input.next() => {
                                if let Some(line) = line {
                                    match line {
                                        Err(e) => {
                                            eprintln!("failed to receive line from {id}: {e}");
                                            break
                                        }
                                        Ok(line) => {
                                            let line = format!("{id}: {line}");
                                            eprintln!("> {line}");
                                            if let Err(e) = tx.send(line) {
                                                eprintln!("failed to send line from {id}: {e}");
                                                break;
                                            }
                                        },
                                    }
                                } else {
                                    break
                                }
                            }
                            line = rx.next() => {
                                if let Some(line) = line {
                                    match line {
                                        Err(e) => {
                                            eprintln!("failed to receive line for {id}: {e}");
                                            break
                                        }
                                        Ok(line) => if let Err(e) = w.write_all(line.as_bytes()).await {
                                            eprintln!("failed to send line to {id}: {e}");
                                            break;
                                        },
                                    }
                                } else {
                                    break
                                }
                            },
                            complete => break,
                        };
                    }

                    eprintln!("{id} peer left");
                    if let Err(e) = tx.send(format!("{id} left the chat")) {
                        eprintln!("failed to send {id} leave event to peers: {e}")
                    }
                }
            };
        })
        .await;
    Ok(())
}
