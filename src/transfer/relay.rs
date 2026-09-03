//! Per-attempt TCP relay splicing the zfs send stream into the zfs recv stream.
//!
//! Both zfs-localpv node agents are outbound `nc` clients: the source connects
//! to `backupDest` and writes the send stream; the target connects to
//! `restoreSrc` and reads it. We listen on two ports (one per role — a single
//! port could not tell sender from receiver apart), pin each to the expected
//! node's IPs, accept exactly one matching connection each, and copy bytes.
//!
//! Both agents run `nc -w 3`, and OpenBSD netcat's `-w` is an *idle* timeout:
//! any 3 s window with no traffic on a connection closes it, on either side.
//! So the target must not be connected before the source has bytes to give
//! it (the state machine creates the ZFSRestore only after
//! `source_connected`), and the source must never be left unread while the
//! target is still on its way: the source is pumped into a bounded buffer as
//! soon as it connects, and the buffer drains into the target once it arrives.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// A network in CIDR terms; hosts are a /32 (or /128). Local, tiny — not
/// worth a dependency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IpNet {
    pub addr: IpAddr,
    pub prefix: u8,
}

impl IpNet {
    pub fn host(addr: IpAddr) -> IpNet {
        let prefix = if addr.is_ipv4() { 32 } else { 128 };
        IpNet { addr, prefix }
    }

    pub fn parse_cidr(s: &str) -> Option<IpNet> {
        let (ip, len) = s.split_once('/')?;
        let addr: IpAddr = ip.trim().parse().ok()?;
        let prefix: u8 = len.trim().parse().ok()?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        (prefix <= max).then_some(IpNet { addr, prefix })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let mask = if self.prefix == 0 { 0 } else { u32::MAX << (32 - self.prefix as u32) };
                u32::from(net) & mask == u32::from(ip) & mask
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let mask = if self.prefix == 0 { 0 } else { u128::MAX << (128 - self.prefix as u32) };
                u128::from(net) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    }
}

pub struct Relay {
    pub backup_port: u16,
    pub restore_port: u16,
    /// Total bytes spliced so far.
    pub bytes: Arc<AtomicU64>,
    /// Unix seconds of last observed progress (accept or data), for stall detection.
    pub last_activity: Arc<AtomicU64>,
    /// Set once the source agent's connection has been accepted — the cue
    /// to create the ZFSRestore so the target connects to a stream that is
    /// already flowing.
    pub source_connected: Arc<AtomicBool>,
    pub task: JoinHandle<Result<u64>>,
}

/// Chunk size read from the source per syscall.
const CHUNK: usize = 256 * 1024;
/// How much of the send stream may be held in memory while the target has
/// not connected yet (chunks × CHUNK = 64 MiB). Keep chunks × CHUNK × the
/// per-node concurrency ceiling (floor(nodes/2) concurrent streams) inside
/// the deployment memory limit (256Mi in brick) with headroom;
/// at direct-path speeds this covers several seconds of source flow, which
/// is far longer than a ZFSRestore takes to turn into a connection.
const BUFFER_CHUNKS: usize = 256;

impl Relay {
    /// Bind both listeners (ephemeral ports) and start the splice task.
    /// `backup_peers`/`restore_peers` are the source/target node IPs allowed
    /// to connect to the respective port.
    pub async fn spawn(backup_peers: Vec<IpNet>, restore_peers: Vec<IpNet>) -> Result<Relay> {
        let backup_listener = TcpListener::bind(("0.0.0.0", 0))
            .await
            .context("bind backup listener")?;
        let restore_listener = TcpListener::bind(("0.0.0.0", 0))
            .await
            .context("bind restore listener")?;
        let backup_port = backup_listener.local_addr()?.port();
        let restore_port = restore_listener.local_addr()?.port();

        let bytes = Arc::new(AtomicU64::new(0));
        let last_activity = Arc::new(AtomicU64::new(now_secs()));
        let source_connected = Arc::new(AtomicBool::new(false));

        let b = bytes.clone();
        let la = last_activity.clone();
        let sc = source_connected.clone();
        let task = tokio::spawn(async move {
            let mut src =
                accept_from(backup_listener, &backup_peers, "backup/source", &la).await?;
            sc.store(true, Ordering::Release);
            // Pump the source into a bounded queue right away so its `nc`
            // never sees an idle socket; the target side drains the queue
            // once it connects. Backpressure past BUFFER_CHUNKS stalls the
            // reader, and the source's own idle timeout then bounds how long
            // a target can take to appear.
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(BUFFER_CHUNKS);
            let la_r = la.clone();
            // Aborting the relay task (drop_relay) must take the reader
            // with it, or it would keep the source socket open until the
            // next read returned.
            let mut reader = AbortOnDrop(tokio::spawn(async move {
                let mut buf = vec![0u8; CHUNK];
                loop {
                    let n = src.read(&mut buf).await.context("relay read")?;
                    if n == 0 {
                        return Ok::<(), anyhow::Error>(());
                    }
                    la_r.store(now_secs(), Ordering::Relaxed);
                    if tx.send(buf[..n].to_vec()).await.is_err() {
                        bail!("relay writer gone");
                    }
                }
            }));
            let mut dst =
                accept_from(restore_listener, &restore_peers, "restore/target", &la).await?;
            let mut total: u64 = 0;
            while let Some(chunk) = rx.recv().await {
                dst.write_all(&chunk).await.context("relay write")?;
                total += chunk.len() as u64;
                b.store(total, Ordering::Relaxed);
                la.store(now_secs(), Ordering::Relaxed);
            }
            // The queue closed: the reader finished (EOF) or failed.
            (&mut reader.0).await.context("relay reader task")??;
            dst.shutdown().await.ok();
            if total == 0 {
                bail!("relay stream closed with zero bytes transferred");
            }
            tracing::info!(total, "relay stream complete");
            Ok(total)
        });

        Ok(Relay {
            backup_port,
            restore_port,
            bytes,
            last_activity,
            source_connected,
            task,
        })
    }
}

struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Accept a single connection from one of the allowed peers; connections from
/// other addresses are dropped and we keep listening (the overall attempt
/// timeout in the state machine bounds this). The listener is closed as soon
/// as the matching peer connects (single-accept).
async fn accept_from(
    listener: TcpListener,
    allowed: &[IpNet],
    role: &str,
    last_activity: &AtomicU64,
) -> Result<TcpStream> {
    loop {
        let (stream, peer): (TcpStream, SocketAddr) = listener.accept().await?;
        // An empty allowlist rejects everyone (fail closed) — callers must
        // resolve real peer addresses before spawning the relay.
        if allowed.iter().any(|net| net.contains(peer.ip())) {
            tracing::info!(%peer, role, "relay accepted connection");
            last_activity.store(now_secs(), Ordering::Relaxed);
            return Ok(stream);
        }
        tracing::warn!(%peer, role, "relay rejected connection from unexpected peer");
        drop(stream);
    }
}

