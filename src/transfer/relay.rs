//! Per-attempt TCP relay splicing the zfs send stream into the zfs recv stream.
//!
//! Both zfs-localpv node agents are outbound `nc` clients: the source connects
//! to `backupDest` and writes the send stream; the target connects to
//! `restoreSrc` and reads it. We listen on two ports (one per role — a single
//! port could not tell sender from receiver apart), pin each to the expected
//! node's IPs, accept exactly one matching connection each, and copy bytes.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
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
    pub task: JoinHandle<Result<u64>>,
}

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

        let b = bytes.clone();
        let la = last_activity.clone();
        let task = tokio::spawn(async move {
            // Accept concurrently: CR creation order doesn't guarantee which
            // agent dials first.
            let (src, dst) = tokio::try_join!(
                accept_from(backup_listener, &backup_peers, "backup/source", &la),
                accept_from(restore_listener, &restore_peers, "restore/target", &la),
            )?;
            splice(src, dst, &b, &la).await
        });

        Ok(Relay {
            backup_port,
            restore_port,
            bytes,
            last_activity,
            task,
        })
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

async fn splice(
    mut src: TcpStream,
    mut dst: TcpStream,
    bytes: &AtomicU64,
    last_activity: &AtomicU64,
) -> Result<u64> {
    let mut buf = vec![0u8; 256 * 1024];
    let mut total: u64 = 0;
    loop {
        let n = src.read(&mut buf).await.context("relay read")?;
        if n == 0 {
            break;
        }
        dst.write_all(&buf[..n]).await.context("relay write")?;
        total += n as u64;
        bytes.store(total, Ordering::Relaxed);
        last_activity.store(now_secs(), Ordering::Relaxed);
    }
    dst.shutdown().await.ok();
    if total == 0 {
        bail!("relay stream closed with zero bytes transferred");
    }
    tracing::info!(total, "relay stream complete");
    Ok(total)
}
