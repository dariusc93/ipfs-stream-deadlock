use std::time::Duration;
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use rand::RngCore;
use rust_ipfs::{Ipfs, Multiaddr, PeerId, Protocol, UninitializedIpfsDefault as UninitializedIpfs};
use rust_ipfs::libp2p::StreamProtocol;
use rust_ipfs::p2p::TransportConfig;

const ECHO_PROTOCOL: StreamProtocol = StreamProtocol::new("/ipfs/echo/0.0.0");

// Change to 512 * 1024 to deadlock
const DEFAULT_BUFFER_SIZE: usize = 128 * 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ipfs_a = UninitializedIpfs::new()
        .set_transport_configuration(TransportConfig { enable_memory_transport: true, ..Default::default()})
        .add_listening_addr(Multiaddr::empty().with(Protocol::Memory(0)))
        .with_streams()
        .start()
        .await?;

    let ipfs_b = UninitializedIpfs::new()
        .set_transport_configuration(TransportConfig { enable_memory_transport: true, ..Default::default()})
        .add_listening_addr(Multiaddr::empty().with(Protocol::Memory(0)))
        .with_streams()
        .start()
        .await?;

    let peer_id_a = ipfs_a.keypair().public().to_peer_id();
    let peer_id_b = ipfs_b.keypair().public().to_peer_id();

    let addrs_a = ipfs_a.listening_addresses().await?;
    let addrs_b = ipfs_b.listening_addresses().await?;


    for addr in addrs_a {
        ipfs_b.add_peer((peer_id_a, addr)).await?;
    }

    for addr in addrs_b {
        ipfs_a.add_peer((peer_id_b, addr)).await?;
    }

    initialize_stream(&ipfs_a).await;
    initialize_stream(&ipfs_b).await;


    tokio::spawn(connection_handler(peer_id_b, ipfs_a));
    // tokio::spawn(connection_handler(peer_id_a, ipfs_b));

    tokio::signal::ctrl_c().await?;

    Ok(())
}

async fn initialize_stream(ipfs: &Ipfs) {
    let mut incoming_streams = ipfs.new_stream(ECHO_PROTOCOL).await.expect("failed to create ipfs stream");

    tokio::spawn(async move {
        while let Some((peer, stream)) = incoming_streams.next().await {
            match echo(stream).await {
                Ok(n) => {
                    println!("{peer} - Echoed {n} bytes!");
                }
                Err(e) => {
                    println!("{peer} - Echo failed: {e}");
                    continue;
                }
            };
        }
    });
}

async fn connection_handler(peer: PeerId, ipfs: Ipfs) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let stream = match ipfs.open_stream(peer, ECHO_PROTOCOL).await {
            Ok(stream) => stream,
            Err(error) => {
                println!("{peer} - {error}");
                continue;
            }
        };

        if let Err(e) = send(stream).await {
            println!("{peer} - Echo protocol failed: {e}");
            continue;
        }

        println!("{peer} - Echo complete!")
    }
}

async fn echo(mut stream: rust_ipfs::libp2p::Stream) -> std::io::Result<usize> {
    let mut total = 0;

    let mut buf = [0u8; DEFAULT_BUFFER_SIZE];

    loop {
        let read = stream.read(&mut buf).await?;
        if read == 0 {
            return Ok(total);
        }

        total += read;
        stream.write_all(&buf[..read]).await?;
    }
}

async fn send(mut stream: rust_ipfs::libp2p::Stream) -> std::io::Result<()> {
    let mut bytes = vec![0; DEFAULT_BUFFER_SIZE];
    rand::thread_rng().fill_bytes(&mut bytes);

    stream.write_all(&bytes).await?;

    let mut buf = vec![0; DEFAULT_BUFFER_SIZE];
    stream.read_exact(&mut buf).await?;

    if bytes != buf {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "incorrect echo",
        ));
    }

    stream.close().await?;

    Ok(())
}
