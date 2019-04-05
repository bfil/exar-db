use std::env;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicUsize, Ordering};

static PORT: AtomicUsize = AtomicUsize::new(0);

fn base_port() -> u16 {
    let cwd = env::current_dir().unwrap();
    let dirs = ["32-opt", "32-nopt", "musl-64-opt", "cross-opt",
                "64-opt", "64-nopt", "64-opt-vg", "64-debug-opt",
                "all-opt", "snap3", "dist"];
    dirs.iter().enumerate().find(|&(_, dir)| {
        cwd.to_str().unwrap().contains(dir)
    }).map(|p| p.0).unwrap_or(0) as u16 * 1000 + 19600
}

pub fn find_available_addr() -> SocketAddr {
    let port = PORT.fetch_add(1, Ordering::SeqCst) as u16 + base_port();
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port))
}
