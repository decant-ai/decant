//! Process control only. No clocks or signals enter deterministic state.
#[cfg(feature = "full")]
use std::{
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[cfg(feature = "full")]
pub(crate) const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

// A Tokio Stdout blocking writer can hold Rust's global stdout lock forever
// when the client stops reading. Exit cleanup also needs that lock. Duplicate
// the descriptor safely, keeping protocol writes out of the global buffer.
#[cfg(unix)]
pub(crate) fn stdout() -> std::io::Result<tokio::fs::File> {
    use std::os::fd::AsFd;
    let descriptor = std::io::stdout().as_fd().try_clone_to_owned()?;
    let mut output = tokio::fs::File::from_std(std::fs::File::from(descriptor));
    output.set_max_buf_size(128 * 1024 + 1);
    Ok(output)
}
#[cfg(not(unix))]
pub(crate) fn stdout() -> std::io::Result<tokio::io::Stdout> {
    Ok(tokio::io::stdout())
}

pub(crate) struct Shutdown {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}
impl Shutdown {
    pub(crate) fn new() -> std::io::Result<Self> {
        Ok(Self {
            #[cfg(unix)]
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            #[cfg(unix)]
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }
    pub(crate) async fn wait(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            let result = tokio::select! {
                result = self.interrupt.recv() => result,
                result = self.terminate.recv() => result,
            };
            result.ok_or_else(|| std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await
        }
    }
}

#[cfg(feature = "full")]
pub(crate) fn join<T>(worker: JoinHandle<T>, deadline: Duration) -> Result<T, ()> {
    let started = Instant::now();
    while !worker.is_finished() {
        if started.elapsed() >= deadline {
            return Err(());
        }
        thread::sleep(Duration::from_millis(10).min(deadline.saturating_sub(started.elapsed())));
    }
    worker.join().map_err(|_| ())
}

#[cfg(all(test, feature = "full"))]
mod tests {
    use super::*;
    #[test]
    fn worker_join_is_bounded_without_claiming_to_cancel_io() {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<()>(0);
        let worker = thread::spawn(move || {
            let _ = receiver.recv();
        });
        assert!(join(worker, Duration::ZERO).is_err());
        drop(sender); // release our own test worker; no leaked blocking thread
        assert_eq!(join(thread::spawn(|| 7), Duration::from_secs(1)), Ok(7));
    }
}
