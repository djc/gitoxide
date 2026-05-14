#[cfg(feature = "blocking-client")]
mod blocking_io;
#[cfg(feature = "blocking-client")]
pub use blocking_io::handshake_blocking;

#[cfg(feature = "async-client")]
mod async_io;
#[cfg(feature = "async-client")]
pub use async_io::handshake_async;
