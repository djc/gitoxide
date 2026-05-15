/// A function that performs a given credential action, trying to obtain credentials for an operation that needs it.
pub type AuthenticateFn<'a> = Box<dyn FnMut(gix_credentials::helper::Action) -> gix_credentials::protocol::Result + 'a>;

///
#[cfg(feature = "blocking-network-client")]
pub mod blocking_io;

///
#[cfg(feature = "async-network-client")]
pub mod async_io;

///
pub mod fetch;

///
pub mod ref_map;
