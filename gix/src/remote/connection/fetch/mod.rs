use crate::{bstr::BString, remote::fetch::RefMap};

mod error;
pub use error::Error;

/// The way reflog messages should be composed whenever a ref is written with recent objects from a remote.
pub enum RefLogMessage {
    /// Prefix the log with `action` and generate the typical suffix as `git` would.
    Prefixed {
        /// The action to use, like `fetch` or `pull`.
        action: String,
    },
    /// Control the entire message, using `message` verbatim.
    Override {
        /// The complete reflog message.
        message: BString,
    },
}

impl RefLogMessage {
    pub(crate) fn compose(&self, context: &str) -> BString {
        match self {
            RefLogMessage::Prefixed { action } => format!("{action}: {context}").into(),
            RefLogMessage::Override { message } => message.to_owned(),
        }
    }
}

/// The status of the repository after the fetch operation
#[derive(Debug, Clone)]
pub enum Status {
    /// Nothing changed as the remote didn't have anything new compared to our tracking branches, thus no pack was received
    /// and no new object was added.
    ///
    /// As we could determine that nothing changed without remote interaction, there was no negotiation at all.
    NoPackReceived {
        /// If `true`, we didn't receive a pack due to dry-run mode being enabled.
        dry_run: bool,
        /// Information about the pack negotiation phase if negotiation happened at all.
        ///
        /// It's possible that negotiation didn't have to happen as no reference of interest changed on the server.
        negotiate: Option<outcome::Negotiate>,
        /// However, depending on the refspecs, references might have been updated nonetheless to point to objects as
        /// reported by the remote.
        update_refs: refs::update::Outcome,
    },
    /// There was at least one tip with a new object which we received.
    Change {
        /// Information about the pack negotiation phase.
        negotiate: outcome::Negotiate,
        /// Information collected while writing the pack and its index.
        write_pack_bundle: gix_pack::bundle::write::Outcome,
        /// Information collected while updating references.
        update_refs: refs::update::Outcome,
    },
}

/// The outcome of receiving a pack via [`Prepare::receive()`](crate::remote::blocking_io::Prepare::receive).
#[derive(Debug, Clone)]
pub struct Outcome {
    /// The result of the initial mapping of references, the prerequisite for any fetch.
    pub ref_map: RefMap,
    /// The outcome of the handshake with the server.
    pub handshake: gix_protocol::Handshake,
    /// The status of the operation to indicate what happened.
    pub status: Status,
}

/// Additional types related to the outcome of a fetch operation.
pub mod outcome {
    /// Information about the negotiation phase of a fetch.
    ///
    /// Note that negotiation can happen even if no pack is ultimately produced.
    #[derive(Default, Debug, Clone)]
    pub struct Negotiate {
        /// The negotiation graph indicating what kind of information 'the algorithm' collected in the end.
        pub graph: gix_negotiate::IdMap,
        /// Additional information for each round of negotiation.
        pub rounds: Vec<gix_protocol::fetch::negotiate::Round>,
    }
}

pub use gix_protocol::fetch::ProgressId;

///
pub mod prepare {
    /// The error returned by `prepare_fetch()`.
    #[derive(Debug, thiserror::Error)]
    #[allow(missing_docs)]
    pub enum Error {
        #[error("Cannot perform a meaningful fetch operation without any configured ref-specs")]
        MissingRefSpecs,
        #[error(transparent)]
        RefMap(#[from] crate::remote::ref_map::Error),
    }

    impl gix_protocol::transport::IsSpuriousError for Error {
        fn is_spurious(&self) -> bool {
            match self {
                Error::RefMap(err) => err.is_spurious(),
                _ => false,
            }
        }
    }
}

pub(crate) mod config;
///
#[path = "update_refs/mod.rs"]
pub mod refs;
