use std::path::Path;

use crate::fetch::{Arguments, Error, Shallow};

#[cfg(feature = "blocking-client")]
mod blocking_io;
#[cfg(feature = "blocking-client")]
pub use blocking_io::fetch_blocking;

#[cfg(feature = "async-client")]
mod async_io;
#[cfg(feature = "async-client")]
pub use async_io::fetch_async;

fn acquire_shallow_lock(shallow_file: &Path) -> Result<gix_lock::File, Error> {
    gix_lock::File::acquire_to_update_resource(shallow_file, gix_lock::acquire::Fail::Immediately, None)
        .map_err(Into::into)
}

fn add_shallow_args(
    args: &mut Arguments,
    shallow: &Shallow,
    shallow_file: &std::path::Path,
) -> Result<(Option<nonempty::NonEmpty<gix_hash::ObjectId>>, Option<gix_lock::File>), Error> {
    let expect_change = *shallow != Shallow::NoChange;
    let shallow_lock = expect_change.then(|| acquire_shallow_lock(shallow_file)).transpose()?;

    let shallow_commits = gix_shallow::read(shallow_file)?;
    if (shallow_commits.is_some() || expect_change) && !args.can_use_shallow() {
        // NOTE: if this is an issue, we can always unshallow the repo ourselves.
        return Err(Error::MissingServerFeature {
            feature: "shallow",
            description: "shallow clones need server support to remain shallow, otherwise bigger than expected packs are sent effectively unshallowing the repository",
        });
    }
    if let Some(shallow_commits) = &shallow_commits {
        for commit in shallow_commits.iter() {
            args.shallow(commit);
        }
    }
    match shallow {
        Shallow::NoChange => {}
        Shallow::DepthAtRemote(commits) => args.deepen(commits.get() as usize),
        Shallow::Deepen(commits) => {
            args.deepen(*commits as usize);
            args.deepen_relative();
        }
        Shallow::Since { cutoff } => {
            args.deepen_since(cutoff.seconds);
        }
        Shallow::Exclude {
            remote_refs,
            since_cutoff,
        } => {
            if let Some(cutoff) = since_cutoff {
                args.deepen_since(cutoff.seconds);
            }
            for ref_ in remote_refs {
                args.deepen_not(ref_.as_ref().as_bstr());
            }
        }
    }
    Ok((shallow_commits, shallow_lock))
}
