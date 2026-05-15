//! Async variant of [`Connection`] and [`Prepare`].

use std::{ops::DerefMut, path::PathBuf, sync::atomic::AtomicBool};

use gix_features::progress::Progress;
use gix_odb::store::RefreshMode;
use gix_protocol::{
    AsyncSendFlushOnDrop,
    fetch::{Arguments, negotiate},
};
use gix_transport::client::async_io::Transport;

use crate::{
    Remote,
    bstr::BString,
    config::{
        cache::util::ApplyLeniency,
        tree::{Clone, Fetch},
    },
    remote,
    remote::{
        Direction,
        connection::{AuthenticateFn, fetch::config},
        fetch,
        fetch::{DryRun, Error, Outcome, RefLogMessage, Status, WritePackedRefs, negotiate::Algorithm, outcome, refs},
        ref_map,
    },
};

/// A type to represent an ongoing connection to a remote host, typically with the connection already established.
///
/// It can be used to perform a variety of operations with the remote without worrying about protocol details,
/// much like a remote procedure call.
pub struct Connection<'a, 'repo, T>
where
    T: Transport,
{
    pub(crate) remote: &'a Remote<'repo>,
    pub(crate) authenticate: Option<AuthenticateFn<'a>>,
    pub(crate) transport_options: Option<Box<dyn std::any::Any>>,
    pub(crate) transport: AsyncSendFlushOnDrop<T>,
    pub(crate) handshake: Option<gix_protocol::Handshake>,
    pub(crate) trace: bool,
}

/// Builder
impl<'a, T> Connection<'a, '_, T>
where
    T: Transport,
{
    /// Set a custom credentials callback to provide credentials if the remotes require authentication.
    ///
    /// Otherwise, we will use the git configuration to perform the same task as the `git credential` helper program,
    /// which is calling other helper programs in succession while resorting to a prompt to obtain credentials from the
    /// user.
    pub fn with_credentials(
        mut self,
        helper: impl FnMut(gix_credentials::helper::Action) -> gix_credentials::protocol::Result + 'a,
    ) -> Self {
        self.authenticate = Some(Box::new(helper));
        self
    }

    /// Provide configuration to be used before the first handshake is conducted.
    pub fn with_transport_options(mut self, config: Box<dyn std::any::Any>) -> Self {
        self.transport_options = Some(config);
        self
    }
}

/// Mutation
impl<'a, T> Connection<'a, '_, T>
where
    T: Transport,
{
    /// Like [`with_credentials()`](Self::with_credentials()), but without consuming the connection.
    pub fn set_credentials(
        &mut self,
        helper: impl FnMut(gix_credentials::helper::Action) -> gix_credentials::protocol::Result + 'a,
    ) -> &mut Self {
        self.authenticate = Some(Box::new(helper));
        self
    }

    /// Like [`with_transport_options()`](Self::with_transport_options()), but without consuming the connection.
    pub fn set_transport_options(&mut self, config: Box<dyn std::any::Any>) -> &mut Self {
        self.transport_options = Some(config);
        self
    }
}

/// Access
impl<'repo, T> Connection<'_, 'repo, T>
where
    T: Transport,
{
    /// A utility to return a function that will use this repository's configuration to obtain credentials, similar to
    /// what `git credential` is doing.
    pub fn configured_credentials(
        &self,
        url: gix_url::Url,
    ) -> Result<AuthenticateFn<'static>, crate::config::credential_helpers::Error> {
        let (mut cascade, _action_with_normalized_url, prompt_opts) =
            self.remote.repo.config_snapshot().credential_helpers(url)?;
        Ok(Box::new(move |action| cascade.invoke(action, prompt_opts.clone())) as AuthenticateFn<'_>)
    }
    /// Return the underlying remote that instantiate this connection.
    pub fn remote(&self) -> &Remote<'repo> {
        self.remote
    }

    /// Provide a mutable transport to allow interacting with it according to its actual type.
    /// Note that the caller _should not_ call `configure()` as we will call it automatically before performing
    /// the handshake. Instead, to bring in custom configuration, call
    /// [`with_transport_options()`](Connection::with_transport_options()).
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport.inner
    }
}

/// Ref-map
impl<T> Connection<'_, '_, T>
where
    T: Transport,
{
    /// List all references on the remote that have been filtered through our remote's [`refspecs`][crate::Remote::refspecs()]
    /// for _fetching_.
    #[allow(clippy::result_large_err)]
    pub async fn ref_map(
        mut self,
        progress: impl Progress,
        options: ref_map::Options,
    ) -> Result<(fetch::RefMap, gix_protocol::Handshake), ref_map::Error> {
        let refmap = self.ref_map_by_ref(progress, options).await?;
        let handshake = self
            .handshake
            .expect("refmap always performs handshake and stores it if it succeeds");
        Ok((refmap, handshake))
    }

    #[allow(clippy::result_large_err)]
    pub(crate) async fn ref_map_by_ref(
        &mut self,
        mut progress: impl Progress,
        ref_map::Options {
            prefix_from_spec_as_filter_on_remote,
            handshake_parameters,
            mut extra_refspecs,
        }: ref_map::Options,
    ) -> Result<fetch::RefMap, ref_map::Error> {
        let _span = gix_trace::coarse!("remote::Connection::ref_map()");
        if let Some(tag_spec) = self.remote.fetch_tags.to_refspec().map(|spec| spec.to_owned()) {
            if !extra_refspecs.contains(&tag_spec) {
                extra_refspecs.push(tag_spec);
            }
        }
        let mut credentials_storage;
        let url = self.transport.inner.to_url();
        let authenticate = match self.authenticate.as_mut() {
            Some(f) => f,
            None => {
                let url = self.remote.url(Direction::Fetch).map_or_else(
                    || gix_url::parse(url.as_ref()).expect("valid URL to be provided by transport"),
                    ToOwned::to_owned,
                );
                credentials_storage = self.configured_credentials(url)?;
                &mut credentials_storage
            }
        };

        let repo = self.remote.repo;
        if self.transport_options.is_none() {
            self.transport_options = repo
                .transport_options(url.as_ref(), self.remote.name().map(crate::remote::Name::as_bstr))
                .map_err(|err| ref_map::Error::GatherTransportConfig {
                    source: err,
                    url: url.into_owned(),
                })?;
        }
        if let Some(config) = self.transport_options.as_ref() {
            self.transport.inner.configure(&**config)?;
        }
        let mut handshake = gix_protocol::handshake_async(
            &mut self.transport.inner,
            gix_transport::Service::UploadPack,
            authenticate,
            handshake_parameters,
            &mut progress,
        )
        .await?;

        let context = fetch::refmap::init::Context {
            fetch_refspecs: self.remote.fetch_specs.clone(),
            extra_refspecs,
        };

        let fetch_refmap = handshake.prepare_lsrefs_or_extract_refmap(
            self.remote.repo.config.user_agent_tuple(),
            prefix_from_spec_as_filter_on_remote,
            context,
        )?;

        let ref_map = fetch_refmap
            .fetch_async(progress, &mut self.transport.inner, self.trace)
            .await?;

        self.handshake = Some(handshake);
        Ok(ref_map)
    }
}

/// Prepare-fetch
impl<'remote, 'repo, T> Connection<'remote, 'repo, T>
where
    T: Transport,
{
    /// Perform a handshake with the remote and obtain a ref-map with `options`, and from there one
    /// can do a fetch operation.
    #[allow(clippy::result_large_err)]
    pub async fn prepare_fetch(
        mut self,
        progress: impl Progress,
        options: ref_map::Options,
    ) -> Result<Prepare<'remote, 'repo, T>, fetch::prepare::Error> {
        if self.remote.refspecs(remote::Direction::Fetch).is_empty() && options.extra_refspecs.is_empty() {
            return Err(fetch::prepare::Error::MissingRefSpecs);
        }
        let ref_map = self.ref_map_by_ref(progress, options).await?;
        Ok(Prepare {
            con: Some(self),
            ref_map,
            dry_run: DryRun::No,
            reflog_message: None,
            write_packed_refs: WritePackedRefs::Never,
            shallow: Default::default(),
        })
    }
}

/// A structure to hold the result of the handshake with the remote and configure the upcoming fetch operation.
pub struct Prepare<'remote, 'repo, T>
where
    T: Transport,
{
    pub(crate) con: Option<Connection<'remote, 'repo, T>>,
    pub(crate) ref_map: fetch::RefMap,
    pub(crate) dry_run: DryRun,
    pub(crate) reflog_message: Option<RefLogMessage>,
    pub(crate) write_packed_refs: WritePackedRefs,
    pub(crate) shallow: remote::fetch::Shallow,
}

/// Access
impl<T> Prepare<'_, '_, T>
where
    T: Transport,
{
    /// Return the `ref_map` (that includes the server handshake) which was part of listing refs prior to fetching a pack.
    pub fn ref_map(&self) -> &fetch::RefMap {
        &self.ref_map
    }
}

/// Builder
impl<T> Prepare<'_, '_, T>
where
    T: Transport,
{
    /// If dry run is enabled, no change to the repository will be made.
    pub fn with_dry_run(mut self, enabled: bool) -> Self {
        self.dry_run = if enabled { DryRun::Yes } else { DryRun::No };
        self
    }

    /// If enabled, don't write ref updates to loose refs, but put them exclusively to packed-refs.
    pub fn with_write_packed_refs_only(mut self, enabled: bool) -> Self {
        self.write_packed_refs = if enabled {
            WritePackedRefs::Only
        } else {
            WritePackedRefs::Never
        };
        self
    }

    /// Set the reflog message to use when updating refs after fetching a pack.
    pub fn with_reflog_message(mut self, reflog_message: RefLogMessage) -> Self {
        self.reflog_message = reflog_message.into();
        self
    }

    /// Define what to do when the current repository is a shallow clone.
    pub fn with_shallow(mut self, shallow: remote::fetch::Shallow) -> Self {
        self.shallow = shallow;
        self
    }
}

/// Receive
impl<T> Prepare<'_, '_, T>
where
    T: Transport,
{
    /// Receive the pack and perform the operation as configured by git via `git-config` or overridden by various builder methods.
    /// Return `Ok(Outcome)` with an [`Outcome::status`] indicating if a change was made or not.
    ///
    /// ### Async Mode Shortcoming
    ///
    /// Currently, the entire process of resolving a pack is blocking the executor. This can be fixed using the `blocking` crate, but it
    /// didn't seem worth the tradeoff of having more complex code.
    #[allow(clippy::result_large_err)]
    pub async fn receive<P>(mut self, progress: P, should_interrupt: &AtomicBool) -> Result<Outcome, Error>
    where
        P: gix_features::progress::NestedProgress,
        P::SubProgress: 'static,
    {
        let ref_map = &self.ref_map;
        if ref_map.is_missing_required_mapping() {
            let mut specs = ref_map.refspecs.clone();
            specs.extend(ref_map.extra_refspecs.clone());
            return Err(Error::NoMapping {
                refspecs: specs,
                num_remote_refs: ref_map.remote_refs.len(),
            });
        }

        let mut con = self.con.take().expect("receive() can only be called once");
        let mut handshake = con.handshake.take().expect("receive() can only be called once");
        let repo = con.remote.repo;

        let expected_object_hash = repo.object_hash();
        if ref_map.object_hash != expected_object_hash {
            return Err(Error::IncompatibleObjectHash {
                local: expected_object_hash,
                remote: ref_map.object_hash,
            });
        }

        let fetch_options = gix_protocol::fetch::Options {
            shallow_file: repo.shallow_file(),
            shallow: &self.shallow,
            tags: con.remote.fetch_tags,
            reject_shallow_remote: repo
                .config
                .resolved
                .boolean_filter("clone.rejectShallow", &mut repo.filter_config_section())
                .map(|val| Clone::REJECT_SHALLOW.enrich_error(val))
                .transpose()?
                .unwrap_or(false),
        };
        let context = gix_protocol::fetch::Context {
            handshake: &mut handshake,
            transport: &mut con.transport.inner,
            user_agent: repo.config.user_agent_tuple(),
            trace_packetlines: con.trace,
        };

        let negotiator = repo
            .config
            .resolved
            .string(Fetch::NEGOTIATION_ALGORITHM)
            .map(|n| Fetch::NEGOTIATION_ALGORITHM.try_into_negotiation_algorithm(n))
            .transpose()
            .with_leniency(repo.config.lenient_config)?
            .unwrap_or(Algorithm::Consecutive)
            .into_negotiator();
        let graph_repo = {
            let mut r = repo.clone();
            r.objects.refresh = RefreshMode::Never;
            r.objects.unset_object_cache();
            r
        };
        let cache = graph_repo.commit_graph_if_enabled().ok().flatten();
        let mut graph = graph_repo.revision_graph(cache.as_ref());
        let alternates = repo.objects.store_ref().alternate_db_paths()?;
        let mut negotiate = Negotiate {
            objects: &graph_repo.objects,
            refs: &graph_repo.refs,
            graph: &mut graph,
            alternates,
            ref_map,
            shallow: &self.shallow,
            tags: con.remote.fetch_tags,
            negotiator,
            open_options: repo.options.clone(),
        };

        let write_pack_options = gix_pack::bundle::write::Options {
            thread_limit: config::index_threads(repo)?,
            index_version: config::pack_index_version(repo)?,
            iteration_mode: gix_pack::data::input::Mode::Verify,
            object_hash: con.remote.repo.object_hash(),
        };
        let mut write_pack_bundle = None;

        let consume_pack = |reader: &mut dyn std::io::BufRead,
                            progress: &mut dyn gix_features::progress::DynNestedProgress,
                            should_interrupt: &std::sync::atomic::AtomicBool|
         -> Result<bool, gix_pack::bundle::write::Error> {
            let mut may_read_to_end = false;
            write_pack_bundle = if matches!(self.dry_run, DryRun::No) {
                let res = gix_pack::Bundle::write_to_directory(
                    reader,
                    Some(&repo.objects.store_ref().path().join("pack")),
                    progress,
                    should_interrupt,
                    Some(Box::new({
                        let repo = repo.clone();
                        repo.objects
                    })),
                    write_pack_options,
                )?;
                may_read_to_end = true;
                Some(res)
            } else {
                None
            };
            Ok(may_read_to_end)
        };
        let res = gix_protocol::fetch_async(
            &mut negotiate,
            consume_pack,
            progress,
            should_interrupt,
            context,
            fetch_options,
        )
        .await?;
        let negotiate = res.map(|v| outcome::Negotiate {
            graph: graph.detach(),
            rounds: v.negotiate.rounds,
        });

        if matches!(handshake.server_protocol_version, gix_protocol::transport::Protocol::V2) {
            gix_protocol::indicate_end_of_interaction_async(&mut con.transport.inner, con.trace)
                .await
                .ok();
        }

        let update_refs = refs::update(
            repo,
            self.reflog_message
                .take()
                .unwrap_or_else(|| RefLogMessage::Prefixed { action: "fetch".into() }),
            &self.ref_map.mappings,
            con.remote.refspecs(remote::Direction::Fetch),
            &self.ref_map.extra_refspecs,
            con.remote.fetch_tags,
            self.dry_run,
            self.write_packed_refs,
        )?;

        if let Some(bundle) = write_pack_bundle.as_mut() {
            if !update_refs.edits.is_empty() || bundle.index.num_objects == 0 {
                if let Some(path) = bundle.keep_path.take() {
                    std::fs::remove_file(&path).map_err(|err| Error::RemovePackKeepFile { path, source: err })?;
                }
            }
        }

        let out = Outcome {
            handshake,
            ref_map: std::mem::take(&mut self.ref_map),
            status: match write_pack_bundle {
                Some(write_pack_bundle) => Status::Change {
                    write_pack_bundle,
                    update_refs,
                    negotiate: negotiate.expect("if we have a pack, we always negotiated it"),
                },
                None => Status::NoPackReceived {
                    dry_run: matches!(self.dry_run, DryRun::Yes),
                    negotiate,
                    update_refs,
                },
            },
        };
        Ok(out)
    }
}

struct Negotiate<'a, 'b, 'c> {
    objects: &'a crate::OdbHandle,
    refs: &'a gix_ref::file::Store,
    graph: &'a mut gix_negotiate::Graph<'b, 'c>,
    alternates: Vec<PathBuf>,
    ref_map: &'a gix_protocol::fetch::RefMap,
    shallow: &'a gix_protocol::fetch::Shallow,
    tags: gix_protocol::fetch::Tags,
    negotiator: Box<dyn gix_negotiate::Negotiator>,
    open_options: crate::open::Options,
}

impl gix_protocol::fetch::Negotiate for Negotiate<'_, '_, '_> {
    fn mark_complete_and_common_ref(&mut self) -> Result<negotiate::Action, negotiate::Error> {
        negotiate::mark_complete_and_common_ref(
            &self.objects,
            self.refs,
            {
                let alternates = std::mem::take(&mut self.alternates);
                let open_options = self.open_options.clone();
                move || -> Result<_, std::convert::Infallible> {
                    Ok(alternates
                        .into_iter()
                        .filter_map(move |path| {
                            path.ancestors()
                                .nth(1)
                                .and_then(|git_dir| crate::open_opts(git_dir, open_options.clone()).ok())
                        })
                        .map(|repo| (repo.refs, repo.objects)))
                }
            },
            self.negotiator.deref_mut(),
            &mut *self.graph,
            self.ref_map,
            self.shallow,
            negotiate::make_refmapping_ignore_predicate(self.tags, self.ref_map),
        )
    }

    fn add_wants(&mut self, arguments: &mut Arguments, remote_ref_target_known: &[bool]) -> bool {
        negotiate::add_wants(
            self.objects,
            arguments,
            self.ref_map,
            remote_ref_target_known,
            self.shallow,
            negotiate::make_refmapping_ignore_predicate(self.tags, self.ref_map),
        )
    }

    fn one_round(
        &mut self,
        state: &mut negotiate::one_round::State,
        arguments: &mut Arguments,
        previous_response: Option<&gix_protocol::fetch::Response>,
    ) -> Result<(negotiate::Round, bool), negotiate::Error> {
        negotiate::one_round(
            self.negotiator.deref_mut(),
            &mut *self.graph,
            state,
            arguments,
            previous_response,
        )
    }
}

#[allow(dead_code)]
type _BString = BString;
