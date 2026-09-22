//! RPC interface for the custom Subtensor rpc methods

use codec::{Decode, Encode};
use jsonrpsee::{
    core::RpcResult,
    proc_macros::rpc,
    types::{ErrorObjectOwned, error::ErrorObject},
};
use sp_blockchain::HeaderBackend;
use sp_runtime::{AccountId32, traits::Block as BlockT};
use std::sync::Arc;
use subtensor_runtime_common::{MechId, NetUid, TaoBalance};

use sp_api::ProvideRuntimeApi;

pub use subtensor_custom_rpc_runtime_api::{
    BetaBasketRuntimeApi, DelegateInfoRuntimeApi, NeuronInfoRuntimeApi, StakeInfoRuntimeApi,
    SubnetInfoRuntimeApi, SubnetRegistrationRuntimeApi,
};

/// Per-block result cache for read-only runtime API calls.
///
/// Keyed by (call, SCALE-encoded params, block hash). Results are pure
/// functions of that key so cached entries are never stale; they are evicted in
/// insertion order once the byte budget is exceeded.
///
/// Disabled unless `SUBTENSOR_RPC_CACHE_BYTES` is set.
pub mod rpc_cache {
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};

    type Key = (&'static str, Vec<u8>, Vec<u8>);
    type Value = Result<Arc<Vec<u8>>, String>;
    /// Per-key slot. Holding the slot lock across the computation is what makes
    /// concurrent identical requests share a single execution.
    type Slot = Arc<Mutex<Option<Value>>>;

    struct Inner {
        map: HashMap<Key, (Slot, usize)>,
        order: VecDeque<Key>,
        bytes: usize,
    }

    pub struct ResultCache {
        cap_bytes: usize,
        inner: Mutex<Inner>,
        hits: AtomicU64,
        misses: AtomicU64,
    }

    static CACHE: OnceLock<ResultCache> = OnceLock::new();

    pub fn global() -> &'static ResultCache {
        CACHE.get_or_init(|| {
            let cap_bytes = std::env::var("SUBTENSOR_RPC_CACHE_BYTES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0);
            if cap_bytes > 0 {
                log::info!(
                    target: "rpc-cache",
                    "Subtensor RPC result cache enabled, capacity {} MiB",
                    cap_bytes / (1024 * 1024)
                );
            }
            ResultCache {
                cap_bytes,
                inner: Mutex::new(Inner {
                    map: HashMap::new(),
                    order: VecDeque::new(),
                    bytes: 0,
                }),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
            }
        })
    }

    /// (hits, misses) since start — used by the benchmark harness to report the
    /// cache hit rate alongside throughput.
    pub fn stats() -> (u64, u64) {
        let c = global();
        (c.hits.load(Ordering::Relaxed), c.misses.load(Ordering::Relaxed))
    }

    impl ResultCache {
        pub fn enabled(&self) -> bool {
            self.cap_bytes > 0
        }

        fn slot(&self, key: &Key) -> (Slot, bool) {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((slot, _)) = inner.map.get(key) {
                return (slot.clone(), true);
            }
            let slot: Slot = Arc::new(Mutex::new(None));
            inner.map.insert(key.clone(), (slot.clone(), 0));
            inner.order.push_back(key.clone());
            (slot, false)
        }

        fn account(&self, key: &Key, size: usize) {
            let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            // Reborrow as a plain `&mut Inner`: field borrows cannot be split
            // through the `MutexGuard`'s `DerefMut`.
            let inner = &mut *guard;
            if let Some(entry) = inner.map.get_mut(key) {
                entry.1 = size;
                inner.bytes = inner.bytes.saturating_add(size);
            }
            while inner.bytes > self.cap_bytes {
                let Some(oldest) = inner.order.pop_front() else { break };
                if let Some((_, sz)) = inner.map.remove(&oldest) {
                    inner.bytes = inner.bytes.saturating_sub(sz);
                }
            }
        }
    }

    /// Return the cached result for this call, computing it if absent.
    ///
    /// `at` must be a concrete block hash, never "latest": the caller resolves
    /// best_hash before calling so that the key pins a specific block.
    pub fn cached<F>(call: &'static str, params: &[u8], at: Vec<u8>, compute: F) -> Result<Vec<u8>, String>
    where
        F: FnOnce() -> Result<Vec<u8>, String>,
    {
        let cache = global();
        if !cache.enabled() {
            return compute();
        }
        let key: Key = (call, params.to_vec(), at);
        let (slot, existed) = cache.slot(&key);

        let mut guard = slot.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(v) = guard.as_ref() {
            cache.hits.fetch_add(1, Ordering::Relaxed);
            return v.clone().map(|b| (*b).clone());
        }
        if existed {
            // Slot was present but empty: a previous computation failed.
            cache.misses.fetch_add(1, Ordering::Relaxed);
        } else {
            cache.misses.fetch_add(1, Ordering::Relaxed);
        }

        let computed = compute();
        let value: Value = computed.map(Arc::new);
        *guard = Some(value.clone());
        drop(guard);

        match &value {
            Ok(bytes) => {
                cache.account(&key, bytes.len());
                Ok((**bytes).clone())
            }
            Err(e) => Err(e.clone()),
        }
    }
}


#[rpc(client, server)]
pub trait SubtensorCustomApi<BlockHash> {
    #[method(name = "delegateInfo_getDelegates", blocking)]
    fn get_delegates(&self, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "delegateInfo_getDelegate", blocking)]
    fn get_delegate(
        &self,
        delegate_account_vec: Vec<u8>,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;
    #[method(name = "delegateInfo_getDelegated", blocking)]
    fn get_delegated(
        &self,
        delegatee_account_vec: Vec<u8>,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;

    #[method(name = "neuronInfo_getNeuronsLite", blocking)]
    fn get_neurons_lite(&self, netuid: NetUid, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "neuronInfo_getNeuronLite", blocking)]
    fn get_neuron_lite(
        &self,
        netuid: NetUid,
        uid: u16,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;
    #[method(name = "neuronInfo_getNeurons", blocking)]
    fn get_neurons(&self, netuid: NetUid, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "neuronInfo_getNeuron", blocking)]
    fn get_neuron(&self, netuid: NetUid, uid: u16, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getSubnetInfo")]
    fn get_subnet_info(&self, netuid: NetUid, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getSubnetsInfo", blocking)]
    fn get_subnets_info(&self, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getSubnetInfo_v2")]
    fn get_subnet_info_v2(&self, netuid: NetUid, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getSubnetsInfo_v2", blocking)]
    fn get_subnets_info_v2(&self, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getSubnetHyperparams", blocking)]
    fn get_subnet_hyperparams(&self, netuid: NetUid, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getSubnetHyperparamsV2", blocking)]
    fn get_subnet_hyperparams_v2(
        &self,
        netuid: NetUid,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getAllDynamicInfo", blocking)]
    fn get_all_dynamic_info(&self, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getDynamicInfo", blocking)]
    fn get_dynamic_info(&self, netuid: NetUid, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getAllMetagraphs", blocking)]
    fn get_all_metagraphs(&self, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getMetagraph", blocking)]
    fn get_metagraph(&self, netuid: NetUid, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getAllMechagraphs", blocking)]
    fn get_all_mechagraphs(&self, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getMechagraph", blocking)]
    fn get_mechagraph(
        &self,
        netuid: NetUid,
        mecid: MechId,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getSubnetState", blocking)]
    fn get_subnet_state(&self, netuid: NetUid, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;

    #[method(name = "subnetInfo_getBlockEmission")]
    fn get_block_emission(&self, at: Option<BlockHash>) -> RpcResult<TaoBalance>;
    #[method(name = "subnetInfo_getLockCost")]
    fn get_network_lock_cost(&self, at: Option<BlockHash>) -> RpcResult<TaoBalance>;
    #[method(name = "subnetInfo_getSelectiveMetagraph", blocking)]
    fn get_selective_metagraph(
        &self,
        netuid: NetUid,
        metagraph_index: Vec<u16>,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getColdkeyAutoStakeHotkey")]
    fn get_coldkey_auto_stake_hotkey(
        &self,
        coldkey: AccountId32,
        netuid: NetUid,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getSelectiveMechagraph", blocking)]
    fn get_selective_mechagraph(
        &self,
        netuid: NetUid,
        mecid: MechId,
        metagraph_index: Vec<u16>,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;
    #[method(name = "subnetInfo_getSubnetToPrune")]
    fn get_subnet_to_prune(&self, at: Option<BlockHash>) -> RpcResult<Option<NetUid>>;
    #[method(name = "subnetInfo_getSubnetAccountId")]
    fn get_subnet_account_id(&self, netuid: NetUid, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    #[method(name = "stakeInfo_getColdkeyLock")]
    fn get_coldkey_lock(
        &self,
        coldkey: AccountId32,
        netuid: NetUid,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;

    /// Total TAO a staker (coldkey) would realize by redeeming all its root beta baskets.
    #[method(name = "betaBasket_getStakerOwed")]
    fn get_root_basket_owed(
        &self,
        coldkey: AccountId32,
        at: Option<BlockHash>,
    ) -> RpcResult<TaoBalance>;
    /// TAO a staker would realize by redeeming its owed shares on one validator.
    #[method(name = "betaBasket_getStakerValidatorOwed")]
    fn get_basket_payout(
        &self,
        hotkey: AccountId32,
        coldkey: AccountId32,
        at: Option<BlockHash>,
    ) -> RpcResult<TaoBalance>;
    /// A validator's beta basket net asset value, in TAO.
    #[method(name = "betaBasket_getValidatorNav")]
    fn get_validator_basket_nav(
        &self,
        hotkey: AccountId32,
        at: Option<BlockHash>,
    ) -> RpcResult<TaoBalance>;
    /// A validator's full basket breakdown: SCALE-encoded `Vec<(NetUid, AlphaBalance, TaoBalance)>`.
    #[method(name = "betaBasket_getValidatorBasket", blocking)]
    fn get_validator_basket(
        &self,
        hotkey: AccountId32,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;
    /// Network-wide total beta basket NAV across all validators, in TAO.
    #[method(name = "betaBasket_getTotalNav")]
    fn get_root_basket_total_nav(&self, at: Option<BlockHash>) -> RpcResult<TaoBalance>;
    /// Full explorer-facing basket summary for one validator: SCALE-encoded `BasketSummary`
    /// (NAV realizable + spot, shares, lifetime deposited/redeemed, holdings).
    #[method(name = "betaBasket_getValidatorSummary")]
    fn get_validator_basket_summary(
        &self,
        hotkey: AccountId32,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;
    /// Summaries for every validator with an active basket: SCALE-encoded `Vec<BasketSummary>`.
    #[method(name = "betaBasket_getAllBaskets", blocking)]
    fn get_all_validator_baskets(&self, at: Option<BlockHash>) -> RpcResult<Vec<u8>>;
    /// A staker's positions across its validators: SCALE-encoded
    /// `Vec<(AccountId32, u64 shares, TaoBalance payout)>`.
    #[method(name = "betaBasket_getStakerPositions")]
    fn get_root_basket_positions(
        &self,
        coldkey: AccountId32,
        at: Option<BlockHash>,
    ) -> RpcResult<Vec<u8>>;
}

pub struct SubtensorCustom<C, P> {
    /// Shared reference to the client.
    client: Arc<C>,
    _marker: std::marker::PhantomData<P>,
}

impl<C, P> SubtensorCustom<C, P> {
    /// Creates a new instance of the TransactionPayment Rpc helper.
    pub fn new(client: Arc<C>) -> Self {
        Self {
            client,
            _marker: Default::default(),
        }
    }
}

/// Error type of this RPC api.
pub enum Error {
    /// The call to runtime failed.
    RuntimeError(String),
}

impl From<Error> for ErrorObjectOwned {
    fn from(e: Error) -> Self {
        match e {
            Error::RuntimeError(e) => ErrorObject::owned(1, e, None::<()>),
        }
    }
}

impl From<Error> for i32 {
    fn from(e: Error) -> i32 {
        match e {
            Error::RuntimeError(_) => 1,
        }
    }
}

impl<C, Block> SubtensorCustomApiServer<<Block as BlockT>::Hash> for SubtensorCustom<C, Block>
where
    Block: BlockT,
    C: ProvideRuntimeApi<Block> + HeaderBackend<Block> + Send + Sync + 'static,
    C::Api: DelegateInfoRuntimeApi<Block>,
    C::Api: NeuronInfoRuntimeApi<Block>,
    C::Api: SubnetInfoRuntimeApi<Block>,
    C::Api: StakeInfoRuntimeApi<Block>,
    C::Api: SubnetRegistrationRuntimeApi<Block>,
    C::Api: BetaBasketRuntimeApi<Block>,
{
    fn get_delegates(&self, at: Option<<Block as BlockT>::Hash>) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = Vec::<u8>::new();
        rpc_cache::cached("get_delegates", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_delegates(at)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get delegates info: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_delegate(
        &self,
        delegate_account_vec: Vec<u8>,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        let delegate_account = match AccountId32::decode(&mut &delegate_account_vec[..]) {
            Ok(delegate_account) => delegate_account,
            Err(e) => {
                return Err(
                    Error::RuntimeError(format!("Unable to get delegates info: {e:?}")).into(),
                );
            }
        };
        match api.get_delegate(at, delegate_account) {
            Ok(result) => Ok(result.encode()),
            Err(e) => {
                Err(Error::RuntimeError(format!("Unable to get delegates info: {e:?}")).into())
            }
        }
    }

    fn get_delegated(
        &self,
        delegatee_account_vec: Vec<u8>,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        let delegatee_account = match AccountId32::decode(&mut &delegatee_account_vec[..]) {
            Ok(delegatee_account) => delegatee_account,
            Err(e) => {
                return Err(
                    Error::RuntimeError(format!("Unable to get delegates info: {e:?}")).into(),
                );
            }
        };
        match api.get_delegated(at, delegatee_account) {
            Ok(result) => Ok(result.encode()),
            Err(e) => {
                Err(Error::RuntimeError(format!("Unable to get delegates info: {e:?}")).into())
            }
        }
    }

    fn get_neurons_lite(
        &self,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid,).encode();
        rpc_cache::cached("get_neurons_lite", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_neurons_lite(at, netuid)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get neurons lite info: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_neuron_lite(
        &self,
        netuid: NetUid,
        uid: u16,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid, &uid).encode();
        rpc_cache::cached("get_neuron_lite", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_neuron_lite(at, netuid, uid)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get neurons lite info: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_neurons(
        &self,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid,).encode();
        rpc_cache::cached("get_neurons", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_neurons(at, netuid)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get neurons info: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_neuron(
        &self,
        netuid: NetUid,
        uid: u16,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid, &uid).encode();
        rpc_cache::cached("get_neuron", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_neuron(at, netuid, uid)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get neuron info: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_subnet_info(
        &self,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_subnet_info(at, netuid) {
            Ok(result) => Ok(result.encode()),
            Err(e) => Err(Error::RuntimeError(format!("Unable to get subnet info: {e:?}")).into()),
        }
    }

    #[allow(deprecated)]
    fn get_subnet_hyperparams(
        &self,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid,).encode();
        rpc_cache::cached("get_subnet_hyperparams", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_subnet_hyperparams(at, netuid)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get subnet info: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    #[allow(deprecated)]
    fn get_subnet_hyperparams_v2(
        &self,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid,).encode();
        rpc_cache::cached("get_subnet_hyperparams_v2", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_subnet_hyperparams_v2(at, netuid)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get subnet info: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_all_dynamic_info(&self, at: Option<<Block as BlockT>::Hash>) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = Vec::<u8>::new();
        rpc_cache::cached("get_all_dynamic_info", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_all_dynamic_info(at)
                .map(|result| result.encode())
                .map_err(|e| format!(
                "Unable to get dynamic subnets info: {e:?}"
            ))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_all_metagraphs(&self, at: Option<<Block as BlockT>::Hash>) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = Vec::<u8>::new();
        rpc_cache::cached("get_all_metagraphs", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_all_metagraphs(at)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get metagraps: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_all_mechagraphs(&self, at: Option<<Block as BlockT>::Hash>) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = Vec::<u8>::new();
        rpc_cache::cached("get_all_mechagraphs", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_all_mechagraphs(at)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get metagraps: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_dynamic_info(
        &self,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid,).encode();
        rpc_cache::cached("get_dynamic_info", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_dynamic_info(at, netuid)
                .map(|result| result.encode())
                .map_err(|e| format!(
                "Unable to get dynamic subnets info: {e:?}"
            ))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_metagraph(
        &self,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid,).encode();
        rpc_cache::cached("get_metagraph", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_metagraph(at, netuid)
                .map(|result| result.encode())
                .map_err(|e| format!(
                "Unable to get dynamic subnets info: {e:?}"
            ))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_mechagraph(
        &self,
        netuid: NetUid,
        mecid: MechId,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid, &mecid).encode();
        rpc_cache::cached("get_mechagraph", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_mechagraph(at, netuid, mecid)
                .map(|result| result.encode())
                .map_err(|e| format!(
                "Unable to get dynamic subnets info: {e:?}"
            ))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_subnet_state(
        &self,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid,).encode();
        rpc_cache::cached("get_subnet_state", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_subnet_state(at, netuid)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get subnet state info: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_subnets_info(&self, at: Option<<Block as BlockT>::Hash>) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = Vec::<u8>::new();
        rpc_cache::cached("get_subnets_info", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_subnets_info(at)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get subnets info: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_subnet_info_v2(
        &self,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_subnet_info_v2(at, netuid) {
            Ok(result) => Ok(result.encode()),
            Err(e) => Err(Error::RuntimeError(format!("Unable to get subnet info: {e:?}")).into()),
        }
    }

    fn get_subnets_info_v2(&self, at: Option<<Block as BlockT>::Hash>) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = Vec::<u8>::new();
        rpc_cache::cached("get_subnets_info_v2", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_subnets_info_v2(at)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get subnets info: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_block_emission(&self, at: Option<<Block as BlockT>::Hash>) -> RpcResult<TaoBalance> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        api.get_block_emission(at)
            .map_err(|e| Error::RuntimeError(format!("Unable to get block emission: {e:?}")).into())
    }

    fn get_network_lock_cost(&self, at: Option<<Block as BlockT>::Hash>) -> RpcResult<TaoBalance> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        api.get_network_registration_cost(at).map_err(|e| {
            Error::RuntimeError(format!("Unable to get subnet lock cost: {e:?}")).into()
        })
    }

    fn get_selective_metagraph(
        &self,
        netuid: NetUid,
        metagraph_index: Vec<u16>,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid, &metagraph_index).encode();
        rpc_cache::cached("get_selective_metagraph", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_selective_metagraph(at, netuid, metagraph_index)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get selective metagraph: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_coldkey_auto_stake_hotkey(
        &self,
        coldkey: AccountId32,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_coldkey_auto_stake_hotkey(at, coldkey, netuid) {
            Ok(result) => Ok(result.encode()),
            Err(e) => Err(Error::RuntimeError(format!(
                "Unable to get coldkey auto stake hotkey: {e:?}"
            ))
            .into()),
        }
    }

    fn get_selective_mechagraph(
        &self,
        netuid: NetUid,
        mecid: MechId,
        metagraph_index: Vec<u16>,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&netuid, &mecid, &metagraph_index).encode();
        rpc_cache::cached("get_selective_mechagraph", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_selective_mechagraph(at, netuid, mecid, metagraph_index)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get selective metagraph: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_subnet_to_prune(
        &self,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Option<NetUid>> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_subnet_to_prune(at) {
            Ok(result) => Ok(result),
            Err(e) => {
                Err(Error::RuntimeError(format!("Unable to get subnet to prune: {e:?}")).into())
            }
        }
    }

    fn get_subnet_account_id(
        &self,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_subnet_account_id(at, netuid) {
            Ok(result) => Ok(result.encode()),
            Err(_) => Err(Error::RuntimeError("Subnet does not exist".to_string()).into()),
        }
    }

    fn get_coldkey_lock(
        &self,
        coldkey: AccountId32,
        netuid: NetUid,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_coldkey_lock(at, coldkey, netuid) {
            Ok(result) => Ok(result.encode()),
            Err(e) => Err(Error::RuntimeError(format!("Unable to get coldkey lock: {e:?}")).into()),
        }
    }

    fn get_root_basket_owed(
        &self,
        coldkey: AccountId32,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<TaoBalance> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_root_basket_owed(at, coldkey) {
            Ok(result) => Ok(result),
            Err(e) => {
                Err(Error::RuntimeError(format!("Unable to get root basket owed: {e:?}")).into())
            }
        }
    }

    fn get_basket_payout(
        &self,
        hotkey: AccountId32,
        coldkey: AccountId32,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<TaoBalance> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_basket_payout(at, hotkey, coldkey) {
            Ok(result) => Ok(result),
            Err(e) => Err(Error::RuntimeError(format!(
                "Unable to get staker validator owed: {e:?}"
            ))
            .into()),
        }
    }

    fn get_validator_basket_nav(
        &self,
        hotkey: AccountId32,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<TaoBalance> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_validator_basket_nav(at, hotkey) {
            Ok(result) => Ok(result),
            Err(e) => Err(Error::RuntimeError(format!(
                "Unable to get validator basket NAV: {e:?}"
            ))
            .into()),
        }
    }

    fn get_validator_basket(
        &self,
        hotkey: AccountId32,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = (&hotkey,).encode();
        rpc_cache::cached("get_validator_basket", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_validator_basket(at, hotkey)
                .map(|result| result.encode())
                .map_err(|e| format!("Unable to get validator basket: {e:?}"))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_root_basket_total_nav(
        &self,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<TaoBalance> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_root_basket_total_nav(at) {
            Ok(result) => Ok(result),
            Err(e) => {
                Err(Error::RuntimeError(format!("Unable to get total basket NAV: {e:?}")).into())
            }
        }
    }

    fn get_validator_basket_summary(
        &self,
        hotkey: AccountId32,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_validator_basket_summary(at, hotkey) {
            Ok(result) => Ok(result.encode()),
            Err(e) => Err(Error::RuntimeError(format!(
                "Unable to get validator basket summary: {e:?}"
            ))
            .into()),
        }
    }

    fn get_all_validator_baskets(&self, at: Option<<Block as BlockT>::Hash>) -> RpcResult<Vec<u8>> {
        let at = at.unwrap_or_else(|| self.client.info().best_hash);
        let params = Vec::<u8>::new();
        rpc_cache::cached("get_all_validator_baskets", &params, at.encode(), || {
            let api = self.client.runtime_api();
            api.get_all_validator_baskets(at)
                .map(|result| result.encode())
                .map_err(|e| format!(
                "Unable to get all validator baskets: {e:?}"
            ))
        })
        .map_err(|e| ErrorObjectOwned::from(Error::RuntimeError(e)))
    }

    fn get_root_basket_positions(
        &self,
        coldkey: AccountId32,
        at: Option<<Block as BlockT>::Hash>,
    ) -> RpcResult<Vec<u8>> {
        let api = self.client.runtime_api();
        let at = at.unwrap_or_else(|| self.client.info().best_hash);

        match api.get_root_basket_positions(at, coldkey) {
            Ok(result) => Ok(result.encode()),
            Err(e) => Err(Error::RuntimeError(format!(
                "Unable to get root basket positions: {e:?}"
            ))
            .into()),
        }
    }
}
