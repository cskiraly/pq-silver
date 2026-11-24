use flux::type_hash_derive::TypeHash;
/// Discriminant is the slot in `TCacheTable` / `TCacheReader`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, TypeHash)]
#[repr(u8)]
pub enum TCacheId {
    NetworkIngress,
    NetworkProcessing,
    ClusterInbound,
    ControlProcessing,
    ControlGossip,
    ControlRpc,
    ClusterOutbound,
    ControlSlot,
    StorageDelivery,
    BoundaryProcessing,
    BeaconStateHandoff,
    ProposedColumns,
    /// The SEACAST store: group assemblies, written and read by Control alone.
    #[cfg(feature = "seacast")]
    ControlSeacast,
    /// The same for the aggregates' groups.
    #[cfg(feature = "seacast")]
    ControlSeacastAggregates,
}

impl TCacheId {
    pub const COUNT: usize = 12 + 2 * cfg!(feature = "seacast") as usize;
    pub const ALL: [Self; Self::COUNT] = {
        let mut all = [Self::NetworkIngress; Self::COUNT];
        let mut index = 0;
        while index < Self::BASE.len() {
            all[index] = Self::BASE[index];
            index += 1;
        }
        #[cfg(feature = "seacast")]
        {
            all[index] = Self::ControlSeacast;
            all[index + 1] = Self::ControlSeacastAggregates;
        }
        all
    };
    const BASE: [Self; 12] = [
        Self::NetworkIngress,
        Self::NetworkProcessing,
        Self::ClusterInbound,
        Self::ControlProcessing,
        Self::ControlGossip,
        Self::ControlRpc,
        Self::ClusterOutbound,
        Self::ControlSlot,
        Self::StorageDelivery,
        Self::BoundaryProcessing,
        Self::BeaconStateHandoff,
        Self::ProposedColumns,
    ];

    pub fn from_index(index: u64) -> Option<Self> {
        usize::try_from(index).ok().and_then(|index| Self::ALL.get(index)).copied()
    }

    /// Metrics label: `counters-tcache-{name}`, `tcache-write-{name}`.
    pub fn name(self) -> &'static str {
        match self {
            Self::NetworkIngress => "network_ingress",
            Self::NetworkProcessing => "network_processing",
            Self::ClusterInbound => "cluster_inbound",
            Self::ControlProcessing => "control_processing",
            Self::ControlGossip => "control_gossip",
            Self::ControlRpc => "control_rpc",
            Self::ClusterOutbound => "cluster_outbound",
            Self::ControlSlot => "control_slot",
            Self::StorageDelivery => "storage_delivery",
            Self::BoundaryProcessing => "boundary_processing",
            Self::BeaconStateHandoff => "beacon_state_handoff",
            Self::ProposedColumns => "proposed_columns",
            #[cfg(feature = "seacast")]
            Self::ControlSeacast => "control_seacast",
            #[cfg(feature = "seacast")]
            Self::ControlSeacastAggregates => "control_seacast_aggregates",
        }
    }
}

const _: () = assert!(TCacheId::ALL[TCacheId::COUNT - 1] as usize + 1 == TCacheId::COUNT);
