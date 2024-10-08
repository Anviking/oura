use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;

use bytes::Bytes;

use pallas::network::miniprotocols::Point;
use pallas::ledger::addresses::Address;
use pallas::interop::utxorpc::spec::cardano::{TxOutput, Tx};

use gasket::framework::*;
use tracing::{debug, warn};

use tokio_tungstenite::WebSocketStream;
use futures_util::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use serde::{Deserialize, Serialize, Deserializer};
use serde_json::Value;
use serde::de::{self};

use crate::framework::*;

// Models /most/ hydra messages. Messages without head_id are excluded.
pub struct HydraMessage {
    seq: u64,
    head_id: Vec<u8>,
    payload: HydraMessagePayload
}

impl<'de> Deserialize<'de> for HydraMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map: Value = Deserialize::deserialize(deserializer)?;

        let seq = map.get("seq")
            .ok_or_else(|| de::Error::missing_field("seq"))?
            .as_u64()
            .ok_or_else(|| de::Error::custom("seq should be a u64"))?;

        let head_id_str = map.get("headId")
            .ok_or_else(|| de::Error::missing_field("headId"))?
            .as_str()
            .ok_or_else(|| de::Error::custom("headId should be a string"))?;

        let head_id = hex::decode(head_id_str)
            .map_err(|_e| serde::de::Error::custom(format!("Expected hex-encoded headId")))?;

        let payload = HydraMessagePayload::deserialize(&map)
            .map_err(de::Error::custom)?;

        Ok(HydraMessage { seq, head_id, payload })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "tag", rename_all = "PascalCase")]
pub enum HydraMessagePayload {
    #[serde(deserialize_with = "deserialize_head_is_open")]
    HeadIsOpen { txs: Vec<Tx>,  }, // FIXME: revisit; see comments in impl
    #[serde(deserialize_with = "deserialize_tx_valid")]
    TxValid { tx: Vec<u8> }, // TODO: Use Tx instead?
    #[serde(other)]
    Other
}

// Example json:
// {
//  "headId": "84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab"
//  "seq": 15
//  "timestamp": "2024-10-03T11:38:45.449663464Z",
//  "transaction": {
//         "cborHex": "84a300d9010281825820635ffa4d3f8b5ccd60a89918866a5bb0776966572324da9a86870f79dcce4aad01018282581d605e4e214a6addd337126b3a61faad5dfe1e4f14f637a8969e3a05eefd1a0098968082581d6069830961c6af9095b0f2648dff31fa9545d8f0b6623db865eb78fde81a039387000200a100d9010281825820f953b2d6b6f319faa9f8462257eb52ad73e33199c650f0755e279e21882399c05840c1f23b630cf3d0ffe4186436225906c81bcddb0a27a632696035d4bb2d32e646c81759789c35c940b9695a87a0978a0408cff550c8d8f9ab4ac6d6d29b82a109f5f6",
//         "description": "Ledger Cddl Format",
//         "txId": "08bb77374329ca28cd3023cace2948d0fc23e2812e8998c966db8b457e6390fe",
//         "type": "Witnessed Tx ConwayEra",
//     },
// }
fn deserialize_tx_valid<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct TxValidJson {
        transaction: TxCborJson,
    }
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct TxCborJson {
        cbor_hex: String
    }

    let msg = TxValidJson::deserialize(deserializer)?;
    let cbor = hex::decode(msg.transaction.cbor_hex)
        .map_err(|_e| serde::de::Error::custom(format!("Expected hex-encoded cbor")))?;

    Ok(cbor)
}


// Example JSON:
// {
//     "headId":"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab",
//     "seq":6,
//     "tag":"HeadIsOpen",
//     "timestamp":"2024-10-07T11:45:12.219902045Z",
//     "utxo": {
//         "060d5256c6335978f59c8434dfec2f814276983c41949ffc150fa9a0d0cd5f6f#0": {
//             "address":"addr_test1vqx5tu4nzz5cuanvac4t9an4djghrx7hkdvjnnhstqm9kegvm6g6c",
//             "datum":null,
//             "datumhash":null,
//             "inlineDatum":null,
//             "referenceScript":null,
//             "value":{"lovelace":25000000}},
//         "9dce218cadb86ec5e26e9bdddab614efa722f1d72f8b98eade4b5ce52bb2eb8a#0": {
//             "address":"addr_test1vp0yug22dtwaxdcjdvaxr74dthlpunc57cm639578gz7algset3fh",
//             "datum":null,
//             "datumhash":null,
//             "inlineDatum":null,
//             "referenceScript":null,
//             "value":{"lovelace":50000000}},
//         "d683dda48161cef8015c40c55a51569c85631d294dc5e4ce39a29a54939ca6f3#0": {
//             "address":"addr_test1vp5cxztpc6hep9ds7fjgmle3l225tk8ske3rmwr9adu0m6qchmx5z",
//             "datum":null,
//             "datumhash":null,
//             "inlineDatum":null,
//             "referenceScript":null,
//             "value":{"lovelace":100000000}}
//     }
// }
fn deserialize_head_is_open<'de, D>(deserializer: D) -> Result<Vec<Tx>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HeadIsOpenJson {
        utxo: HashMap<String, TxOutJson>,
    }
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct TxOutJson {
        address: String,
        datum: Option<String>,
        datumhash: Option<String>,
        inline_datum: Option<String>,
        reference_script: Option<String>,
        value: HashMap<String, u64>
    }
    // Access the `utxo` field within the JSON object
    let msg = HeadIsOpenJson::deserialize(deserializer)?;


    // FIXME: Do we really want to turn the utxo of the HeadIsOpen message to pseodo-txs?
    // Kupo may have done this out of necessity. We may not need to.
    let mut txs = Vec::new();

    for (in_str, out_json) in msg.utxo.iter() {
        let parts: Vec<&str> = in_str.split('#').collect();
        if parts.len() != 2 { return Err(serde::de::Error::custom("Invalid transaction input format")); }
        let hash_vec = hex::decode(parts[0])
            .map_err(|e| serde::de::Error::custom(format!("Failed to parse transaction ID: {}", e)))?;
        let hash = Bytes::from(hash_vec);

        let addr = Address::from_bech32(&out_json.address)
            .map_err(|_| serde::de::Error::custom("invalid address"))?
            .to_vec();

        let o = TxOutput {
            address: Bytes::from(addr),
            coin: 0, // TODO
            assets: [].to_vec(), // TODO
            datum: None, // TODO
            script: None, // TODO
        };

        // FIXME: Temporary
        let tx = Tx {
            inputs: [].to_vec(),
            outputs: [o].to_vec(), // FIXME: This is utterly wrong! The ix is not captured.
            certificates: [].to_vec(),
            withdrawals: [].to_vec(),
            mint: [].to_vec(),
            reference_inputs: [].to_vec(),
            witnesses: None,
            collateral: None,
            fee: 0,
            validity: None,
            successful: true,
            auxiliary: None,
            hash,
        };

        txs.push(tx);
    }

    Ok(txs)

}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Snapshot {
    number: u64,
    confirmed_transaction_ids: Vec<String>,
}
type HydraConnection = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Stage)]
#[stage(
    name = "source",
    unit = "Message",
    worker = "Worker"
)]
pub struct Stage {
    config: Config,

    chain: GenesisValues,

    intersect: IntersectConfig,

    breadcrumbs: Breadcrumbs,

    pub output: SourceOutputPort,

    #[metric]
    ops_count: gasket::metrics::Counter,

    #[metric]
    chain_tip: gasket::metrics::Gauge,

    #[metric]
    current_slot: gasket::metrics::Gauge,

    #[metric]
    rollback_count: gasket::metrics::Counter,
}

pub struct Worker {
    socket: HydraConnection,
}

impl Worker {
    async fn process_next(
        &mut self,
        stage: &mut Stage,
        next: HydraMessage,
    ) -> Result<(), WorkerError> {

        // As a first implementation, we'll treat the msg seq number
        // as a slot number, and the head id as a block hash.
        //
        // This means all points on the same chain will share the same block hash, but hopefully
        // this shouldn't matter.
        let slot = next.seq;
        let hash = next.head_id;
        let point = Point::Specific(slot, hash);

        let events = match next.payload {
            HydraMessagePayload::HeadIsOpen { txs } => {
                txs.into_iter()
                    .map (|tx| ChainEvent::Apply(point.clone(), Record::ParsedTx(tx)) )
                    .collect()
            },
            HydraMessagePayload::TxValid { tx } => {
                [ChainEvent::Apply(point.clone(), Record::CborTx(tx))].to_vec()
            }
            HydraMessagePayload::Other => [].to_vec()
        };

        for evt in events {
            stage.output.send(evt.into()).await.or_panic()?;
            stage.ops_count.inc(1);
        }

        stage.breadcrumbs.track(point.clone());

        stage.chain_tip.set(point.slot_or_default() as i64);
        stage.current_slot.set(point.slot_or_default() as i64);
        stage.ops_count.inc(1);

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        debug!("connecting to hydra WebSocket");

        let url = &stage.config.hydra_socket_url;
        let (socket, _) = connect_async(url).await.expect("Can't connect");
        let worker = Self { socket };

        Ok(worker)
    }

    async fn schedule(&mut self, _stage: &mut Stage) -> Result<WorkSchedule<Message>, WorkerError> {
        let next_msg = self.socket.next().await.transpose().or_restart()?;

        Ok(match next_msg {
            Some(message) => WorkSchedule::Unit(message),
            None => WorkSchedule::Idle,
        })
    }

    async fn execute(&mut self, message: &Message, stage: &mut Stage) -> Result<(), WorkerError> {
        match message {
            Message::Text(text) => {
                debug!("Hydra message: {}", text);
                match serde_json::from_str::<HydraMessage>(text) {
                    Ok(hydra_message) => self.process_next(stage, hydra_message).await,
                    Err(err) => {
                        warn!("Failed to parse Hydra message: {}", err);
                        Ok(())
                    }
                }
            }
            _ => Ok(())
        }
    }
}

#[derive(Deserialize)]
pub struct Config {
    hydra_socket_url: String,
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            config: self,
            breadcrumbs: ctx.breadcrumbs.clone(),
            chain: ctx.chain.clone().into(),
            intersect: ctx.intersect.clone(),
            output: Default::default(),
            ops_count: Default::default(),
            chain_tip: Default::default(),
            current_slot: Default::default(),
            rollback_count: Default::default(),
        };

        Ok(stage)
    }
}
