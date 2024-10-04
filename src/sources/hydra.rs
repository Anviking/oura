use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;

use bytes::Bytes;

use pallas::network::miniprotocols::Point;
use pallas::ledger::addresses::Address;

use pallas::interop::utxorpc::spec::cardano::{TxOutput, TxInput, Tx};

use gasket::framework::*;
use serde::{Deserialize, Serialize, Deserializer};
use tracing::{debug, info, warn};

use tokio_tungstenite::WebSocketStream;
use futures_util::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::framework::*;


#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "tag", rename_all = "PascalCase")]
pub enum HydraMessage {
    #[serde(deserialize_with = "deserialize_head_is_open")]
    HeadIsOpen { txs: Vec<Tx> }, // FIXME: revisit; see comments in impl


    #[serde(deserialize_with = "deserialize_tx_valid")]
    TxValid { tx: Vec<u8>}, // TODO: Use Tx instead?
                                     //
//    SnapshotConfirmed { snapshot: Snapshot },

//    #[serde(other)]
//    SomethingElse,
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
        transaction: TxCborJson
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


fn deserialize_head_is_open<'de, D>(deserializer: D) -> Result<Vec<Tx>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HeadIsOpenJson {
        utxo: HashMap<String, TxOutJson>
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
        let ix = parts[1].parse::<u32>()
            .map_err(|e| serde::de::Error::custom(format!("Failed to parse transaction index: {}", e)))?;

        let _i = TxInput {
            tx_hash: hash.clone(),
            output_index: ix,
            as_output: None,
            redeemer: None,
        };

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
            hash: hash,
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
        match next {
            HydraMessage::HeadIsOpen { txs } => {
                        // FIXME: Dummy values
                        let slot = 0;
                        let hash = vec![0u8; 32];
                        let point = Point::Specific(slot, hash.to_vec());

                        for tx in txs {
                            let evt = ChainEvent::Apply(point.clone(), Record::ParsedTx(tx));
                            stage.output.send(evt.into()).await.or_panic()?;
                            stage.ops_count.inc(1);
                        };

                    },
            HydraMessage::TxValid { tx } => {
                // FIXME: Dummy values
                let slot = 0;
                let hash = vec![0u8; 32];
                let point = Point::Specific(slot, hash.to_vec());

                let evt = ChainEvent::Apply(point.clone(), Record::CborTx(tx));
                stage.output.send(evt.into()).await.or_panic()?;
                stage.ops_count.inc(1);

                // FIXME: Ensure we do everything we should here (c.f. n2c implementation)
            }

            _ => (),
        };

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
                match serde_json::from_str::<HydraMessage>(text) {
                    Ok(hydra_message) => {
                        self.process_next(stage, hydra_message).await;
                    }
                    Err(err) => {
                        debug!("Some other hydra message: {}", text);
                    }
                }
            }
            Message::Binary(data) => {
                debug!("Received binary data: {:?}", data);
            }
            _ => {}
        }

        Ok(())
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
