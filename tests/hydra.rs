use std::fs;

use oura::sources::hydra::{HydraMessage, HydraMessagePayload};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run_scenario(_expected_msgs: &[HydraMessage], expected_file: &str) -> TestResult {
    let _file = fs::read_to_string(expected_file)?;
    Ok(())
}

fn test_tx_valid_event_deserialization(expected: HydraMessage, input: &str) -> TestResult {
    let deserialized: HydraMessage = serde_json::from_str(&input)?;
    assert_eq!(deserialized, expected);
    Ok(())
}

fn test_other_event_deserialization(
    expected: HydraMessage,
    expected_str: &[&str],
    input: &str,
) -> TestResult {
    let deserialized: HydraMessage = serde_json::from_str(&input)?;
    assert_eq!(deserialized.seq, expected.seq);
    match deserialized.payload {
        HydraMessagePayload::PeerConnected { raw_json } => {
            for thestr in expected_str {
                assert_eq!(raw_json.contains(thestr), true);
            }
        }
        HydraMessagePayload::Idle { raw_json } => {
            for thestr in expected_str {
                assert_eq!(raw_json.contains(thestr), true);
            }
        }
        HydraMessagePayload::HeadIsInitializing { raw_json } => {
            for thestr in expected_str {
                assert_eq!(raw_json.contains(thestr), true);
            }
        }
        HydraMessagePayload::Committed { raw_json } => {
            for thestr in expected_str {
                assert_eq!(raw_json.contains(thestr), true);
            }
        }
        HydraMessagePayload::HeadIsOpen { raw_json } => {
            for thestr in expected_str {
                assert_eq!(raw_json.contains(thestr), true);
            }
        }
        HydraMessagePayload::HeadIsClosed { raw_json } => {
            for thestr in expected_str {
                assert_eq!(raw_json.contains(thestr), true);
            }
        }
        HydraMessagePayload::ReadyToFanout { raw_json } => {
            for thestr in expected_str {
                assert_eq!(raw_json.contains(thestr), true);
            }
        }
        HydraMessagePayload::HeadIsFinalized { raw_json } => {
            for thestr in expected_str {
                assert_eq!(raw_json.contains(thestr), true);
            }
        }
        _ => {
            panic!("Only other events tested here");
        }
    };
    Ok(())
}

#[test]
fn hydra_scenario_1() -> TestResult {
    let msgs = [HydraMessage {
        seq: 0,
        payload: HydraMessagePayload::Other,
    }];
    run_scenario(&msgs, "tests/hydra/scenario_1.txt")
}

#[test]
fn hydra_scenario_2() -> TestResult {
    let msgs = [HydraMessage {
        seq: 0,
        payload: HydraMessagePayload::Other,
    }];
    run_scenario(&msgs, "tests/hydra/scenario_2.txt")
}

#[test]
fn tx_valid_evt() -> TestResult {
    let evt = HydraMessage {
        seq: 15,
        payload: HydraMessagePayload::TxValid {
            tx: hex::decode("84a300d9010281825820635ffa4d3f8b5ccd60a89918866a5bb0776966572324da9a86870f79dcce4aad01018282581d605e4e214a6addd337126b3a61faad5dfe1e4f14f637a8969e3a05eefd1a0098968082581d6069830961c6af9095b0f2648dff31fa9545d8f0b6623db865eb78fde81a039387000200a100d9010281825820f953b2d6b6f319faa9f8462257eb52ad73e33199c650f0755e279e21882399c05840c1f23b630cf3d0ffe4186436225906c81bcddb0a27a632696035d4bb2d32e646c81759789c35c940b9695a87a0978a0408cff550c8d8f9ab4ac6d6d29b82a109f5f6")
                    .unwrap()
                    .to_vec(),
            head_id: hex::decode("84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab").unwrap()
                    .to_vec(),
        },
    };

    let raw_str = r#"
 {
  "headId": "84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab",
  "seq": 15,
  "timestamp": "2024-10-03T11:38:45.449663464Z",
  "tag":"TxValid",
  "transaction": {
         "cborHex": "84a300d9010281825820635ffa4d3f8b5ccd60a89918866a5bb0776966572324da9a86870f79dcce4aad01018282581d605e4e214a6addd337126b3a61faad5dfe1e4f14f637a8969e3a05eefd1a0098968082581d6069830961c6af9095b0f2648dff31fa9545d8f0b6623db865eb78fde81a039387000200a100d9010281825820f953b2d6b6f319faa9f8462257eb52ad73e33199c650f0755e279e21882399c05840c1f23b630cf3d0ffe4186436225906c81bcddb0a27a632696035d4bb2d32e646c81759789c35c940b9695a87a0978a0408cff550c8d8f9ab4ac6d6d29b82a109f5f6",
         "description": "Ledger Cddl Format",
         "txId": "08bb77374329ca28cd3023cace2948d0fc23e2812e8998c966db8b457e6390fe",
         "type": "Witnessed Tx ConwayEra"
     }
 }
"#;
    test_tx_valid_event_deserialization(evt, &raw_str)
}

#[test]
fn peer_connected_evt() -> TestResult {
    let evt = HydraMessage {
        seq: 0,
        payload: HydraMessagePayload::PeerConnected {
            raw_json: String::from(
                "{\"tag\":\"PeerConnected\",\"timestamp\":\"2024-10-08T13:01:20.556003751Z\",\"peer\":\"3\"}",
            ),
        },
    };
    let json_parts = vec![
        "\"tag\":\"PeerConnected\"",
        "\"timestamp\":\"2024-10-08T13:01:20.556003751Z\"",
        "\"peer\":\"3\"",
    ];

    let raw_str = r#"
 {
   "peer": "3",
   "seq": 0,
   "tag": "PeerConnected",
   "timestamp": "2024-10-08T13:01:20.556003751Z"
 }
"#;
    test_other_event_deserialization(evt, &json_parts, &raw_str)
}

#[test]
fn idle_evt() -> TestResult {
    let evt = HydraMessage {
        seq: 2,
        payload: HydraMessagePayload::Idle {
            raw_json: String::from(
                "{\"hydraNodeVersion\":\"0.19.0-1ffe7c6b505e3f38b5546ae5e5b97de26bc70425\",\"me\":{\"vkey\":\"b37aabd81024c043f53a069c91e51a5b52e4ea399ae17ee1fe3cb9c44db707eb\"},\"timestamp\":\"2024-10-08T13:04:56.445761285Z\",\"tag\":\"Idle\"}",
            ),
        },
    };
    let json_parts = vec![
        "\"tag\":\"Idle\"",
        "\"hydraNodeVersion\":\"0.19.0-1ffe7c6b505e3f38b5546ae5e5b97de26bc70425\"",
        "\"me\":{\"vkey\":\"b37aabd81024c043f53a069c91e51a5b52e4ea399ae17ee1fe3cb9c44db707eb\"",
        "\"timestamp\":\"2024-10-08T13:04:56.445761285Z\"",
    ];

    let raw_str = r#"
 {
   "headStatus": "Idle",
   "hydraNodeVersion": "0.19.0-1ffe7c6b505e3f38b5546ae5e5b97de26bc70425",
   "me": {
     "vkey": "b37aabd81024c043f53a069c91e51a5b52e4ea399ae17ee1fe3cb9c44db707eb"
   },
   "seq": 2,
   "tag": "Greetings",
   "timestamp": "2024-10-08T13:04:56.445761285Z"
 }
"#;
    test_other_event_deserialization(evt, &json_parts, &raw_str)
}

#[test]
fn head_is_initializing_evt() -> TestResult {
    let evt = HydraMessage {
        seq: 2,
        payload: HydraMessagePayload::HeadIsInitializing {
            raw_json: String::from(
                "{\"timestamp\":\"2024-10-08T13:05:47.330461177Z\",\"parties\":[{\"vkey\":\"b37aabd81024c043f53a069c91e51a5b52e4ea399ae17ee1fe3cb9c44db707eb\"},{\"vkey\":\"f68e5624f885d521d2f43c3959a0de70496d5464bd3171aba8248f50d5d72b41\"},{\"vkey\":\"7abcda7de6d883e7570118c1ccc8ee2e911f2e628a41ab0685ffee15f39bba96\"}],\"tag\":\"HeadIsInitializing\",\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\"}",
            ),
        },
    };
    let json_parts = vec![
        "\"timestamp\":\"2024-10-08T13:05:47.330461177Z\"",
        "\"parties\":[{\"vkey\":\"b37aabd81024c043f53a069c91e51a5b52e4ea399ae17ee1fe3cb9c44db707eb\"},{\"vkey\":\"f68e5624f885d521d2f43c3959a0de70496d5464bd3171aba8248f50d5d72b41\"},{\"vkey\":\"7abcda7de6d883e7570118c1ccc8ee2e911f2e628a41ab0685ffee15f39bba96\"}]",
        "\"tag\":\"HeadIsInitializing\"",
        "\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\"",
    ];

    let raw_str = r#"
 {
   "headId": "84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab",
   "parties": [
     {
       "vkey": "b37aabd81024c043f53a069c91e51a5b52e4ea399ae17ee1fe3cb9c44db707eb"
     },
     {
       "vkey": "f68e5624f885d521d2f43c3959a0de70496d5464bd3171aba8248f50d5d72b41"
     },
     {
       "vkey": "7abcda7de6d883e7570118c1ccc8ee2e911f2e628a41ab0685ffee15f39bba96"
     }
   ],
   "seq": 2,
   "tag": "HeadIsInitializing",
   "timestamp": "2024-10-08T13:05:47.330461177Z"
 }
"#;
    test_other_event_deserialization(evt, &json_parts, &raw_str)
}

#[test]
fn committed_evt() -> TestResult {
    let evt = HydraMessage {
        seq: 3,
        payload: HydraMessagePayload::Committed {
            raw_json: String::from(
                "{\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\",\"timestamp\":\"2024-10-08T13:05:56.918549005Z\",\"utxo\":{\"c9a5fb7ca6f55f07facefccb7c5d824eed00ce18719d28ec4c4a2e4041e85d97#0\":{\"address\":\"addr_test1vp5cxztpc6hep9ds7fjgmle3l225tk8ske3rmwr9adu0m6qchmx5z\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":100000000}}},\"party\":{\"vkey\":\"b37aabd81024c043f53a069c91e51a5b52e4ea399ae17ee1fe3cb9c44db707eb\"},\"tag\":\"Committed\"}",
            ),
        },
    };
    let json_parts = vec![
        "\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\"",
        "\"timestamp\":\"2024-10-08T13:05:56.918549005Z\"",
        "\"utxo\":{\"c9a5fb7ca6f55f07facefccb7c5d824eed00ce18719d28ec4c4a2e4041e85d97#0\":{\"address\":\"addr_test1vp5cxztpc6hep9ds7fjgmle3l225tk8ske3rmwr9adu0m6qchmx5z\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":100000000}}}",
        "\"party\":{\"vkey\":\"b37aabd81024c043f53a069c91e51a5b52e4ea399ae17ee1fe3cb9c44db707eb\"}",
        "\"tag\":\"Committed\"",
    ];

    let raw_str = r#"
 {
   "headId": "84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab",
   "party": {
     "vkey": "b37aabd81024c043f53a069c91e51a5b52e4ea399ae17ee1fe3cb9c44db707eb"
   },
   "seq": 3,
   "tag": "Committed",
   "timestamp": "2024-10-08T13:05:56.918549005Z",
   "utxo": {
     "c9a5fb7ca6f55f07facefccb7c5d824eed00ce18719d28ec4c4a2e4041e85d97#0": {
       "address": "addr_test1vp5cxztpc6hep9ds7fjgmle3l225tk8ske3rmwr9adu0m6qchmx5z",
       "datum": null,
       "datumhash": null,
       "inlineDatum": null,
       "referenceScript": null,
       "value": {
         "lovelace": 100000000
       }
     }
   }
 }
"#;
    test_other_event_deserialization(evt, &json_parts, &raw_str)
}

#[test]
fn head_is_open_evt() -> TestResult {
    let evt = HydraMessage {
        seq: 6,
        payload: HydraMessagePayload::HeadIsOpen {
            raw_json: String::from(
                "{\"tag\":\"HeadIsOpen\",\"utxo\":{\"7b27f432e04984dc21ee61e8b1539775cd72cc8669f72cf39aebf6d87e35c697#0\":{\"address\":\"addr_test1vp0yug22dtwaxdcjdvaxr74dthlpunc57cm639578gz7algset3fh\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":50000000}},\"c9a5fb7ca6f55f07facefccb7c5d824eed00ce18719d28ec4c4a2e4041e85d97#0\":{\"address\":\"addr_test1vp5cxztpc6hep9ds7fjgmle3l225tk8ske3rmwr9adu0m6qchmx5z\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":100000000}},\"f0a39560ea80ccc68e8dffb6a4a077c8927811f06c5d9058d0fa2d1a8d047d20#0\":{\"address\":\"addr_test1vqx5tu4nzz5cuanvac4t9an4djghrx7hkdvjnnhstqm9kegvm6g6c\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":25000000}}},\"timestamp\":\"2024-10-08T13:06:18.687120539Z\",\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\"}",
            ),
        },
    };
    let json_parts = vec![
        "\"tag\":\"HeadIsOpen\"",
        "\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\"",
        "\"timestamp\":\"2024-10-08T13:06:18.687120539Z\"",
        "\"utxo\":{\"7b27f432e04984dc21ee61e8b1539775cd72cc8669f72cf39aebf6d87e35c697#0\":{\"address\":\"addr_test1vp0yug22dtwaxdcjdvaxr74dthlpunc57cm639578gz7algset3fh\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":50000000}},\"c9a5fb7ca6f55f07facefccb7c5d824eed00ce18719d28ec4c4a2e4041e85d97#0\":{\"address\":\"addr_test1vp5cxztpc6hep9ds7fjgmle3l225tk8ske3rmwr9adu0m6qchmx5z\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":100000000}},\"f0a39560ea80ccc68e8dffb6a4a077c8927811f06c5d9058d0fa2d1a8d047d20#0\":{\"address\":\"addr_test1vqx5tu4nzz5cuanvac4t9an4djghrx7hkdvjnnhstqm9kegvm6g6c\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":25000000}}}",
    ];

    let raw_str = r#"
 {
   "headId": "84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab",
   "seq": 6,
   "tag": "HeadIsOpen",
   "timestamp": "2024-10-08T13:06:18.687120539Z",
   "utxo": {
     "7b27f432e04984dc21ee61e8b1539775cd72cc8669f72cf39aebf6d87e35c697#0": {
       "address": "addr_test1vp0yug22dtwaxdcjdvaxr74dthlpunc57cm639578gz7algset3fh",
       "datum": null,
       "datumhash": null,
       "inlineDatum": null,
       "referenceScript": null,
       "value": {
         "lovelace": 50000000
       }
     },
     "c9a5fb7ca6f55f07facefccb7c5d824eed00ce18719d28ec4c4a2e4041e85d97#0": {
       "address": "addr_test1vp5cxztpc6hep9ds7fjgmle3l225tk8ske3rmwr9adu0m6qchmx5z",
       "datum": null,
       "datumhash": null,
       "inlineDatum": null,
       "referenceScript": null,
       "value": {
         "lovelace": 100000000
       }
     },
     "f0a39560ea80ccc68e8dffb6a4a077c8927811f06c5d9058d0fa2d1a8d047d20#0": {
       "address": "addr_test1vqx5tu4nzz5cuanvac4t9an4djghrx7hkdvjnnhstqm9kegvm6g6c",
       "datum": null,
       "datumhash": null,
       "inlineDatum": null,
       "referenceScript": null,
       "value": {
         "lovelace": 25000000
       }
     }
   }
 }
"#;
    test_other_event_deserialization(evt, &json_parts, &raw_str)
}

#[test]
fn head_is_closed_evt() -> TestResult {
    let evt = HydraMessage {
        seq: 9,
        payload: HydraMessagePayload::HeadIsClosed {
            raw_json: String::from(
                "{\"snapshotNumber\":1,\"tag\":\"HeadIsClosed\",\"contestationDeadline\":\"2024-10-08T13:07:37.7Z\",\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\",\"timestamp\":\"2024-10-08T13:07:31.814065753Z\"}",
            ),
        },
    };
    let json_parts = vec![
        "\"snapshotNumber\":1",
        "\"tag\":\"HeadIsClosed\"",
        "\"contestationDeadline\":\"2024-10-08T13:07:37.7Z\"",
        "\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\"",
        "\"timestamp\":\"2024-10-08T13:07:31.814065753Z\"",
    ];

    let raw_str = r#"
 {
   "contestationDeadline": "2024-10-08T13:07:37.7Z",
   "headId": "84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab",
   "seq": 9,
   "snapshotNumber": 1,
   "tag": "HeadIsClosed",
   "timestamp": "2024-10-08T13:07:31.814065753Z"
 }
"#;
    test_other_event_deserialization(evt, &json_parts, &raw_str)
}

#[test]
fn ready_to_fanout_evt() -> TestResult {
    let evt = HydraMessage {
        seq: 10,
        payload: HydraMessagePayload::ReadyToFanout {
            raw_json: String::from(
                "{\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\",\"tag\":\"ReadyToFanout\",\"timestamp\":\"2024-10-08T13:07:37.807683329Z\"}",
            ),
        },
    };
    let json_parts = vec![
        "\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\"",
        "\"tag\":\"ReadyToFanout\"",
        "\"timestamp\":\"2024-10-08T13:07:37.807683329Z\"",
    ];

    let raw_str = r#"
 {
   "headId": "84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab",
   "seq": 10,
   "tag": "ReadyToFanout",
   "timestamp": "2024-10-08T13:07:37.807683329Z"
 }
"#;
    test_other_event_deserialization(evt, &json_parts, &raw_str)
}

#[test]
fn head_is_finalized_evt() -> TestResult {
    let evt = HydraMessage {
        seq: 11,
        payload: HydraMessagePayload::HeadIsFinalized {
            raw_json: String::from(
                "{\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\",\"utxo\":{\"633777d68a85fe989f88aa839aa84743f64d68a931192c41f4df8ed0f16e03d1#0\":{\"address\":\"addr_test1vp0yug22dtwaxdcjdvaxr74dthlpunc57cm639578gz7algset3fh\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":2000000}},\"633777d68a85fe989f88aa839aa84743f64d68a931192c41f4df8ed0f16e03d1#1\":{\"address\":\"addr_test1vqx5tu4nzz5cuanvac4t9an4djghrx7hkdvjnnhstqm9kegvm6g6c\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":23000000}},\"7b27f432e04984dc21ee61e8b1539775cd72cc8669f72cf39aebf6d87e35c697#0\":{\"address\":\"addr_test1vp0yug22dtwaxdcjdvaxr74dthlpunc57cm639578gz7algset3fh\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":50000000}},\"c9a5fb7ca6f55f07facefccb7c5d824eed00ce18719d28ec4c4a2e4041e85d97#0\":{\"address\":\"addr_test1vp5cxztpc6hep9ds7fjgmle3l225tk8ske3rmwr9adu0m6qchmx5z\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":100000000}}},\"timestamp\":\"2024-10-08T13:07:40.815046135Z\",\"tag\":\"HeadIsFinalized\"}",
            ),
        },
    };
    let json_parts = vec![
        "\"headId\":\"84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab\"",
        "\"utxo\":{\"633777d68a85fe989f88aa839aa84743f64d68a931192c41f4df8ed0f16e03d1#0\":{\"address\":\"addr_test1vp0yug22dtwaxdcjdvaxr74dthlpunc57cm639578gz7algset3fh\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":2000000}},\"633777d68a85fe989f88aa839aa84743f64d68a931192c41f4df8ed0f16e03d1#1\":{\"address\":\"addr_test1vqx5tu4nzz5cuanvac4t9an4djghrx7hkdvjnnhstqm9kegvm6g6c\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":23000000}},\"7b27f432e04984dc21ee61e8b1539775cd72cc8669f72cf39aebf6d87e35c697#0\":{\"address\":\"addr_test1vp0yug22dtwaxdcjdvaxr74dthlpunc57cm639578gz7algset3fh\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":50000000}},\"c9a5fb7ca6f55f07facefccb7c5d824eed00ce18719d28ec4c4a2e4041e85d97#0\":{\"address\":\"addr_test1vp5cxztpc6hep9ds7fjgmle3l225tk8ske3rmwr9adu0m6qchmx5z\",\"datum\":null,\"datumhash\":null,\"inlineDatum\":null,\"referenceScript\":null,\"value\":{\"lovelace\":100000000}}}",
        "\"timestamp\":\"2024-10-08T13:07:40.815046135Z\"",
        "\"tag\":\"HeadIsFinalized\"",
    ];

    let raw_str = r#"
 {
   "headId": "84e657e3dd5241caac75b749195f78684023583736cc08b2896290ab",
   "seq": 11,
   "tag": "HeadIsFinalized",
   "timestamp": "2024-10-08T13:07:40.815046135Z",
   "utxo": {
     "633777d68a85fe989f88aa839aa84743f64d68a931192c41f4df8ed0f16e03d1#0": {
       "address": "addr_test1vp0yug22dtwaxdcjdvaxr74dthlpunc57cm639578gz7algset3fh",
       "datum": null,
       "datumhash": null,
       "inlineDatum": null,
       "referenceScript": null,
       "value": {
         "lovelace": 2000000
       }
     },
     "633777d68a85fe989f88aa839aa84743f64d68a931192c41f4df8ed0f16e03d1#1": {
       "address": "addr_test1vqx5tu4nzz5cuanvac4t9an4djghrx7hkdvjnnhstqm9kegvm6g6c",
       "datum": null,
       "datumhash": null,
       "inlineDatum": null,
       "referenceScript": null,
       "value": {
         "lovelace": 23000000
       }
     },
     "7b27f432e04984dc21ee61e8b1539775cd72cc8669f72cf39aebf6d87e35c697#0": {
       "address": "addr_test1vp0yug22dtwaxdcjdvaxr74dthlpunc57cm639578gz7algset3fh",
       "datum": null,
       "datumhash": null,
       "inlineDatum": null,
       "referenceScript": null,
       "value": {
         "lovelace": 50000000
       }
     },
     "c9a5fb7ca6f55f07facefccb7c5d824eed00ce18719d28ec4c4a2e4041e85d97#0": {
       "address": "addr_test1vp5cxztpc6hep9ds7fjgmle3l225tk8ske3rmwr9adu0m6qchmx5z",
       "datum": null,
       "datumhash": null,
       "inlineDatum": null,
       "referenceScript": null,
       "value": {
         "lovelace": 100000000
       }
     }
   }
}
"#;
    test_other_event_deserialization(evt, &json_parts, &raw_str)
}
