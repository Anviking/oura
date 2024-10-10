use std::fs;

use oura::sources::hydra::{HydraMessage, HydraMessagePayload};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run_scenario(_expected_msgs: &[HydraMessage], expected_file: &str) -> TestResult {
    let _file = fs::read_to_string(expected_file)?;
    Ok(())
}

fn test_event_deserialization(expected: HydraMessage, input: &str) -> TestResult {
    let deserialized: HydraMessage = serde_json::from_str(&input)?;
    assert_eq!(deserialized, expected);
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
fn peer_connected_evt_1() -> TestResult {
    let evt = HydraMessage {
        seq: 0,
        payload: HydraMessagePayload::PeerConnected {
            peer: String::from("3"),
        },
    };

    let raw_str = r#"{"peer":"3","seq":0,"tag":"PeerConnected","timestamp":"2024-10-08T13:01:20.556003751Z"}"#;
    test_event_deserialization(evt, &raw_str)
}

#[test]
fn peer_connected_evt_2() -> TestResult {
    let evt = HydraMessage {
        seq: 1,
        payload: HydraMessagePayload::PeerConnected {
            peer: String::from("0"),
        },
    };

    let raw_str = r#"{"peer":"0","seq":1,"tag":"PeerConnected","timestamp":"2024-10-08T13:19:06.954897681Z"}"#;
    test_event_deserialization(evt, &raw_str)
}
