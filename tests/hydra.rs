use std::fs;

use oura::sources::hydra::HydraMessage;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run_scenario(_expected_msgs: &[HydraMessage], expected_file: &str) -> TestResult {
    let _file = fs::read_to_string(expected_file)?;
    Ok(())
}

#[test]
fn hydra_scenario_1() -> TestResult {
    run_scenario(&[HydraMessage::SomethingElse], "tests/hydra/scenario_1.txt")
}

#[test]
fn hydra_scenario_2() -> TestResult {
    run_scenario(&[HydraMessage::SomethingElse], "tests/hydra/scenario_2.txt")
}
