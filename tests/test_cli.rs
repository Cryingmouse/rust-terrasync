#[cfg(test)]
extern crate assert_cmd;
extern crate predicates;

use assert_cmd::prelude::*;
use predicates::prelude::*;

use std::process::Command;

#[test]
fn test_cli() {
    let mut cmd = Command::cargo_bin("rust-terrasync").expect("Calling binary failed");
    cmd.assert().failure();
}

#[test]
fn test_version() {
    let expected_version = "rust-terrasync 2.0.1\n";
    let mut cmd = Command::cargo_bin("rust-terrasync").expect("Calling binary failed");
    cmd.arg("--version").assert().stdout(expected_version);
}

#[test]
fn test_scan_exit_code() {
    let mut cmd = Command::cargo_bin("rust-terrasync").expect("Calling binary failed");
    cmd.arg("scan").assert().code(0);
}

#[test]
fn test_scan_stdout() {
    let scan_predicate =
        predicate::function(|x: &str| x == "Scan completed successfully!\n" || x == "Scan completed with issues!\n");
    let mut cmd = Command::cargo_bin("rust-terrasync").expect("Calling binary failed");
    cmd.arg("scan").assert().stdout(scan_predicate);
}
