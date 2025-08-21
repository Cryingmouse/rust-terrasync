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
    let mut cmd = Command::cargo_bin("rust-terrasync").expect("Calling binary failed");
    cmd.arg("--version").assert().failure(); // 版本参数不被支持，应该失败
}

#[test]
fn test_scan_exit_code() {
    let mut cmd = Command::cargo_bin("rust-terrasync").expect("Calling binary failed");
    cmd.arg("scan").assert().code(0);
}

#[test]
fn test_scan_stdout() {
    let scan_predicate = predicate::function(|x: &str| x.contains("Walkdir completed"));
    let mut cmd = Command::cargo_bin("rust-terrasync").expect("Calling binary failed");
    cmd.arg("scan").assert().stdout(scan_predicate);
}
