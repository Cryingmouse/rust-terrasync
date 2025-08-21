//! ACL (Access Control List) functionality - temporarily disabled to prevent antivirus alerts

use serde::{Deserialize, Serialize};

/// File ACL information structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileAclInfo {
    pub owner: String,
    pub group: String,
    pub permissions: Vec<AclEntry>,
}

/// ACL entry structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclEntry {
    pub access_type: AclAccessType,
    pub account: String,
    pub rights: Vec<String>,
    pub is_inherited: bool,
}

/// ACL access type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AclAccessType {
    Allow,
    Deny,
}

/// Check if file is readable - always returns true when disabled
pub fn is_file_readable<P: AsRef<std::path::Path>>(_file_path: P) -> bool {
    true
}

/// Get file ACL information - returns empty/default info when disabled
pub fn get_file_acl<P: AsRef<std::path::Path>>(_file_path: P) -> Result<FileAclInfo, Box<dyn std::error::Error>> {
    Ok(FileAclInfo {
        owner: "Disabled".to_string(),
        group: "Disabled".to_string(),
        permissions: Vec::new(),
    })
}