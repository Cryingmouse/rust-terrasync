use serde::{Deserialize, Serialize};
use utils::error::Result;

/// Filter expression for file/directory matching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterExpression {
    /// Raw expression string
    pub expression: String,

    /// Parsed conditions
    pub conditions: Vec<FilterCondition>,
}

/// Individual filter condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterCondition {
    /// Name matching (exact, contains, starts_with, ends_with, like)
    Name {
        operator: String, // "==", "!=", "contains", "starts_with", "ends_with", "like", "in"
        value: String,
    },

    /// Path matching
    Path {
        operator: String, // "==", "!=", "contains", "starts_with", "ends_with", "like", "in"
        value: String,
    },

    /// File type matching
    Type {
        operator: String, // "=="
        value: String,    // "file", "dir", "symlink"
    },

    /// Modification time (days)
    Modified {
        operator: String, // "<", ">", "<=", ">="
        value: f64,
    },

    /// File size (bytes)
    Size {
        operator: String, // "<", ">", "<=", ">="
        value: u64,
    },

    /// Extension matching
    Extension {
        operator: String, // "==", "!=", "contains", "like"
        value: String,
    },
}

/// Parse a filter expression string
pub fn parse_filter_expression(expr: &str) -> Result<FilterExpression> {
    let expr = expr.trim();
    let mut conditions = Vec::new();

    // Split by "and" to handle multiple conditions
    let parts: Vec<&str> = expr.split("and").map(|s| s.trim()).collect();

    for part in parts {
        if part.is_empty() {
            continue;
        }

        // Parse each condition
        if let Some(condition) = parse_single_condition(part)? {
            conditions.push(condition);
        }
    }

    Ok(FilterExpression {
        expression: expr.to_string(),
        conditions,
    })
}

/// Parse a single filter condition
fn parse_single_condition(expr: &str) -> Result<Option<FilterCondition>> {
    let expr = expr.trim();

    // Handle "in" operator for name and path
    if let Some(pos) = expr.find(" in name") {
        let value = extract_quoted_value(&expr[..pos], "");
        return Ok(Some(FilterCondition::Name {
            operator: "contains".to_string(),
            value,
        }));
    }

    if let Some(pos) = expr.find(" in path") {
        let value = extract_quoted_value(&expr[..pos], "");
        return Ok(Some(FilterCondition::Path {
            operator: "contains".to_string(),
            value,
        }));
    }

    // Handle like operator
    if let Some(pos) = expr.find(" like ") {
        let field = expr[..pos].trim();
        let value = extract_quoted_value(&expr[pos + 6..], "");
        match field {
            "name" => {
                return Ok(Some(FilterCondition::Name {
                    operator: "like".to_string(),
                    value,
                }))
            }
            "path" => {
                return Ok(Some(FilterCondition::Path {
                    operator: "like".to_string(),
                    value,
                }))
            }
            "extension" => {
                return Ok(Some(FilterCondition::Extension {
                    operator: "like".to_string(),
                    value,
                }))
            }
            _ => {}
        }
    }

    // Handle comparison operators
    let operators = ["==", "!=", "<=", ">=", "<", ">"];
    for op in operators.iter() {
        if let Some(pos) = expr.find(op) {
            let field = expr[..pos].trim();
            let value = expr[pos + op.len()..].trim();

            match field {
                "name" => {
                    let value = extract_quoted_value(value, "");
                    return Ok(Some(FilterCondition::Name {
                        operator: op.to_string(),
                        value,
                    }));
                }
                "path" => {
                    let value = extract_quoted_value(value, "");
                    return Ok(Some(FilterCondition::Path {
                        operator: op.to_string(),
                        value,
                    }));
                }
                "type" => {
                    let value = extract_quoted_value(value, "");
                    return Ok(Some(FilterCondition::Type {
                        operator: op.to_string(),
                        value,
                    }));
                }
                "modified" => {
                    let value = value.parse::<f64>().map_err(|e| {
                        utils::error::Error::new(&format!("Failed to parse modified value: {}", e))
                    })?;
                    return Ok(Some(FilterCondition::Modified {
                        operator: op.to_string(),
                        value,
                    }));
                }
                "size" => {
                    let value = value.parse::<u64>().map_err(|e| {
                        utils::error::Error::new(&format!("Failed to parse size value: {}", e))
                    })?;
                    return Ok(Some(FilterCondition::Size {
                        operator: op.to_string(),
                        value,
                    }));
                }
                "extension" => {
                    let value = extract_quoted_value(value, "");
                    return Ok(Some(FilterCondition::Extension {
                        operator: op.to_string(),
                        value,
                    }));
                }
                _ => {}
            }
        }
    }

    // Handle contains/starts_with/ends_with keywords
    if let Some(pos) = expr.find(" contains ") {
        let field = expr[..pos].trim();
        let value = extract_quoted_value(&expr[pos + 9..], "");
        match field {
            "name" => {
                return Ok(Some(FilterCondition::Name {
                    operator: "contains".to_string(),
                    value,
                }))
            }
            "path" => {
                return Ok(Some(FilterCondition::Path {
                    operator: "contains".to_string(),
                    value,
                }))
            }
            "extension" => {
                return Ok(Some(FilterCondition::Extension {
                    operator: "contains".to_string(),
                    value,
                }))
            }
            _ => {}
        }
    }

    if let Some(pos) = expr.find(" starts with ") {
        let field = expr[..pos].trim();
        let value = extract_quoted_value(&expr[pos + 12..], "");
        match field {
            "name" => {
                return Ok(Some(FilterCondition::Name {
                    operator: "starts_with".to_string(),
                    value,
                }))
            }
            "path" => {
                return Ok(Some(FilterCondition::Path {
                    operator: "starts_with".to_string(),
                    value,
                }))
            }
            _ => {}
        }
    }

    if let Some(pos) = expr.find(" ends with ") {
        let field = expr[..pos].trim();
        let value = extract_quoted_value(&expr[pos + 10..], "");
        match field {
            "name" => {
                return Ok(Some(FilterCondition::Name {
                    operator: "ends_with".to_string(),
                    value,
                }))
            }
            "path" => {
                return Ok(Some(FilterCondition::Path {
                    operator: "ends_with".to_string(),
                    value,
                }))
            }
            _ => {}
        }
    }

    Ok(None)
}

/// Evaluate a filter expression against file metadata
pub fn evaluate_filter(
    expr: &FilterExpression, file_name: &str, file_path: &str, file_type: &str, modified_days: f64,
    size: u64, extension: &str,
) -> bool {
    for condition in &expr.conditions {
        match condition {
            FilterCondition::Name { operator, value } => {
                let result = match operator.as_str() {
                    "==" => file_name == value,
                    "!=" => file_name != value,
                    "contains" | "in" => file_name.contains(value),
                    "starts_with" => file_name.starts_with(value),
                    "ends_with" => file_name.ends_with(value),
                    "like" => {
                        // Simple like pattern matching (supports % as wildcard)
                        if value.starts_with('%') && value.ends_with('%') {
                            let pattern = &value[1..value.len() - 1];
                            file_name.contains(pattern)
                        } else if value.starts_with('%') {
                            let pattern = &value[1..];
                            file_name.ends_with(pattern)
                        } else if value.ends_with('%') {
                            let pattern = &value[..value.len() - 1];
                            file_name.starts_with(pattern)
                        } else if value.contains('%') {
                            // Handle patterns like "doc%.txt" where % is in the middle
                            let parts: Vec<&str> = value.split('%').collect();
                            if parts.len() == 2 {
                                let prefix = parts[0];
                                let suffix = parts[1];
                                file_name.starts_with(prefix) && file_name.ends_with(suffix)
                            } else {
                                file_name.contains('%')
                            }
                        } else {
                            file_name == value
                        }
                    }
                    _ => false,
                };
                if !result {
                    return false;
                }
            }
            FilterCondition::Path { operator, value } => {
                let result = match operator.as_str() {
                    "==" => file_path == value,
                    "!=" => file_path != value,
                    "contains" | "in" => file_path.contains(value),
                    "starts_with" => file_path.starts_with(value),
                    "ends_with" => file_path.ends_with(value),
                    "like" => {
                        // Simple like pattern matching (supports % as wildcard)
                        if value.starts_with('%') && value.ends_with('%') {
                            let pattern = &value[1..value.len() - 1];
                            file_path.contains(pattern)
                        } else if value.starts_with('%') {
                            let pattern = &value[1..];
                            file_path.ends_with(pattern)
                        } else if value.ends_with('%') {
                            let pattern = &value[..value.len() - 1];
                            file_path.starts_with(pattern)
                        } else if value.contains('%') {
                            // Handle patterns like "doc%.txt" where % is in the middle
                            let parts: Vec<&str> = value.split('%').collect();
                            if parts.len() == 2 {
                                let prefix = parts[0];
                                let suffix = parts[1];
                                file_path.starts_with(prefix) && file_path.ends_with(suffix)
                            } else {
                                file_path.contains('%')
                            }
                        } else {
                            file_path == value
                        }
                    }
                    _ => false,
                };
                if !result {
                    return false;
                }
            }
            FilterCondition::Type { operator, value } => {
                let result = match operator.as_str() {
                    "==" => file_type == value,
                    _ => false,
                };
                if !result {
                    return false;
                }
            }
            FilterCondition::Modified { operator, value } => {
                let result = match operator.as_str() {
                    "<" => modified_days < *value,
                    ">" => modified_days > *value,
                    "<=" => modified_days <= *value,
                    ">=" => modified_days >= *value,
                    _ => false,
                };
                if !result {
                    return false;
                }
            }
            FilterCondition::Size { operator, value } => {
                let result = match operator.as_str() {
                    "<" => size < *value,
                    ">" => size > *value,
                    "<=" => size <= *value,
                    ">=" => size >= *value,
                    _ => false,
                };
                if !result {
                    return false;
                }
            }
            FilterCondition::Extension { operator, value } => {
                let result = match operator.as_str() {
                    "==" => extension == value,
                    "!=" => extension != value,
                    "contains" => extension.contains(value),
                    "like" => {
                        // Simple like pattern matching (supports % as wildcard)
                        if value.starts_with('%') && value.ends_with('%') {
                            let pattern = &value[1..value.len() - 1];
                            extension.contains(pattern)
                        } else if value.starts_with('%') {
                            let pattern = &value[1..];
                            extension.ends_with(pattern)
                        } else if value.ends_with('%') {
                            let pattern = &value[..value.len() - 1];
                            extension.starts_with(pattern)
                        } else if value.contains('%') {
                            // Handle patterns like "doc%.txt" where % is in the middle
                            let parts: Vec<&str> = value.split('%').collect();
                            if parts.len() == 2 {
                                let prefix = parts[0];
                                let suffix = parts[1];
                                extension.starts_with(prefix) && extension.ends_with(suffix)
                            } else {
                                extension.contains('%')
                            }
                        } else {
                            extension == value
                        }
                    }
                    _ => false,
                };
                if !result {
                    return false;
                }
            }
        }
    }

    true
}

/// Extract quoted string value from expression
fn extract_quoted_value(expr: &str, prefix: &str) -> String {
    if !prefix.is_empty() {
        if let Some(start) = expr.find(prefix) {
            let rest = &expr[start + prefix.len()..];
            return extract_quoted_value(rest, "");
        }
    }

    let rest = expr.trim_start();

    // Handle both single and double quotes
    for quote_char in &['"', '\''] {
        if let Some(quote_start) = rest.find(*quote_char) {
            let after_quote = &rest[quote_start + 1..];
            if let Some(quote_end) = after_quote.find(*quote_char) {
                return after_quote[..quote_end].to_string();
            }
        }
    }

    // 如果没有引号，尝试提取下一个token
    rest.split_whitespace().next().unwrap_or("").to_string()
}

/// Test module for filter functionality
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_name_equals() {
        let expr = parse_filter_expression("name==\"test.txt\"").unwrap();
        assert_eq!(expr.conditions.len(), 1);
        match &expr.conditions[0] {
            FilterCondition::Name { operator, value } => {
                assert_eq!(operator, "==");
                assert_eq!(value, "test.txt");
            }
            _ => panic!("Expected Name condition"),
        }
    }

    #[test]
    fn test_parse_type_condition() {
        let expr = parse_filter_expression("type==\"file\"").unwrap();
        assert_eq!(expr.conditions.len(), 1);
        match &expr.conditions[0] {
            FilterCondition::Type { operator, value } => {
                assert_eq!(operator, "==");
                assert_eq!(value, "file");
            }
            _ => panic!("Expected Type condition"),
        }
    }

    #[test]
    fn test_parse_modified_condition() {
        let expr = parse_filter_expression("modified<0.5").unwrap();
        assert_eq!(expr.conditions.len(), 1);
        match &expr.conditions[0] {
            FilterCondition::Modified { operator, value } => {
                assert_eq!(operator, "<");
                assert_eq!(*value, 0.5);
            }
            _ => panic!("Expected Modified condition"),
        }
    }

    #[test]
    fn test_evaluate_filter() {
        let expr = parse_filter_expression("name==\"test.txt\" and type==file").unwrap();

        // 应该匹配
        assert!(evaluate_filter(
            &expr,
            "test.txt",
            "/path/test.txt",
            "file",
            0.0,
            100,
            "txt"
        ));

        // 不应该匹配 - 名字不匹配
        assert!(!evaluate_filter(
            &expr,
            "other.txt",
            "/path/other.txt",
            "file",
            0.0,
            100,
            "txt"
        ));

        // 不应该匹配 - 类型不匹配
        assert!(!evaluate_filter(
            &expr,
            "test.txt",
            "/path/test.txt",
            "dir",
            0.0,
            100,
            "txt"
        ));
    }

    #[test]
    fn test_path_conditions() {
        let expr = parse_filter_expression("path contains \"netapp\"").unwrap();

        // 应该匹配
        assert!(evaluate_filter(
            &expr,
            "file.txt",
            "/netapp/data/file.txt",
            "file",
            0.0,
            100,
            "txt"
        ));

        // 不应该匹配
        assert!(!evaluate_filter(
            &expr,
            "file.txt",
            "/local/data/file.txt",
            "file",
            0.0,
            100,
            "txt"
        ));
    }

    #[test]
    fn test_like_operator() {
        // Test like operator with wildcards
        let expr = parse_filter_expression("name like \"%.txt\"").unwrap();
        assert!(evaluate_filter(
            &expr,
            "document.txt",
            "/path/document.txt",
            "file",
            0.0,
            100,
            "txt"
        ));

        let expr = parse_filter_expression("name like \"doc%.txt\"").unwrap();
        assert!(evaluate_filter(
            &expr,
            "document.txt",
            "/path/document.txt",
            "file",
            0.0,
            100,
            "txt"
        ));

        let expr = parse_filter_expression("name like \"%document%\"").unwrap();
        assert!(evaluate_filter(
            &expr,
            "my_document.txt",
            "/path/my_document.txt",
            "file",
            0.0,
            100,
            "txt"
        ));
    }

    #[test]
    fn test_in_operator() {
        let expr = parse_filter_expression("\"netapp\" in name").unwrap();

        // 应该匹配
        assert!(evaluate_filter(
            &expr,
            "netapp_config.txt",
            "/path/netapp_config.txt",
            "file",
            0.0,
            100,
            "txt"
        ));

        // 不应该匹配
        assert!(!evaluate_filter(
            &expr,
            "config.txt",
            "/path/config.txt",
            "file",
            0.0,
            100,
            "txt"
        ));
    }
}
