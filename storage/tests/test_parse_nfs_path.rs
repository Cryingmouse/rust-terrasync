use storage::parse_nfs_path;

#[cfg(test)]
mod tests {
    use super::*;
    use nfs3_client::nfs3_types::portmap::PMAP_PORT;

    #[test]
    fn test_nfs_url_format_basic() {
        let result = parse_nfs_path("nfs://server/path");
        assert_eq!(result, ("server".to_string(), PMAP_PORT, "/path".to_string()));
    }

    #[test]
    fn test_nfs_url_format_with_port() {
        let result = parse_nfs_path("nfs://server:2049/path");
        assert_eq!(result, ("server".to_string(), 2049, "/path".to_string()));
    }

    #[test]
    fn test_nfs_url_format_with_subpath() {
        let result = parse_nfs_path("nfs://192.168.1.100/home/user/data");
        assert_eq!(result, ("192.168.1.100".to_string(), PMAP_PORT, "/home/user/data".to_string()));
    }

    #[test]
    fn test_nfs_url_format_with_port_and_complex_path() {
        let result = parse_nfs_path("nfs://my-server:1234/mnt/nfs/share/data");
        assert_eq!(result, ("my-server".to_string(), 1234, "/mnt/nfs/share/data".to_string()));
    }

    #[test]
    fn test_nfs_url_format_root_path() {
        let result = parse_nfs_path("nfs://server/");
        assert_eq!(result, ("server".to_string(), PMAP_PORT, "/".to_string()));
    }

    #[test]
    fn test_nfs_url_format_with_spaces() {
        let result = parse_nfs_path("  nfs://server:2049/path  ");
        assert_eq!(result, ("server".to_string(), 2049, "/path".to_string()));
    }

    #[test]
    fn test_traditional_format_server_port_path() {
        let result = parse_nfs_path("server:2049:/path");
        assert_eq!(result, ("server".to_string(), 2049, "/path".to_string()));
    }

    #[test]
    fn test_traditional_format_server_path() {
        let result = parse_nfs_path("server:/path");
        assert_eq!(result, ("server".to_string(), PMAP_PORT, "/path".to_string()));
    }

    #[test]
    fn test_traditional_format_server_only() {
        let result = parse_nfs_path("server");
        assert_eq!(result, ("server".to_string(), PMAP_PORT, "/".to_string()));
    }

    #[test]
    fn test_traditional_format_complex_path() {
        let result = parse_nfs_path("192.168.1.100:2049:/mnt/nfs/data/share");
        assert_eq!(result, ("192.168.1.100".to_string(), 2049, "/mnt/nfs/data/share".to_string()));
    }

    #[test]
    fn test_traditional_format_path_with_colon() {
        let result = parse_nfs_path("server:2049:/path:with:colons");
        assert_eq!(result, ("server".to_string(), 2049, "/path:with:colons".to_string()));
    }

    #[test]
    fn test_traditional_format_with_spaces() {
        let result = parse_nfs_path("  server  :  2049  :  /path  ");
        assert_eq!(result, ("server".to_string(), 2049, "/path".to_string()));
    }

    #[test]
    fn test_relative_path_normalization() {
        let result = parse_nfs_path("server:path");
        assert_eq!(result, ("server".to_string(), PMAP_PORT, "/path".to_string()));
    }

    #[test]
    fn test_relative_path_normalization_with_port() {
        let result = parse_nfs_path("server:1234:relative/path");
        assert_eq!(result, ("server".to_string(), 1234, "/relative/path".to_string()));
    }

    // 错误测试用例
    #[test]
    #[should_panic(expected = "无效的NFS路径: 空字符串")]
    fn test_empty_string() {
        parse_nfs_path("");
    }

    #[test]
    #[should_panic(expected = "无效的NFS路径: 空字符串")]
    fn test_whitespace_only() {
        parse_nfs_path("   ");
    }

    #[test]
    #[should_panic(expected = "无效的NFS URL格式: 缺少路径部分")]
    fn test_nfs_url_missing_path() {
        parse_nfs_path("nfs://server");
    }

    #[test]
    #[should_panic(expected = "无效的NFS路径: 服务器名不能为空")]
    fn test_nfs_url_empty_server() {
        parse_nfs_path("nfs://:2049/path");
    }

    #[test]
    #[should_panic(expected = "无效的端口号")]
    fn test_nfs_url_invalid_port() {
        parse_nfs_path("nfs://server:invalid/path");
    }

    #[test]
    #[should_panic(expected = "无效的NFS路径: 服务器名不能为空")]
    fn test_traditional_empty_server() {
        parse_nfs_path(":2049:/path");
    }

    #[test]
    #[should_panic(expected = "无效的端口号")]
    fn test_traditional_invalid_port() {
        parse_nfs_path("server:invalid:/path");
    }

    #[test]
    #[should_panic(expected = "无效的NFS路径: 路径不能为空")]
    fn test_traditional_empty_path() {
        parse_nfs_path("server:2049:");
    }

    #[test]
    #[should_panic(expected = "无效的NFS路径: 服务器名不能为空")]
    fn test_traditional_server_only_empty() {
        parse_nfs_path(":");
    }

    #[test]
    #[should_panic(expected = "无效的NFS路径: 路径不能为空")]
    fn test_server_path_empty_path() {
        parse_nfs_path("server:");
    }

    // 边界测试用例
    #[test]
    fn test_ipv4_address() {
        let result = parse_nfs_path("nfs://192.168.1.1/path");
        assert_eq!(result, ("192.168.1.1".to_string(), PMAP_PORT, "/path".to_string()));
    }

    #[test]
    fn test_ipv6_address_like() {
        let result = parse_nfs_path("nfs://ipv6-server:2049/path");
        assert_eq!(result, ("ipv6-server".to_string(), 2049, "/path".to_string()));
    }

    #[test]
    fn test_max_port_number() {
        let result = parse_nfs_path("nfs://server:65535/path");
        assert_eq!(result, ("server".to_string(), 65535, "/path".to_string()));
    }

    #[test]
    fn test_min_port_number() {
        let result = parse_nfs_path("nfs://server:0/path");
        assert_eq!(result, ("server".to_string(), 0, "/path".to_string()));
    }

    #[test]
    fn test_very_long_path() {
        let long_path = "a".repeat(1000);
        let input = format!("nfs://server/{}" , long_path);
        let result = parse_nfs_path(&input);
        assert_eq!(result.0, "server");
        assert_eq!(result.1, PMAP_PORT);
        assert!(result.2.len() > 1000);
    }

    // 实际使用场景测试
    #[test]
    fn test_docker_nfs_example() {
        let result = parse_nfs_path("nfs://nfs-server/var/nfs");
        assert_eq!(result, ("nfs-server".to_string(), PMAP_PORT, "/var/nfs".to_string()));
    }

    #[test]
    fn test_kubernetes_nfs_example() {
        let result = parse_nfs_path("nfs://10.0.0.100:2049/export/data");
        assert_eq!(result, ("10.0.0.100".to_string(), 2049, "/export/data".to_string()));
    }

    #[test]
    fn test_aws_efs_example() {
        let result = parse_nfs_path("fs-12345.efs.us-east-1.amazonaws.com:/");
        assert_eq!(result, ("fs-12345.efs.us-east-1.amazonaws.com".to_string(), PMAP_PORT, "/".to_string()));
    }
}