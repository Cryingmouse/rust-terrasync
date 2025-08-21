#[cfg(test)]
mod tests {
    use crate::scan::{ScanParams, scan};
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_walkdir_basic() {
        let temp_dir = tempdir().unwrap();
        let root_path = temp_dir.path();

        // 创建测试文件结构
        fs::create_dir_all(root_path.join("dir1")).unwrap();
        fs::create_dir_all(root_path.join("dir2")).unwrap();
        fs::write(root_path.join("file1.txt"), "content1").unwrap();
        fs::write(root_path.join("file2.log"), "content2").unwrap();
        fs::write(root_path.join("dir1").join("file3.txt"), "content3").unwrap();

        let params = ScanParams {
            path: root_path.to_string_lossy().into_owned(),
            match_expressions: vec!["name like \"%.txt\"".to_string()],
            ..Default::default()
        };

        let results = scan(params).await.unwrap();
        
        // 应该找到2个txt文件
        assert_eq!(results.len(), 2);
        
        let file_names: Vec<_> = results.iter().map(|r| r.file_name.clone()).collect();
        assert!(file_names.contains(&"file1.txt".to_string()));
        assert!(file_names.contains(&"file3.txt".to_string()));
        
        // 验证路径分隔符处理
        for result in &results {
            assert!(!result.file_path.contains('\\'), "路径应该使用正斜杠");
        }
    }

    #[tokio::test]
    async fn test_walkdir_directory_separator() {
        let temp_dir = tempdir().unwrap();
        let root_path = temp_dir.path();

        // 创建目录结构
        fs::create_dir_all(root_path.join("test_dir")).unwrap();
        fs::write(root_path.join("test_dir").join("file.txt"), "content").unwrap();

        let params = ScanParams {
            path: root_path.to_string_lossy().into_owned(),
            match_expressions: vec!["type==dir".to_string()],
            ..Default::default()
        };

        let results = scan(params).await.unwrap();
        
        // 找到目录
        let dir_result = results.iter().find(|r| r.is_dir && r.file_name == "test_dir").unwrap();
        
        // 验证目录路径以斜杠结尾
        assert!(dir_result.file_path.ends_with("test_dir/"), "目录路径应以斜杠结尾");
        
        // 验证使用正斜杠
        assert!(!dir_result.file_path.contains('\\'), "路径应该使用正斜杠");
    }

    #[tokio::test]
    async fn test_walkdir_with_exclude() {
        let temp_dir = tempdir().unwrap();
        let root_path = temp_dir.path();

        // 创建测试文件结构
        fs::create_dir_all(root_path.join("logs")).unwrap();
        fs::write(root_path.join("app.log"), "log content").unwrap();
        fs::write(root_path.join("debug.log"), "debug content").unwrap();
        fs::write(root_path.join("data.txt"), "data content").unwrap();

        let params = ScanParams {
            path: root_path.to_string_lossy().into_owned(),
            match_expressions: vec!["type==file".to_string()],
            exclude_expressions: vec!["name like \"%.log\"".to_string()],
            ..Default::default()
        };

        let results = scan(params).await.unwrap();
        
        // 应该只找到data.txt，排除所有.log文件
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].file_name, "data.txt");
    }

    #[tokio::test]
    async fn test_walkdir_empty_filters() {
        let temp_dir = tempdir().unwrap();
        let root_path = temp_dir.path();

        // 创建测试文件结构
        fs::write(root_path.join("file1.txt"), "content1").unwrap();
        fs::write(root_path.join("file2.log"), "content2").unwrap();

        let params = ScanParams {
            path: root_path.to_string_lossy().into_owned(),
            ..Default::default()
        };

        let results = scan(params).await.unwrap();
        
        // 没有过滤条件时，应该返回所有文件和目录
        assert!(results.len() >= 2); // 至少2个文件
    }
}