#[cfg(test)]
mod tests {
    use crate::scan::{scan, ScanParams};
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

        scan(params).await.unwrap();
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

        scan(params).await.unwrap();
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

        scan(params).await.unwrap();
    }
}
