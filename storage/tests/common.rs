use std::fs;
use tempfile::TempDir;

/// 创建测试用的临时目录结构
pub fn create_test_structure() -> TempDir {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    // 创建目录结构
    fs::create_dir_all(root.join("dir1/subdir1")).unwrap();
    fs::create_dir_all(root.join("dir2/subdir2")).unwrap();
    fs::create_dir_all(root.join("empty_dir")).unwrap();
    fs::create_dir_all(root.join("deep/nested/structure/here")).unwrap();

    // 创建文件
    fs::write(root.join("file1.txt"), b"content1").unwrap();
    fs::write(root.join("file2.txt"), b"content2").unwrap();
    fs::write(root.join("dir1/file3.txt"), b"content3").unwrap();
    fs::write(root.join("dir1/subdir1/file4.txt"), b"content4").unwrap();
    fs::write(root.join("dir2/file5.txt"), b"content5").unwrap();
    fs::write(root.join("dir2/subdir2/file6.txt"), b"content6").unwrap();
    fs::write(
        root.join("deep/nested/structure/here/file7.txt"),
        b"content7",
    )
    .unwrap();

    // 创建空文件
    fs::write(root.join("empty_file.txt"), b"").unwrap();

    // 创建二进制文件
    fs::write(root.join("binary.dat"), vec![0u8; 1024]).unwrap();

    temp_dir
}

/// 创建大型测试结构用于性能测试
pub fn create_large_test_structure() -> TempDir {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    // 创建100个文件和目录
    for i in 0..20 {
        let dir_name = format!("dir_{:02}", i);
        fs::create_dir_all(root.join(&dir_name)).unwrap();

        for j in 0..5 {
            let file_name = format!("file_{:02}_{:02}.txt", i, j);
            fs::write(
                root.join(&dir_name).join(&file_name),
                format!("content_{}_{}", i, j),
            )
            .unwrap();
        }
    }

    temp_dir
}
