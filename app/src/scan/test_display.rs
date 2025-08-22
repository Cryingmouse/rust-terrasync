use super::stats::ScanStats;

#[test]
fn test_display_format() {
    let mut stats = ScanStats::default();
    
    // 设置一些测试数据
    stats.total_files = 425;
    stats.total_dirs = 230;
    stats.total_size = 34856960; // 33.24 MiB
    stats.total_name_length = 21750; // 平均32 * 680个文件
    stats.max_name_length = 50;
    stats.total_dir_depth = 460; // 平均2 * 230个目录
    stats.max_dir_depth = 5;
    
    println!("{}", stats);
}