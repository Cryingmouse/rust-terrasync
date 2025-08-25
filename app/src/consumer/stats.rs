use crate::scan::ScanParams;
use serde::Serialize;
use std::fmt;
use std::path::Path;

/// 扫描统计结构体 - 整体统计信息
#[derive(Debug, Clone, Serialize)]
pub struct ScanStats {
    // 基本统计
    pub total_files: usize,
    pub total_dirs: usize,
    pub matched_files: usize,
    pub matched_dirs: usize,
    pub total_size: i64, // 总大小（字节）

    // 扩展统计信息
    pub total_symlink: i64,      // 符号链接总数
    pub total_regular_file: i64, // 普通文件总数
    pub total_name_length: i64,  // 总文件名长度
    pub max_name_length: usize,  // 最大文件名长度
    pub total_dir_depth: i64,    // 总目录深度
    pub max_dir_depth: usize,    // 最大目录深度

    // 显示相关元数据
    pub command: String,
    pub job_id: String,
    pub log_path: String,
    pub total_time: String,
}

impl ScanStats {
    /// 根据ScanParams构建完整的命令字符串
    pub fn build_command(params: &ScanParams) -> String {
        let mut command_parts = vec![format!("terrasync scan \"{}\"", params.path)];

        if let Some(id) = &params.id {
            command_parts.push(format!("--id \"{}\"", id));
        }
        if params.depth > 0 {
            command_parts.push(format!("--depth {}", params.depth));
        }
        if !params.match_expressions.is_empty() {
            command_parts.push(format!(
                "--expression \"{}\"",
                params.match_expressions.join(" \"")
            ));
        }
        if !params.exclude_expressions.is_empty() {
            command_parts.push(format!(
                "--exclude \"{}\"",
                params.exclude_expressions.join(" \"")
            ));
        }

        command_parts.join(" ")
    }

    /// 构建日志文件路径（使用当前执行目录）
    pub fn build_log_path() -> String {
        let current_dir = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        current_dir
            .join("terrasync.log")
            .to_string_lossy()
            .to_string()
    }

    /// 从另一个ScanStats合并统计信息（保留显示元数据）
    pub fn merge_from(&mut self, other: &ScanStats) {
        self.total_files = other.total_files;
        self.total_dirs = other.total_dirs;
        self.matched_files = other.matched_files;
        self.matched_dirs = other.matched_dirs;
        self.total_size = other.total_size;
        self.total_symlink = other.total_symlink;
        self.total_regular_file = other.total_regular_file;
        self.total_name_length = other.total_name_length;
        self.max_name_length = self.max_name_length.max(other.max_name_length);
        self.total_dir_depth = other.total_dir_depth;
        self.max_dir_depth = self.max_dir_depth.max(other.max_dir_depth);
    }
}

impl Default for ScanStats {
    fn default() -> Self {
        Self {
            // 基本统计
            total_files: 0,
            total_dirs: 0,
            matched_files: 0,
            matched_dirs: 0,

            // 扩展统计信息
            total_size: 0,
            total_symlink: 0,
            total_regular_file: 0,
            total_name_length: 0,
            max_name_length: 0,
            total_dir_depth: 0,
            max_dir_depth: 0,

            // 显示相关元数据
            command: String::from("terrasync scan"),
            job_id: String::new(),
            log_path: String::new(),
            total_time: String::from("0s"),
        }
    }
}

impl fmt::Display for ScanStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_items = self.total_files + self.total_dirs;
        let avg_file_size = if self.total_files > 0 {
            self.total_size as f64 / self.total_files as f64
        } else {
            0.0
        };
        let avg_name_length = if total_items > 0 {
            self.total_name_length as f64 / total_items as f64
        } else {
            0.0
        };
        let avg_dir_depth = if self.total_dirs > 0 {
            self.total_dir_depth as f64 / self.total_dirs as f64
        } else {
            0.0
        };

        // 格式化字节大小
        fn format_bytes(bytes: f64) -> String {
            const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
            let mut size = bytes;
            let mut unit_index = 0;

            while size >= 1024.0 && unit_index < UNITS.len() - 1 {
                size /= 1024.0;
                unit_index += 1;
            }

            if unit_index == 0 {
                format!("{:.0} {}", size, UNITS[unit_index])
            } else {
                format!("{:.2} {}", size, UNITS[unit_index])
            }
        }

        writeln!(
            f,
            "=================================================================="
        )?;
        writeln!(
            f,
            "                           Scan Statistics                          "
        )?;
        writeln!(
            f,
            " =================================================================="
        )?;
        writeln!(f)?;
        writeln!(f, "   Command    :    {}", self.command)?;
        writeln!(f, "   Total time :    {}", self.total_time)?;
        writeln!(f, "   Job ID     :    {}", self.job_id)?;
        writeln!(f, "   Log Path   :    {}", self.log_path)?;
        writeln!(f)?;
        writeln!(
            f,
            " ------------------------- Sanned Count -------------------------"
        )?;
        writeln!(
            f,
            "   Total:                                       {}",
            total_items
        )?;
        writeln!(
            f,
            "   Files:                                       {}",
            self.total_files
        )?;
        writeln!(
            f,
            "   Directories:                                 {}",
            self.total_dirs
        )?;
        writeln!(
            f,
            " --------------------------- Capacity ---------------------------"
        )?;
        writeln!(
            f,
            "   Total:                                 {}",
            format_bytes(self.total_size as f64)
        )?;
        writeln!(
            f,
            "   Average:                               {}",
            format_bytes(avg_file_size)
        )?;
        writeln!(
            f,
            " ------------------------ Filename Length ------------------------"
        )?;
        writeln!(
            f,
            "   Avg:                                          {:.0}",
            avg_name_length
        )?;
        writeln!(
            f,
            "   Max:                                          {}",
            self.max_name_length
        )?;
        writeln!(
            f,
            " ------------------------ Directory Depth ------------------------"
        )?;
        writeln!(
            f,
            "   Avg:                                           {:.0}",
            avg_dir_depth
        )?;
        writeln!(
            f,
            "   Max:                                           {}",
            self.max_dir_depth
        )?;
        writeln!(
            f,
            " -------------------------------------------------------------"
        )?;
        writeln!(
            f,
            " ================================================================="
        )
    }
}

/// 统计计算器 - 处理所有统计相关的计算逻辑
pub struct StatsCalculator {
    base_path: String,
}

impl StatsCalculator {
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: base_path.to_string(),
        }
    }

    /// 更新文件统计信息
    pub fn update_file_stats(
        &self, stats: &mut ScanStats, file_name: &str, file_size: u64, is_symlink: bool,
    ) {
        stats.total_size += file_size as i64;
        stats.total_name_length += file_name.len() as i64;
        stats.max_name_length = stats.max_name_length.max(file_name.len());

        if is_symlink {
            stats.total_symlink += 1;
        } else {
            stats.total_regular_file += 1;
        }
    }

    /// 更新目录统计信息
    pub fn update_dir_stats(&self, stats: &mut ScanStats, dir_name: &str, depth: usize) {
        stats.total_name_length += dir_name.len() as i64;
        stats.max_name_length = stats.max_name_length.max(dir_name.len());
        stats.total_dir_depth += depth as i64;
        stats.max_dir_depth = stats.max_dir_depth.max(depth);
    }

    /// 计算目录深度
    pub fn calculate_depth(&self, path: &Path) -> usize {
        path.strip_prefix(&self.base_path)
            .unwrap_or(path)
            .components()
            .count()
            .max(1)
    }

    /// 获取文件名长度
    pub fn get_name_length(&self, path: &Path) -> usize {
        path.file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.len())
            .unwrap_or(0)
    }
}
