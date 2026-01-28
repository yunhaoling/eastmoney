# 东方财富网年报业绩数据下载器

下载东方财富网年报业绩数据到CSV文件的Python工具。

**数据来源**: https://data.eastmoney.com/bbsj/202412/yjbb.html

## 功能特点

1. ✅ **下载数据到CSV** - 自动将数据保存为CSV格式，支持中文列名
2. ✅ **自动分页** - 自动遍历所有页面获取完整数据
3. ✅ **增量下载** - 自动跳过已下载的记录，避免重复数据

## 文件说明

| 文件 | 说明 |
|------|------|
| `eastmoney_annual_report_downloader.py` | 完整版 - 支持命令行参数 |
| `eastmoney_simple_downloader.py` | 简易版 - 交互式操作 |

## 安装依赖

```bash
pip install requests
```

## 使用方法

### 方法一：简易版（交互式）

```bash
python eastmoney_simple_downloader.py
```

按提示输入年份即可下载。

### 方法二：完整版（命令行）

```bash
# 下载2024年年报数据
python eastmoney_annual_report_downloader.py --year 2024

# 下载2020-2024年所有年报数据
python eastmoney_annual_report_downloader.py --start 2020 --end 2024

# 全量下载（不跳过已有数据）
python eastmoney_annual_report_downloader.py --year 2024 --full

# 指定输出目录
python eastmoney_annual_report_downloader.py --year 2024 --output ./output
```

### 方法三：在Python代码中调用

```python
from eastmoney_simple_downloader import download_annual_report, download_multiple_years

# 下载单一年份
download_annual_report(2024)

# 下载多年份
download_multiple_years(2020, 2024, output_dir="./data")
```

## 命令行参数

| 参数 | 简写 | 说明 |
|------|------|------|
| `--year` | `-y` | 下载指定年份的年报数据 |
| `--start` | `-s` | 起始年份（与--end一起使用） |
| `--end` | `-e` | 结束年份（与--start一起使用） |
| `--output` | `-o` | 输出目录 (默认: ./data) |
| `--full` | `-f` | 全量下载（不跳过已有数据） |
| `--delay` | `-d` | 请求间隔时间（秒）(默认: 0.5) |

## 输出数据字段

| 字段名 | 说明 |
|--------|------|
| 股票代码 | 股票代码 |
| 股票简称 | 股票名称缩写 |
| 交易市场 | 上交所/深交所等 |
| 更新日期 | 数据更新日期 |
| 报告日期 | 年报报告期 |
| 每股收益(元) | 基本每股收益 |
| 扣非每股收益(元) | 扣除非经常性损益后的每股收益 |
| 营业总收入(元) | 营业总收入 |
| 净利润(元) | 归属于母公司的净利润 |
| 净资产收益率(%) | 加权平均净资产收益率 |
| 营收同比增长(%) | 营业收入同比增长率 |
| 净利润同比增长(%) | 净利润同比增长率 |
| 每股净资产(元) | 每股净资产 |
| 每股经营现金流(元) | 每股经营现金流量净额 |
| 销售毛利率(%) | 销售毛利率 |
| 分配方案 | 股利分配方案 |
| 公告日期 | 首次公告日期 |

## 增量下载原理

程序会检查CSV文件中已有的股票代码，下载时自动跳过这些记录，只下载新增的数据。这样可以：

- 避免重复数据
- 节省下载时间
- 支持断点续传

## 注意事项

1. **网络要求**: 需要能够访问东方财富网
2. **请求频率**: 程序自动控制请求频率，避免被封禁
3. **数据更新**: 年报数据通常在每年4月前后陆续发布
4. **编码格式**: CSV文件使用UTF-8-BOM编码，可直接用Excel打开

## 常见问题

**Q: 为什么2026年没有数据？**  
A: 因为2025年年报要到2026年才会发布，现在是2026年初，数据还未发布。

**Q: 如何更新已有数据？**  
A: 直接运行程序即可，程序会自动检测并下载新增的记录。

**Q: 如何强制重新下载所有数据？**  
A: 使用 `--full` 参数，或删除已有的CSV文件。

## 免责声明

本工具仅供学习交流使用，请勿用于商业用途。数据版权归东方财富网所有。
