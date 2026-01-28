#!/usr/bin/env python3
"""
东方财富网业绩报表数据下载器
East Money Financial Report Data Downloader

支持下载:
  - 一季报 (Q1)
  - 半年报/中报 (Q2)
  - 三季报 (Q3)
  - 年报 (Q4)

特点:
  - 每页下载后立即保存，支持中断续传
  - 自动跳过已下载的记录
  - 失败自动重试
"""

import os
import csv
import json
import time
import requests
from datetime import datetime
from typing import Optional, Dict, List


class EastMoneyDownloader:
    """东方财富网业绩报表数据下载器"""
    
    # 季度配置
    QUARTERS = {
        'Q1': {'month': '03', 'day': '31', 'name': '一季报'},
        'Q2': {'month': '06', 'day': '30', 'name': '半年报'},
        'Q3': {'month': '09', 'day': '30', 'name': '三季报'},
        'Q4': {'month': '12', 'day': '31', 'name': '年报'},
    }
    
    # 也支持中文输入
    QUARTER_ALIASES = {
        '一季报': 'Q1', '1季报': 'Q1', '1': 'Q1', 'q1': 'Q1',
        '半年报': 'Q2', '中报': 'Q2', '2季报': 'Q2', '2': 'Q2', 'q2': 'Q2',
        '三季报': 'Q3', '3季报': 'Q3', '3': 'Q3', 'q3': 'Q3',
        '年报': 'Q4', '四季报': 'Q4', '4季报': 'Q4', '4': 'Q4', 'q4': 'Q4',
    }
    
    def __init__(self, output_dir: str = "."):
        self.output_dir = output_dir
        self.api_url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        self.headers = {
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Referer': 'https://data.eastmoney.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # 列名映射
        self.column_names = {
            'SECURITY_CODE': '股票代码',
            'SECURITY_NAME_ABBR': '股票简称',
            'TRADE_MARKET': '交易市场',
            'UPDATE_DATE': '更新日期',
            'REPORTDATE': '报告日期',
            'BASIC_EPS': '每股收益(元)',
            'DEDUCT_BASIC_EPS': '扣非每股收益(元)',
            'TOTAL_OPERATE_INCOME': '营业总收入(元)',
            'PARENT_NETPROFIT': '净利润(元)',
            'WEIGHTAVG_ROE': '净资产收益率(%)',
            'YSTZ': '营收同比增长(%)',
            'SJLTZ': '净利润同比增长(%)',
            'BPS': '每股净资产(元)',
            'MGJYXJJE': '每股经营现金流(元)',
            'XSMLL': '销售毛利率(%)',
            'YSHZ': '营收环比增长(%)',
            'SJLHZ': '净利润环比增长(%)',
            'ASSIGNDSCRPT': '分配方案',
            'NOTICE_DATE': '公告日期',
            'ORG_CODE': '组织代码',
            'SECUCODE': '证券代码'
        }
        
        os.makedirs(self.output_dir, exist_ok=True)
    
    def normalize_quarter(self, quarter: str) -> str:
        """
        标准化季度参数
        
        Args:
            quarter: 季度，支持多种格式如 'Q1', '1', '一季报' 等
            
        Returns:
            标准化后的季度 'Q1'/'Q2'/'Q3'/'Q4'
        """
        q = quarter.strip().upper()
        if q in self.QUARTERS:
            return q
        
        q_lower = quarter.strip().lower()
        if q_lower in self.QUARTER_ALIASES:
            return self.QUARTER_ALIASES[q_lower]
        
        raise ValueError(f"无效的季度参数: {quarter}，支持: Q1/Q2/Q3/Q4 或 一季报/半年报/三季报/年报")
    
    def get_report_date(self, year: int, quarter: str) -> str:
        """
        获取报告日期字符串
        
        Args:
            year: 年份
            quarter: 季度 (Q1/Q2/Q3/Q4)
            
        Returns:
            报告日期字符串，如 '2024-03-31'
        """
        q = self.normalize_quarter(quarter)
        config = self.QUARTERS[q]
        return f"{year}-{config['month']}-{config['day']}"
    
    def get_quarter_name(self, quarter: str) -> str:
        """获取季度中文名称"""
        q = self.normalize_quarter(quarter)
        return self.QUARTERS[q]['name']
    
    def get_output_filepath(self, year: int, quarter: str) -> str:
        """获取输出文件路径"""
        q = self.normalize_quarter(quarter)
        name = self.QUARTERS[q]['name']
        return os.path.join(self.output_dir, f"业绩报表_{year}年{name}.csv")
    
    def load_existing_codes(self, filepath: str) -> set:
        """加载已有的股票代码"""
        existing_codes = set()
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        code = row.get('股票代码') or row.get('SECURITY_CODE')
                        if code:
                            existing_codes.add(code)
                print(f"📋 已加载 {len(existing_codes)} 条现有记录")
            except Exception as e:
                print(f"⚠️ 读取现有文件出错: {e}")
        return existing_codes
    
    def fetch_page(
        self, 
        year: int, 
        quarter: str,
        page: int, 
        page_size: int = 50, 
        max_retries: int = 3
    ) -> Optional[dict]:
        """
        获取单页数据，支持失败重试
        """
        report_date = self.get_report_date(year, quarter)
        
        params = {
            'sortColumns': 'UPDATE_DATE,SECURITY_CODE',
            'sortTypes': '-1,-1',
            'pageSize': str(page_size),
            'pageNumber': str(page),
            'reportName': 'RPT_LICO_FN_CPD',
            'columns': 'ALL',
            'filter': f"(REPORTDATE='{report_date}')",
            'source': 'WEB',
            'client': 'WEB',
        }
        
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(
                    self.api_url, 
                    params=params, 
                    headers=self.headers, 
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                if data.get('success'):
                    return data.get('result')
                else:
                    print(f"❌ API错误: {data.get('message', '未知错误')}")
                    return None
                    
            except (requests.RequestException, json.JSONDecodeError) as e:
                if attempt < max_retries:
                    wait_time = attempt * 2
                    print(f"  ⚠️ 第{page}页请求失败 (尝试 {attempt}/{max_retries}): {e}")
                    print(f"     {wait_time}秒后重试...")
                    time.sleep(wait_time)
                else:
                    print(f"  ❌ 第{page}页请求失败，已重试{max_retries}次: {e}")
                    return None
        
        return None
    
    def download(
        self, 
        year: int, 
        quarter: str = 'Q4',
        delay: float = 0.5
    ) -> int:
        """
        下载指定年份和季度的业绩报表数据
        
        Args:
            year: 年份
            quarter: 季度，支持 Q1/Q2/Q3/Q4 或 一季报/半年报/三季报/年报
            delay: 请求间隔（秒）
            
        Returns:
            新增记录数
        """
        quarter = self.normalize_quarter(quarter)
        quarter_name = self.get_quarter_name(quarter)
        filepath = self.get_output_filepath(year, quarter)
        existing_codes = self.load_existing_codes(filepath)
        
        print(f"\n🚀 开始下载 {year}年{quarter_name}业绩数据...")
        print(f"📁 输出文件: {filepath}")
        
        # 获取第一页，确定总页数
        result = self.fetch_page(year, quarter, page=1)
        if not result:
            print("❌ 获取数据失败")
            return 0
        
        total_count = result.get('count', 0)
        total_pages = result.get('pages', 1)
        
        print(f"📊 共有 {total_count} 条记录，{total_pages} 页")
        
        if total_count == 0:
            print(f"⚠️ {year}年{quarter_name}暂无数据")
            return 0
        
        # 获取字段名
        first_page_data = result.get('data', [])
        if not first_page_data:
            print("❌ 第一页数据为空")
            return 0
        
        fieldnames = list(first_page_data[0].keys())
        chinese_names = [self.column_names.get(f, f) for f in fieldnames]
        
        file_exists = os.path.exists(filepath) and os.path.getsize(filepath) > 0
        
        total_new_records = 0
        
        with open(filepath, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if not file_exists:
                f.write(','.join(chinese_names) + '\n')
                f.flush()
            
            # 处理第一页
            page_new = self._process_page(first_page_data, existing_codes, writer, f)
            total_new_records += page_new
            print(f"  📥 第 1/{total_pages} 页 - 新增 {page_new} 条 (已保存)")
            
            # 处理剩余页面
            for page in range(2, total_pages + 1):
                time.sleep(delay)
                
                result = self.fetch_page(year, quarter, page)
                if not result:
                    print(f"  ⚠️ 第 {page} 页获取失败，跳过")
                    continue
                
                page_data = result.get('data', [])
                page_new = self._process_page(page_data, existing_codes, writer, f)
                total_new_records += page_new
                print(f"  📥 第 {page}/{total_pages} 页 - 新增 {page_new} 条 (已保存)")
        
        print(f"\n✅ 下载完成！共新增 {total_new_records} 条记录")
        print(f"📁 文件位置: {os.path.abspath(filepath)}")
        
        return total_new_records
    
    def _process_page(
        self, 
        page_data: list, 
        existing_codes: set, 
        writer: csv.DictWriter,
        file_handle
    ) -> int:
        """处理单页数据：过滤、写入、flush"""
        new_count = 0
        
        for record in page_data:
            code = record.get('SECURITY_CODE')
            
            if code and code not in existing_codes:
                writer.writerow(record)
                existing_codes.add(code)
                new_count += 1
        
        if new_count > 0:
            file_handle.flush()
            os.fsync(file_handle.fileno())
        
        return new_count
    
    def download_year(self, year: int, delay: float = 0.5) -> Dict[str, int]:
        """
        下载指定年份的所有季度报表
        
        Args:
            year: 年份
            delay: 请求间隔
            
        Returns:
            {季度: 新增记录数} 字典
        """
        results = {}
        
        for quarter in ['Q1', 'Q2', 'Q3', 'Q4']:
            count = self.download(year, quarter, delay)
            results[quarter] = count
            time.sleep(1)
        
        return results
    
    def download_range(
        self,
        start_year: int,
        end_year: int,
        quarters: List[str] = None,
        delay: float = 0.5
    ) -> Dict[str, int]:
        """
        下载指定年份范围和季度的报表
        
        Args:
            start_year: 起始年份
            end_year: 结束年份
            quarters: 季度列表，默认全部 ['Q1','Q2','Q3','Q4']
            delay: 请求间隔
            
        Returns:
            {年份_季度: 新增记录数} 字典
        """
        if quarters is None:
            quarters = ['Q1', 'Q2', 'Q3', 'Q4']
        
        quarters = [self.normalize_quarter(q) for q in quarters]
        results = {}
        
        for year in range(start_year, end_year + 1):
            for quarter in quarters:
                key = f"{year}_{quarter}"
                count = self.download(year, quarter, delay)
                results[key] = count
                time.sleep(1)
        
        # 打印汇总
        print("\n" + "=" * 60)
        print("📊 下载汇总")
        print("=" * 60)
        total = 0
        for key, count in sorted(results.items()):
            year, q = key.split('_')
            name = self.QUARTERS[q]['name']
            print(f"   {year}年{name}: {count} 条新记录")
            total += count
        print(f"   总计: {total} 条新记录")
        print("=" * 60)
        
        return results


# ============================================================
# 便捷函数
# ============================================================

def download_report(year: int, quarter: str = 'Q4', output_dir: str = ".") -> int:
    """
    下载指定年份和季度的业绩报表
    
    Args:
        year: 年份
        quarter: 季度 (Q1/Q2/Q3/Q4 或 一季报/半年报/三季报/年报)
        output_dir: 输出目录
    """
    downloader = EastMoneyDownloader(output_dir=output_dir)
    return downloader.download(year, quarter)


def download_all_quarters(year: int, output_dir: str = ".") -> dict:
    """下载指定年份的所有季度报表"""
    downloader = EastMoneyDownloader(output_dir=output_dir)
    return downloader.download_year(year)


# ============================================================
# 命令行接口
# ============================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='东方财富网业绩报表数据下载器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 下载2024年年报
  python eastmoney_downloader.py --year 2024 --quarter Q4
  
  # 下载2024年一季报
  python eastmoney_downloader.py --year 2024 --quarter Q1
  python eastmoney_downloader.py -y 2024 -q 一季报
  
  # 下载2024年所有季度报表
  python eastmoney_downloader.py --year 2024 --all
  
  # 下载2020-2024年的半年报
  python eastmoney_downloader.py --start 2020 --end 2024 --quarter Q2
  
  # 下载2020-2024年所有报表
  python eastmoney_downloader.py --start 2020 --end 2024

季度参数支持:
  Q1, 1, 一季报     → 一季报 (03-31)
  Q2, 2, 半年报, 中报 → 半年报 (06-30)
  Q3, 3, 三季报     → 三季报 (09-30)
  Q4, 4, 年报      → 年报 (12-31)
        """
    )
    
    parser.add_argument('--year', '-y', type=int, help='下载指定年份')
    parser.add_argument('--quarter', '-q', type=str, default='Q4', 
                        help='季度 (Q1/Q2/Q3/Q4 或 一季报/半年报/三季报/年报)')
    parser.add_argument('--all', '-a', action='store_true', help='下载全部季度')
    parser.add_argument('--start', '-s', type=int, help='起始年份')
    parser.add_argument('--end', '-e', type=int, help='结束年份')
    parser.add_argument('--output', '-o', type=str, default='.', help='输出目录')
    parser.add_argument('--delay', '-d', type=float, default=0.5, help='请求间隔(秒)')
    
    args = parser.parse_args()
    
    downloader = EastMoneyDownloader(output_dir=args.output)
    
    try:
        if args.year and args.all:
            # 下载单年所有季度
            downloader.download_year(args.year, args.delay)
        elif args.year:
            # 下载单年单季度
            downloader.download(args.year, args.quarter, args.delay)
        elif args.start and args.end:
            # 下载多年
            quarters = None if args.all else [args.quarter]
            downloader.download_range(args.start, args.end, quarters, args.delay)
        else:
            # 交互模式
            interactive_mode(downloader)
            
    except KeyboardInterrupt:
        print("\n\n👋 已中断，已下载的数据已保存")
    except ValueError as e:
        print(f"❌ 参数错误: {e}")


def interactive_mode(downloader: EastMoneyDownloader):
    """交互式模式"""
    print("=" * 60)
    print("     东方财富网 业绩报表数据下载器")
    print("     支持: 一季报 / 半年报 / 三季报 / 年报")
    print("=" * 60)
    print()
    
    current_year = datetime.now().year
    
    print("请选择下载模式:")
    print("  1. 下载单一报表")
    print("  2. 下载某年全部季度报表")
    print("  3. 下载多年报表")
    print()
    
    choice = input("请输入选项 (1/2/3): ").strip()
    
    if choice == '1':
        year = int(input(f"请输入年份 (2007-{current_year}): ").strip())
        print("\n季度选项: 1=一季报, 2=半年报, 3=三季报, 4=年报")
        quarter = input("请输入季度: ").strip()
        downloader.download(year, quarter)
        
    elif choice == '2':
        year = int(input(f"请输入年份 (2007-{current_year}): ").strip())
        downloader.download_year(year)
        
    elif choice == '3':
        start = int(input("请输入起始年份: ").strip())
        end = int(input("请输入结束年份: ").strip())
        print("\n季度选项: 1=一季报, 2=半年报, 3=三季报, 4=年报, all=全部")
        quarter = input("请输入季度 (默认all): ").strip() or 'all'
        
        if quarter.lower() == 'all':
            downloader.download_range(start, end)
        else:
            downloader.download_range(start, end, [quarter])
    else:
        print("⚠️ 无效选项")


if __name__ == '__main__':
    main()
