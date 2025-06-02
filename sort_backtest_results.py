#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测结果排序工具
用于读取回测结果文件，按照收益率进行排序，并输出到新文件
"""

import re
import argparse
from typing import List, Dict, Any, Tuple


def parse_result_line(line: str) -> Dict[str, Any]:
    """
    解析回测结果行，提取关键信息
    
    Args:
        line: 回测结果行文本
        
    Returns:
        包含解析结果的字典，如果解析失败则返回None
    """
    # 匹配回测结果行的正则表达式
    pattern = r'板块: (.*?), 止盈率: ([\d.]+), 止损率: ([\d.]+), 均线: (.*?), 盈利: ([-\d.]+), 收益率: ([-\d.]+)%'
    match = re.match(pattern, line)
    
    if match:
        region, zy_rate, zs_rate, ma_line, profit, profit_rate = match.groups()
        return {
            'region': region,
            'zy_rate': float(zy_rate),
            'zs_rate': float(zs_rate),
            'ma_line': ma_line,
            'profit': float(profit),
            'profit_rate': float(profit_rate),
            'original_line': line  # 保存原始行，以便输出
        }
    return None


def read_backtest_results(file_path: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    读取回测结果文件
    
    Args:
        file_path: 回测结果文件路径
        
    Returns:
        解析后的结果列表和非结果行列表的元组
    """
    results = []
    other_lines = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                result = parse_result_line(line)
                if result:
                    results.append(result)
                else:
                    other_lines.append(line)
                    
        return results, other_lines
    except Exception as e:
        print(f"读取文件 {file_path} 时出错: {e}")
        return [], []


def sort_results(results: List[Dict[str, Any]], reverse: bool = True) -> List[Dict[str, Any]]:
    """
    根据收益率对结果进行排序
    
    Args:
        results: 解析后的结果列表
        reverse: 是否降序排序，默认为True（从高到低）
        
    Returns:
        排序后的结果列表
    """
    return sorted(results, key=lambda x: x['profit_rate'], reverse=reverse)


def write_sorted_results(results: List[Dict[str, Any]], other_lines: List[str], output_file: str):
    """
    将排序后的结果写入输出文件
    
    Args:
        results: 排序后的结果列表
        other_lines: 非结果行列表
        output_file: 输出文件路径
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # 写入文件头
            f.write("# 回测结果（按收益率排序）\n")
            f.write("===========================================\n\n")
            
            # 写入排序后的结果
            f.write("## 排序后的回测结果\n")
            for i, result in enumerate(results, 1):
                f.write(f"{i}. {result['original_line']}\n")
            
            # 写入统计信息
            f.write("\n## 统计信息\n")
            f.write(f"总回测组合数: {len(results)}\n")
            
            if results:
                # 最佳组合
                best = results[0]
                f.write(f"最佳组合: 板块={best['region']}, 止盈率={best['zy_rate']}, "
                        f"止损率={best['zs_rate']}, 均线={best['ma_line']}, "
                        f"收益率={best['profit_rate']}%\n")
                
                # 最差组合
                worst = results[-1]
                f.write(f"最差组合: 板块={worst['region']}, 止盈率={worst['zy_rate']}, "
                        f"止损率={worst['zs_rate']}, 均线={worst['ma_line']}, "
                        f"收益率={worst['profit_rate']}%\n")
                
                # 平均收益率
                avg_profit_rate = sum(r['profit_rate'] for r in results) / len(results)
                f.write(f"平均收益率: {avg_profit_rate:.2f}%\n")
            
            # 写入其他行
            if other_lines:
                f.write("\n## 其他信息\n")
                for line in other_lines:
                    f.write(f"{line}\n")
                    
            print(f"排序结果已写入文件: {output_file}")
    except Exception as e:
        print(f"写入文件 {output_file} 时出错: {e}")


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='对回测结果按收益率进行排序')
    parser.add_argument('-i', '--input', default='backtest_results1.txt', help='输入文件路径，默认为backtest_results1.txt')
    parser.add_argument('-o', '--output', default='sorted_backtest_results.txt', help='输出文件路径，默认为sorted_backtest_results.txt')
    parser.add_argument('-a', '--ascending', action='store_true', help='使用升序排序（从低到高），默认为降序（从高到低）')
    args = parser.parse_args()
    
    # 读取回测结果
    print(f"正在读取回测结果文件: {args.input}")
    results, other_lines = read_backtest_results(args.input)
    
    if not results:
        print("未找到有效的回测结果，请检查输入文件格式")
        return
    
    # 排序结果
    print(f"正在对 {len(results)} 条回测结果进行{'升序' if args.ascending else '降序'}排序")
    sorted_results = sort_results(results, not args.ascending)
    
    # 写入排序后的结果
    write_sorted_results(sorted_results, other_lines, args.output)


if __name__ == '__main__':
    main() 