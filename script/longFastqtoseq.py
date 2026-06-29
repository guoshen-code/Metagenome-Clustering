import sys

def convert_fastq_to_tsv(input_file, output_file):
    """
    将 FastQ 格式转换为 ID [TAB] Sequence 格式
    """
    try:
        count = 0
        with open(input_file, 'r') as f_in, open(output_file, 'w') as f_out:
            while True:
                # FastQ 格式通常每条记录占 4 行
                header = f_in.readline().strip()

                # 如果读不到 header，说明文件结束
                if not header:
                    break

                sequence = f_in.readline().strip()
                plus_line = f_in.readline()  # 跳过 '+' 行
                quality_line = f_in.readline()  # 跳过质量值行

                # 确保这是一个合法的 header 行（以 @ 开头）
                if header.startswith('@'):
                    # 去掉开头的 '@' 符号
                    clean_id = header[1:]

                    # 写入输出文件：ID + tab + 序列
                    f_out.write(f"{clean_id}\t{sequence}\n")
                    count += 1
                else:
                    print(f"警告: 遇到格式异常的行: {header}")

        print(f"处理完成！共转换了 {count} 条序列。")
        print(f"结果已保存至: {output_file}")

    except FileNotFoundError:
        print(f"错误: 找不到文件 {input_file}")
    except Exception as e:
        print(f"发生错误: {e}")

# --- 配置部分 ---
if __name__ == "__main__":
    # 在这里修改你的文件名
    input_filename = "data.fastq"   # 输入文件名 (你的原始数据)
    output_filename = "output.txt"  # 输出文件名 (你想要的结果)

    # 如果你想通过命令行传参，可以使用下面这两行（可选）：
    if len(sys.argv) == 3:
        input_filename = sys.argv[1]
        output_filename = sys.argv[2]

    convert_fastq_to_tsv(input_filename, output_filename)
