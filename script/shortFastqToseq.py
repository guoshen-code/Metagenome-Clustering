# 为 Spark-shared-kmer 准备数据
# 输入是来自 Illumina 的 fastq.gz 文件
# 如果 Read pairs 尚未合并，将在此合并
# 输出 .seq 文件

import os
import gzip
import sys
import getopt
import random

def extract_data(in_file, out_file, paired, shuffle):
    """
    从 gzip 文件列表中提取特定数量的碱基
    输出格式: ID    Seq
    """
    # 显式指定 utf-8 编码，Python 3 处理文本更稳健
    out = open(out_file, 'w', encoding='utf-8')
    seq_list = []

    # 判定是否为压缩文件
    is_gz = in_file.endswith(".gz")

    # Python 3 推荐使用 'rt' (read text) 模式读取 gzip
    if is_gz:
        seq = gzip.open(in_file, 'rt', encoding='utf-8')
    else:
        seq = open(in_file, 'r', encoding='utf-8')

    try:
        if paired:
            """
            用 'N' 将两端 read 连接起来，使用第一端的 ID
            逻辑：8行一个循环
            """
            lineno = 0
            seqID = ""
            seqSeq = ""
            for line in seq:
                lineno += 1
                rem = lineno % 8
                if rem == 1:
                    seqID = line[1:-1] # 去掉开头的 @ 和结尾换行
                elif rem == 2:
                    seqSeq = line[:-1] # 获取第一端序列
                elif rem == 6:
                    # 获取第二端序列并拼接，统一转大写
                    full_seq = seqSeq.upper() + 'N' + line[:-1].upper()
                    seq_list.append(seqID + "\t" + full_seq + "\n")

                # 每 100 万行写入一次，避免内存占用过高
                if len(seq_list) >= 1000000:
                    if shuffle:
                        random.shuffle(seq_list)
                    out.writelines(seq_list)
                    seq_list = []
        else:
            """
            普通模式：4行一个循环
            """
            lineno = 0
            seqID = ""
            for line in seq:
                lineno += 1
                rem = lineno % 4
                if rem == 1:
                    seqID = line[1:-1]
                elif rem == 2:
                    seq_list.append(seqID + "\t" + line[:-1].upper() + "\n")

                if len(seq_list) >= 1000000:
                    if shuffle:
                        random.shuffle(seq_list)
                    out.writelines(seq_list)
                    seq_list = []
    finally:
        seq.close()

    # 处理最后剩余的部分
    if seq_list:
        if shuffle:
            random.shuffle(seq_list)
        out.writelines(seq_list)

    out.close()

def main(argv):
    in_file = ''
    paired = False
    shuffle = False
    out_file = ''
    # 更新帮助信息
    help_msg = sys.argv[0] + ' -i <fastq_file> -p <1 is paired, 0 otherwise> -o <outfile> -s <1 to shuffle, 0 otherwise>'

    if len(argv) < 2:
        print(help_msg)
        sys.exit(2)

    try:
        # 修正了 getopt 的定义，确保与 Python 3 兼容
        opts, args = getopt.getopt(argv, "hi:p:o:s:", ["in_file=", "paired=", "out_file=", "shuffle="])
    except getopt.GetoptError:
        print(help_msg)
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(help_msg)
            sys.exit()
        elif opt in ("-i", "--in_file"):
            in_file = arg
        elif opt in ("-p", "--paired"):
            if int(arg) == 1:
                paired = True
        elif opt in ("-s", "--shuffle"):
            if int(arg) == 1:
                shuffle = True
        elif opt in ("-o", "--out_file"):
            out_file = arg

    if not in_file or not out_file:
        print("Error: Input and Output files are required.")
        print(help_msg)
        sys.exit(2)

    extract_data(in_file, out_file, paired, shuffle)

if __name__ == "__main__":
    main(sys.argv[1:])
