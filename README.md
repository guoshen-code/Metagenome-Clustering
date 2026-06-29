<p align="center">
  <h1 align="center">🧬 Spark-MLSC</h1>
  <p align="center">
    <strong>基于 Spark 的多因子标签扩散宏基因组序列聚类算法</strong>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/Scala-2.12-red.svg" alt="Scala"/>
    <img src="https://img.shields.io/badge/Spark-3.5.7-orange.svg" alt="Spark"/>
    <img src="https://img.shields.io/badge/Hadoop-3.3.6-blue.svg" alt="Hadoop"/>
    <img src="https://img.shields.io/badge/Java-1.8-green.svg" alt="Java"/>
  </p>
</p>

<p align="center">
  <a href="#-项目简介">简介</a> •
  <a href="#-运行命令">运行</a> •
  <a href="#-项目结构">结构</a> •
  <a href="#-构建">构建</a>
</p>

---

## 📖 项目简介

Spark-MLSC 是一个基于 Apache Spark 的大规模宏基因组序列聚类工具，采用**多因子标签扩散算法**，通过节点重要性评估、核心选择、重要性梯度传播和社区合并等步骤，实现高效的序列聚类分析。


## 🖥️ 环境要求

| | 组件 | 版本 |
|:---:|:---:|:---:|
| 🔶 | **Spark** | 3.5.7 |
| 🐘 | **Hadoop** | 3.3.6 |
| ☕ | **Java** | 1.8 |
| 📐 | **Scala** | 2.12 |

## 🔄 流水线概览

整个聚类流程分为 **5 个阶段**，依次执行：

```
┌──────────┐    ┌──────────────┐    ┌──────────┐    ┌───────────┐    ┌──────────┐
│ SeqAddId │───▶│KmerMapReads  │───▶│ GraphGen │───▶│ SparkMLSC │───▶│ CCAddSeq │
│ 序列编号  │    │  K-mer 映射   │    │  图构建   │    │  核心聚类  │    │ 结果还原  │
└──────────┘    └──────────────┘    └──────────┘    └───────────┘    └──────────┘
     ①                ②                 ③               ④                ⑤
```

| 阶段 | 模块 | 类名 | 说明 |
|:---:|:---:|:---|:---|
| ① | **SeqAddId** | `o.j.s.l.t.SeqAddId` | 数据预处理，为序列添加唯一 ID |
| ② | **KmerMapReads** | `o.j.s.l.t.KmerMapReads` | K-mer 映射，建立 k-mer 与 reads 的对应关系 |
| ③ | **GraphGen** | `o.j.s.l.t.GraphGen` | 图构建，基于共享 k-mer 生成序列相似性图 |
| ④ | **SparkMLSC** | `o.j.s.l.t.SparkMLSC` | 核心聚类，多因子标签扩散算法执行聚类 |
| ⑤ | **CCAddSeq** | `o.j.s.l.t.CCAddSeq` | 结果还原，将聚类结果映射回原始序列 |

> 💡 **快速参考**：所有模块均通过 `spark-submit` 提交，JAR 包统一为 `lizhenshi-sparc_5778-1.0-SNAPSHOT-jar-with-dependencies.jar`。



## 🚀 运行命令

### 1. 数据预处理 — SeqAddId

为输入序列文件添加唯一数字标识。

<details>
<summary>📋 参数说明</summary>

| 参数 | 说明 |
|:---|:---|
| `--in` | 输入序列文件路径 |
| `--output` | 输出文件路径（HDFS） |

</details>

```bash
spark-submit \
  --class org.jgi.spark.localcluster.tools.SeqAddId \
  --master spark://hadoop50:7077 \
  --deploy-mode client \
  --driver-memory 30G --driver-cores 5 \
  --executor-cores 4 --executor-memory 6G \
  --conf spark.driver.maxResultSize=5g \
  --conf spark.network.timeout=360000 \
  --conf spark.speculation=true \
  file:///home/guoshen/data/lizhenshi-sparc_5778-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --in=hdfs://hadoop50:8020///user/guoshen/paperData/Mouse16_ShortPart_40.seq \
  --output=hdfs://hadoop50:8020//user/guoshen/paperData/Mouse16_ShortPart_Id_40.seq
```

### 2. K-mer 映射 — KmerMapReads

将 reads 与 k-mer 进行映射，构建 k-mer 共享关系。

<details>
<summary>📋 参数说明</summary>

| 参数 | 说明 |
|:---|:---|
| `--reads` | 带 ID 的序列文件路径 |
| `--output` | 输出 k-mer 映射结果路径 |
| `--format` | 输入格式（`seq` / `parquet` / `base64`） |
| `--kmer_length` | k-mer 长度（>= 11） |
| `--contamination` | 污染过滤比例（0~1） |
| `--min_kmer_count` | k-mer 最小共享 reads 数 |
| `--max_kmer_count` | k-mer 最大共享 reads 数 |
| `-C` | 启用 canonical k-mer |
| `--n_iteration` | 迭代次数 |
| `--n_partition` | 输入分区数 |

</details>

```bash
spark-submit \
  --class org.jgi.spark.localcluster.tools.KmerMapReads \
  --master spark://hadoop50:7077 \
  --deploy-mode client \
  --driver-memory 45G --driver-cores 5 \
  --executor-cores 7 --executor-memory 52G \
  --conf spark.driver.maxResultSize=10g \
  --conf spark.network.timeout=3600 \
  --conf spark.speculation=true \
  hdfs://hadoop50:8020/user/guoshen/paperData/lizhenshi-sparc_5778-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --reads=hdfs://hadoop50:8020//user/guoshen/paperData/Mouse16_ShortPart_Id_40.seq \
  --output=hdfs://hadoop50:8020//user/guoshen/paperData/kmerMap_Mouse16Part_Short_40 \
  --wait=1 --format=seq --kmer_length=13 \
  --contamination=0 --min_kmer_count=2 --max_kmer_count=100000 \
  -C --n_iteration=1 --n_partition=50
```

### 3. 图构建 — GraphGen

基于 k-mer 共享信息构建序列相似性图。

<details>
<summary>📋 参数说明</summary>

| 参数 | 说明 |
|:---|:---|
| `--kmer_reads` | KmerMapReads 的输出路径 |
| `--output` | 边列表输出路径（含权重） |
| `--output_two_edges` | 双向边输出路径 |
| `--min_shared_kmers` | 两 reads 间最小共享 k-mer 数 |
| `--max_degree` | 节点最大度数限制 |
| `--n_partition` | 输入分区数 |

</details>

```bash
spark-submit \
  --master spark://hadoop50:7077 \
  --deploy-mode client \
  --driver-memory 30G --driver-cores 5 \
  --executor-cores 4 --executor-memory 15G \
  --conf spark.network.timeout=3600 \
  --conf spark.speculation=true \
  hdfs://hadoop50:8020/user/guoshen/paperData/lizhenshi-sparc_5778-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --kmer_reads=hdfs://hadoop50:8020//user/guoshen/paperData/kmerMap_Mouse16Part_Short_40 \
  --output=hdfs://hadoop50:8020///user/guoshen/paperData/GraphGen_Mouse16Part_Short_edges_40 \
  --output_two_edges=hdfs://hadoop50:8020//user/guoshen/paperData/GraphGen_Mouse16Part_Short_vertexs_40 \
  --wait=1 --min_shared_kmers=6 --max_degree=50 --n_partition=50
```

### 4. 核心聚类 — SparkMLSC

执行多因子标签扩散聚类算法。

<details>
<summary>📋 参数说明</summary>

| 参数 | 说明 |
|:---|:---|
| `--edge_file` | GraphGen 输出的顶点文件路径 |
| `--output` | 聚类结果输出路径 |
| `--min_reads_per_cluster` | 每个聚类最小 reads 数 |
| `--n_partition` | 输入分区数 |
| `--enable_merge` | 是否启用社区合并步骤（`true` / `false`） |

</details>

```bash
spark-submit \
  --class org.jgi.spark.localcluster.tools.SparkMLSC \
  --master spark://hadoop50:7077 \
  --deploy-mode client \
  --driver-memory 30G --driver-cores 5 \
  --executor-cores 4 --executor-memory 15G \
  --conf spark.driver.maxResultSize=6g \
  --conf spark.network.timeout=3600 \
  --conf spark.default.parallelism=50 \
  /home/guoshen/data/lizhenshi-sparc_5778-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --edge_file=hdfs://hadoop50:8020//user/guoshen/paperData/GraphGen_Mouse16Part_Short_vertexs_40 \
  --output=hdfs://hadoop50:8020//user/guoshen/paperData/test08_Mouse16part_Short_40 \
  --min_reads_per_cluster=1 --n_partition=50 --enable_merge=true
```

### 5. 结果还原 — CCAddSeq

将聚类结果映射回原始序列数据。

<details>
<summary>📋 参数说明</summary>

| 参数 | 说明 |
|:---|:---|
| `--cc_file` | SparkMLSC 聚类结果路径 |
| `--reads` | 带 ID 的序列文件路径 |
| `--output` | 最终结果输出路径 |
| `--format` | 输入格式（`seq` / `parquet` / `base64`） |
| `--n_partition` | 输入分区数 |
| `--num_output` | 输出分区数 |

</details>

```bash
spark-submit \
  --class org.jgi.spark.localcluster.tools.CCAddSeq \
  --master spark://hadoop50:7077 \
  --driver-memory 55G --driver-cores 3 \
  --executor-cores 4 --executor-memory 15G \
  file:///home/guoshen/data/lizhenshi-sparc_5778-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --wait=1 \
  --cc_file=hdfs://hadoop50:8020//user/guoshen/paperData/test08_Mouse16part_Short_40 \
  --reads=hdfs://hadoop50:8020/user/guoshen/paperData/Mouse16_ShortPart_Id_40.seq \
  --output=hdfs://hadoop50:8020/user/guoshen/paperData/test08add_Mouse16Part_Short_40 \
  --n_partition=20
```

---

## 📁 项目结构

```
MLSC/
├── src/main/scala/
│   ├── tools/
│   │   ├── DBManger.scala      # 数据库管理
│   │   ├── DNASeq.scala        # DNA 序列数据结构
│   │   ├── Kmer.scala          # K-mer 生成（标准）
│   │   ├── Kmer2.scala         # K-mer 生成（备选）
│   │   ├── cKmer.scala         # K-mer 生成（C++ 原生加速）
│   │   └── Utils.scala         # 通用工具函数
│   ├── SeqAddId.scala          # ① 序列 ID 添加
│   ├── KmerMapReads.scala      # ② K-mer 与 Reads 映射
│   ├── KmerMapReads2.scala     #    K-mer 映射（备选版本）
│   ├── KmerCounting.scala      #    K-mer 计数统计
│   ├── GraphGen.scala          # ③ 相似性图构建
│   ├── GraphLPA2.scala         #    图标签传播（辅助）
│   ├── MyLabelPropagation.scala#    自定义标签传播算法
│   ├── SparkMLSC.scala         # ④ 核心聚类算法
│   └── CCAddSeq.scala          # ⑤ 聚类结果还原
├── lib/                         # 外部依赖库
│   ├── graphframes-0.8.1-*.jar #   GraphFrames 图计算框架
│   └── sparc-cpp-lib-0.1.jar   #   SPARC C++ 原生加速库
├── script/
│   └── shortFastqToseq.py      # FASTQ 转 seq 格式脚本
└── pom.xml                      # Maven 构建配置
```

## 📦 关键依赖

| | 依赖 | 版本 | 用途 |
|:---:|:---|:---:|:---|
| 🔶 | Apache Spark (GraphX) | 3.2.1 | 分布式图计算引擎 |
| 🐘 | Apache Hadoop | 3.3.2 | 分布式存储（HDFS） |
| 🔧 | scopt | 4.0.1 | 命令行参数解析 |
| 💾 | RocksDB JNI | 7.1.2 | 本地 KV 存储加速 |
| 🛠️ | Guava | 31.1-jre | 通用工具库 |
| 📊 | GraphFrames | 0.8.1 | Spark 图数据框扩展 |

## 🔨 构建

```bash
mvn clean package
```

生成的 JAR 包位于 `target/` 目录下。

> 💡 **提示**：打包前请确认 `lib/` 下的系统依赖 JAR 已就位，Maven 会通过 `system` scope 引入。

---

<p align="center">
  <a href="#">🔝 回到顶部</a>
</p>

<p align="center">
  <sub>Made with ❤ for metagenomics research</sub>
</p>
