/**
  * Created by Lizhen Shi on 5/16/17.
  */
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, lib => graphxlib}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import sext._
import tools.{DNASeq, Utils}

import scala.reflect.ClassTag


object GraphLPA2 extends App with LazyLogging {


  case class Config(edge_file: String = "",output: String = "",min_shared_kmers: Int = 2, max_iteration: Int = 10,
                    n_output_blocks: Int = 180,
                    min_reads_per_cluster: Int = 2, max_shared_kmers: Int = 20000, sleep: Int = 0,
                    n_partition: Int = 0)

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("GraphLPA") {
      head("GraphLPA", Utils.VERSION)

      opt[String]('i', "edge_file").required().valueName("<file>").action((x, c) =>
        c.copy(edge_file = x)).text("files of graph edges. e.g. output from GraphGen")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")


      opt[Int]("max_iteration").action((x, c) =>
        c.copy(max_iteration = x))
        .text("max ietration for LPA")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")


      opt[Int]("n_output_blocks").action((x, c) =>
        c.copy(n_output_blocks = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n_output_blocks should be greater than 0"))
        .text("output block number")

      opt[Int]("min_shared_kmers").action((x, c) =>
        c.copy(min_shared_kmers = x)).
        validate(x =>
          if (x >= 1) success
          else failure("min_shared_kmers should be greater than 2"))
        .text("minimum number of kmers that two reads share")

      opt[Int]("max_shared_kmers").action((x, c) =>
        c.copy(max_shared_kmers = x)).
        validate(x =>
          if (x >= 1) success
          else failure("max_shared_kmers should be greater than 1"))
        .text("max number of kmers that two reads share")


      opt[Int]("min_reads_per_cluster").action((x, c) =>
        c.copy(min_reads_per_cluster = x))
        .text("minimum reads per cluster")

      help("help").text("prints this usage text")
    }
    parser.parse(args, Config())
  }

  //5..........
  def cc(edgeTuples: RDD[(Int, Int)], config: Config, sqlContext: SQLContext) = {
    logInfo(s"1")
    cc_graphx(edgeTuples, sqlContext, config.max_iteration)
  }
  //8..............
//构建图的入口方法有两种，分别是根据边构建和根据边的两个顶点构建
  //根据边构建图(Graph.fromEdges)
  //根据边的两个顶点数据构建(Graph.fromEdgeTuples)
  //本次使用根据边的两个顶点数据构建图--三步走--核心步骤
  //VD和ED代表了用户自定义的顶点和边类，对应的图是参数化类型的泛类型Graph[VD,ED]。
  def fromEdgeTuples[VD: ClassTag](
                                    rawEdges: RDD[(Int, Int)],//存储的是原始数据，即(srcId, dstId)--(起始顶点，终点)
                                    defaultValue: VD,
                                    uniqueEdges: Option[PartitionStrategy] = None,
                                    edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                    vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, Int] = {
    //1.转换源数据格式：从文件中加载信息，转换成 tuple 的形式,即(srcId, dstId)
    //2.将数据转换为Edge对象：把源数据映射成 Edge(srcId, dstId, attr)对象,attr 默认为 1。这样元数据就构建成了RDD[Edge[ED]]
    val edges = rawEdges.map(p => Edge(p._1, p._2, 1))

    logInfo(s"3")
    //3.构建图：使用GraphImpl构建图---GraphImpl的过程
    //构建图的过程很简单，分为三步，它们分别是构建边EdgeRDD、构建顶点VertexRDD、生成Graph对象。
    val graph = GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)

    uniqueEdges match {
      case Some(p) => graph.partitionBy(p).groupEdges((a, b) => a + b)
      case None => graph
    }
  }
  //6.............
  //edgeTuples:存储构建图的顶点元组(srcid,dstid)
  def cc_graphx(edgeTuples: RDD[(Int, Int)], sqlContext: SQLContext, max_iteration: Int) = {
    //graph:存储构建好的图
    logInfo(s"2")
    //7...........
    val graph = fromEdgeTuples(
      edgeTuples, 1.toInt, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER
    )
    logInfo(s"4")
    logInfo(s"5")
    //9................
    val cc = MyLabelPropagation.run(graph, max_iteration)
    logInfo(s"6")
    val clusters = cc.vertices.map(x => (x._1.toLong, x._2.toLong))
    print(clusters)
    logInfo(s"7")
    clusters
  }

  def logInfo(str: String) = {
    logger.info(str)
    println("AAAA " + str)
  }
  //3........
  protected def run_cc(all_edges: RDD[Array[Int]], config: Config, spark: SparkSession,
                       n_reads: Long) = {
    val sqlContext = spark.sqlContext

    val edgeTuples = all_edges.map {//生成边的元组 all_edges:存储若干个数组，每个数组有两个顶点
      x =>
        if (x(0) < x(1)) (x(0), x(1)) else (x(1), x(0))  // 可能是为使用CanonicalRandomVertexCut分区做准备--顶点小的在前面。CanonicalRandomVertexCut分区：通过取源顶点和目标顶点 id 的哈希值来将边分配到不同的分区。哈希值的产生带有确定 的方向（即两个顶点中较小 id 的顶点在前）。两个顶点之间所有的边都会分配到同一个分区，而不管方向如何
    }
    logInfo(s"loaded ${edgeTuples.count} edges")//元组数量等于边的数量

    //4...........
    val clusters = this.cc(edgeTuples, config, sqlContext).map(_.swap)
    clusters.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logInfo(s"#records=${clusters.count} are persisted")
    logInfo(s"8")
    val final_clusters = {
      clusters.groupByKey.filter(_._2.size >= config.min_reads_per_cluster).map(u => (u._1, u._2.toSeq))
    }
    println("final_clustes1-------")
    final_clusters.take(10).foreach(println(_))
    logInfo(s"9")
    final_clusters.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logInfo(s"Got ${final_clusters.count} clusters")
    clusters.unpersist(blocking = false)
    final_clusters
  }

  def run(config: Config, spark: SparkSession): Long = {

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sc.setCheckpointDir("hdfs://hadoop50:9000/wxy/sparkCheckpoint")
    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")
    //1..........
    val edges =
      (if (config.n_partition > 0)
        sc.textFile(config.edge_file).repartition(config.n_partition)
      else
        sc.textFile(config.edge_file)).
        map { line =>
          line.split(",").map(_.toInt)
        }.filter(x => x(2) >= config.min_shared_kmers && x(2) <= config.max_shared_kmers).map(_.take(2))//保留两个顶点之间的共享kmer小于最大大于最小的边，且只取前两个
    edges.cache()
    logInfo("loaded %d edges".format(edges.count))

    edges.take(5).map(_.mkString(",")).foreach(println)
    val n_reads = edges.flatMap(x => x).distinct().count()//n_reads:边数量
    logInfo(s"total #reads = $n_reads")

    //2.............
    val final_clusters = run_cc(edges, config, spark, n_reads)
    KmerCounting.delete_hdfs_file(config.output)

    val result = final_clusters.map(_._2.toList.sorted).map(_.mkString(","))
    println("result-------")
    result.take(10).foreach(println(_))
    result.repartition(config.n_output_blocks).saveAsTextFile(config.output)
    val result_count = sc.textFile(config.output).count
    logInfo(s"total #records=${result_count} save results to ${config.output}")

    val totalTime1 = System.currentTimeMillis
    logInfo("Processing time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))

    // may be have the bug as https://issues.apache.org/jira/browse/SPARK-15002
    edges.unpersist(blocking = false)
    final_clusters.unpersist(blocking = false)
    result_count
  }

  override def main(args: Array[String]) {
    val APPNAME = "GraphLPA2"

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logInfo(s"called with arguments\n${options.valueTreeString}")
        require(config.min_shared_kmers <= config.max_shared_kmers)

        val conf = new SparkConf().setAppName(APPNAME)
        conf.registerKryoClasses(Array(classOf[DNASeq]))

        val spark = SparkSession
          .builder().config(conf)
          .appName(APPNAME)
          .getOrCreate()

        run(config, spark)
        if (config.sleep > 0) Thread.sleep(config.sleep * 1000)
        spark.stop()
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main
}
