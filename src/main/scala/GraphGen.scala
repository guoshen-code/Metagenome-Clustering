/**
  * Created by Lizhen Shi on 5/16/17.
  */
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import tools.{DNASeq, Utils, cKmer}
import sext._

import scala.math.Fractional.Implicits.infixFractionalOps

/**
  * 为什么要设置最小共享Kmer数量
  * 为什么max_degree
  * 为什么要设置迭代次数
  */

object GraphGen extends App with LazyLogging {

  case class Config(kmer_reads: String = "", output: String = "", output_two_edges: String = "",max_degree: Int = 100,
                    n_iteration: Int = 1, min_shared_kmers: Int = 2, sleep: Int = 0,
                    use_native: Boolean = false,
                    n_partition: Int = 0)


  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("GraphGen") {
      head("GraphGen", Utils.VERSION)

      opt[String]('i', "kmer_reads").required().valueName("<file>").action((x, c) =>
        c.copy(kmer_reads = x)).text("reads that a kmer shares. e.g. output from KmerMapReads")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output of the top k-mers")


      opt[String]('o', "output_two_edges").required().valueName("<dir>").action((x, c) =>
        c.copy(output_two_edges = x)).text("output of the top k-mers")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait $slep second before stop spark session. For debug purpose, default 0.")

      opt[Int]("max_degree").action((x, c) =>
        c.copy(max_degree = x)).
        validate(x =>
          if (x >= 1) success
          else failure("max_degree should be greater than 1"))
        .text("max_degree of a node")

      opt[Int]("min_shared_kmers").action((x, c) =>
        c.copy(min_shared_kmers = x)).
        validate(x =>
          if (x >= 0) success//原本是最小大于1
          else failure("min_shared_kmers should be greater than 1"))
        .text("minimum number of kmers that two reads share")

      opt[Unit]("use_native").action((_, c) =>
        c.copy(use_native = true)).text("use native c++ lib")


      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("paritions for the input")

      opt[Int]("n_iteration").action((x, c) =>
        c.copy(n_iteration = x)).
        validate(x =>
          if (x >= 1) success
          else failure("n should be positive"))
        .text("#iterations to finish the task. default 1. set a bigger value if resource is low.")


      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }

  private def generate_edges(reads: Array[Int], max_degree: Int) = {
    if (reads.length <= max_degree) {
      val r = reads.combinations(2)
      r.map(u => (u(0), u(1))).toSeq
    }
    else {
      val n: Int = math.ceil(max_degree / 2.0).toInt
      val nodes1 = (0 to n).map(_ => reads.clone()).flatten
      val nodes2 = util.Random.shuffle(nodes1)
      val r = nodes1.zip(nodes2)//将nodes1和nodes2打包成一个元组
      r.filter(u => u._1 != u._2)
    }
  }
//generate_edges,(出，入，权重)算法中的4/5/6/7/8
private def process_iteration_spark(i: Int, kmer_reads: RDD[Array[Int]], config: Config, sc: SparkContext): RDD[(Int, Int, Int)] = {
    val generate_edges_fun =  //??????
      if (config.use_native) {
            //传入数据是(kmers,reads),根据两个序列之间的kmer重叠数，判断那两个序列可以组成边
        (reads: Array[Int]) => cKmer.generate_edges(reads, config.max_degree)
      } else {
        (reads: Array[Int]) => generate_edges(reads, config.max_degree)
      }
    //4.生成边：  (nodes1,nodes2)---组边
    val edges = kmer_reads.map(u => generate_edges_fun(u).map(x => if (x._1 <= x._2) x else x.swap).filter {
      case a =>
        Utils.pos_mod((a._1 + a._2).toInt, config.n_iteration) == i
    }).flatMap(x => x)
    //7.过滤边：(nodes1,nodes2,权重)
    val rdd = edges.map(x => (x, 1)).reduceByKey(_ + _).filter(x => x._2 >= config.min_shared_kmers)
      .map(x => (x._1._1.toInt, x._1._2.toInt, x._2.toInt))

    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    rdd

  }
  private def process_iteration(i: Int, kmer_reads: RDD[Array[Int]], config: Config, sc: SparkContext): RDD[(Int, Int, Int)] = {

    process_iteration_spark(i, kmer_reads, config, sc)
  }

  def run(config: Config, sc: SparkContext): Unit = {

    val start = System.currentTimeMillis
    logger.info(new java.util.Date(start) + ": Program started ...")


    val kmer_reads =
      (if (config.n_partition > 0)//kmer_reads:同一个seq中所有kmer组成的一个数组和某个seq的kmer数组对应的reads
         sc.textFile(config.kmer_reads, minPartitions = config.n_partition)//读取kmer-reads的hdfs存储地址
      else {
        sc.textFile(config.kmer_reads)
      }
        ).
        map { line =>
          line.split(" ").apply(1).split(",").map(_.toInt)
        }                                                                                                                 //每一行有多个kmer-reads键值对,每个键值对之间用" "隔开，对于键值对中的第二个元素，遇到,就分割，并将第二个元素转为INT

    kmer_reads.cache()
    logger.info("loaded %d kmer-reads-mapping".format(kmer_reads.count))
    kmer_reads.take(5).map(_.mkString(",")).foreach(x => logger.info(x))//输出
    //take:用于获取从0-4的元素，每获取5个就用“,”隔开
    val values = 0.until(config.n_iteration).map {//values:(INT INT INT)第一个：顶点1、第二个：顶点二、第三个：两个顶点的kmer重叠数
      i =>
        val r = process_iteration(i, kmer_reads, config, sc)
        r
    }


    if (true) {
      //hdfs
      val rdds = values
      KmerCounting.delete_hdfs_file(config.output)
      val rdd = sc.union(rdds).map(x => s"${x._1},${x._2},${x._3}")//将rdds于sc合并，存入rdd,此时的edge_file存的是边的信息

      val rdd_two_edges=sc.union(rdds).map(x => s"${x._1}	${x._2}")
      rdd_two_edges.take(10).foreach(println(_))

      rdd.saveAsTextFile(config.output)//减少分区

      rdd_two_edges.coalesce(50).saveAsTextFile(config.output_two_edges)
      logger.info(s"total #records=${rdd.count} save results to hdfs ${config.output}")//6.计算边数量：
      //cleanup
      try {
        kmer_reads.unpersist()
        rdds.foreach(_.unpersist())
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }


    val totalTime1 = System.currentTimeMillis
    logger.info("EdgeGen time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
  }

//生成稀疏图的边格式(出度，入度，权重)
  override def main(args: Array[String]) {

    val options = parse_command_line(args)

    options match {
      case Some(_) =>
        val config = options.get

        logger.info(s"called with arguments\n${options.valueTreeString}")
        //require(config.min_shared_kmers <= config.max_shared_kmers)
        val conf = new SparkConf().setAppName("Spark Graph Gen")
        conf.registerKryoClasses(Array(classOf[DNASeq])) //don't know why kryo cannot find the class--原版本没有这个

        val sc = new SparkContext(conf)
        run(config, sc)
        if (config.sleep > 0) Thread.sleep(config.sleep * 1000)
        sc.stop()
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main
}
