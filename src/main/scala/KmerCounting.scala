/**
  * Created by Lizhen Shi on 5/13/17.
  */
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import sext._
import tools.{DNASeq, Kmer, Kmer2, Utils}

/**---------计算kmer数量，并进行排序-----------
  * App是scala语言内置的一个特质，使用它，则把对应object内部的整体作为scala main的一部分，有延迟启动的特性。
  * 同时，命令行参数args也作为App特质的一部分，可以被开发者直接使用。而main函数则是scala的默认启动方式。
  *
  * 1.kmer长度为什么必须大于11
  * 2.为什么计算kmer时要迭代很多次
  */
object KmerCounting extends App with LazyLogging {
//样例类（case class）适合用于不可变的数据。它是一种特殊的类，能够被优化以用于模式匹配。
  //config.input/config.output---通过这种调用方式可以获取样例类中的值
  //input: String = "",----input是string类型的，默认值是“”，如果命令行输入新的值，则替换input的默认值
  // 如果命令行没有输入新的值，则使用默认值
  //***config里面包含了一下内容****
  case class Config(input: String = "",//seq文件所在的本地目录、本地文件或hdfs文件
                    output: String = "",//输出第一个k-mer
                    n_iteration: Int = 1,  //迭代次数
                    pattern: String = "",  //如果输入是本地目录，请在此处指定文件模式
                    k: Int = -1,   //规定k-mer长度，必须大于等于11
                    format: String = "seq",  //输入格式只能是"seq", "parquet", "base64"
                    sleep: Int = 0,    //在停止sparksession之前等待若干秒，默认0秒
                    canonical_kmer: Boolean = false,
                    n_partition: Int = 0   //分区数量
                   )
  //Scala里Option[T]实际上是一个容器，就像数组或是List一样，你可以把他看成是一个可能有零到一个元素的List。
  def parse_command_line(args: Array[String]): Option[Config] = {
    //OptionParser是一个用于命令行选项分析的类..OptionParser类的类型是Config类型
    //创建一个OptionParser实例命名为 parser
    //scopt是一个小的命令行选项解析库。
    val parser = new scopt.OptionParser[Config]("KmerCounting") {
      head("kmer counting", Utils.VERSION)

      //命令行选项是使用 opt[A]('f', "foo") 或 opt[A]("foo") 定义的
      // 其中A可以是任意类型, 它是 Read typeclass 的实例
      // opt是string类型的，name指定config中的字符串
      opt[String]('i', "input")  //-----seq文件所在的本地目录、本地文件或hdfs文件-----
        .required().valueName("<dir>")
        .action((x, c) => c.copy(input = x))
        //在不可变的解析样式中，config 配置对象作为参数传递给 action 回调。
        //(x, c) => c.copy(input = x)--x是命令行输入的参数，c代表config,将输入的值x传入c中的input属性中
        .text("a local dir where seq files are located in,  or a local file, or an hdfs file")

      opt[String]('p', "pattern")//如果输入是本地目录，请在此处指定文件模式
        .valueName("<pattern>")
        .action((x, c) => c.copy(pattern = x)).text("if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq")


      opt[String]('o', "output")//第一个k-mer的输出
        .required().valueName("<dir>")
        .action((x, c) => c.copy(output = x)).text("output of the top k-mers")

      opt[Unit]('C', "canonical_kmer")//应用典型的k-mer
        .action((_, c) => c.copy(canonical_kmer = true)).text("apply canonical kmer")


      opt[String]("format")//输入格式只能是"seq", "parquet", "base64"
        .valueName("<format>")
        .action((x, c) => c.copy(format = x))
        .validate(x =>
          if (List("seq", "parquet", "base64").contains(x)) success
          else failure("only valid for seq, parquet or base64")
        ).text("input format (seq, parquet or base64)")


      opt[Int]('n', "n_partition")//输入分区
        .action((x, c) => c.copy(n_partition = x))
        .text("paritions for the input")

      opt[Int]("wait")//在停止sparksession之前等待若干秒，默认0秒
        .action((x, c) => c.copy(sleep = x))
        .text("wait $sleep second before stop spark session. For debug purpose, default 0.")


      opt[Int]('k', "kmer_length")//规定k-mer长度，必须大于等于11
        .required()
        .action((x, c) => c.copy(k = x))
        .validate(x =>
          if (x >= 11) success
          else failure("k is too small, should not be smaller than 11")).text("length of k-mer")

      opt[Int]("n_iteration")//设置迭代次数必须大于等于1，如果资源不足，请设置更大的值
        .action((x, c) => c.copy(n_iteration = x))
        .validate(x =>
          if (x >= 1) success
          else failure("n should be positive"))
        .text("#iterations to finish the task. default 1. set a bigger value if resource is low.")

      help("help").text("prints this usage text")

    }
    parser.parse(args, Config())
  }

  def logInfo(str: String) = {
    println(str)
    logger.info(str)
  }
  // delete existing directory
  def delete_hdfs_file(filepath: String): Unit = {
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}

    //创建一个Configuration对象时，其构造方法会默认读取hadoop中的两个配置文件，
    // 分别是hdfs-site.xml以及core-site.xml，这两个文件中会有访问hdfs所需的参数值，
    // 主要是fs.default.name，指定了hdfs的地址，有了这个地址客户端就可以通过这个地址访问hdfs了。
    // 即可理解为configuration就是hadoop中的配置信息。
    val conf = new Configuration()

    //org.apache.hadoop.fs.FileSystem  是一个通用的文件系统API，提供了不同文件系统的统一访问方式。
    //org.apache.hadoop.fs.Path  是Hadoop文件系统中统一的文件或目录描述。
    val output = new Path(filepath) //删除文件时，需要将文件地址传入output
    val hdfs = FileSystem.get(conf)//从conf中获取hdfs的地址


    if (hdfs.exists(output)) {  //如果output文件存在与hdfs中，则删除文件
      hdfs.delete(output, true)
    }
  }
  //process_iteration函数中---canonical_kmer作用是什么
  //i:第几次循环  ReadsRDD：原文件中的基因序列  config ：命令行的输入内容  sc:操作RDD
  private def process_iteration(i: Int, readsRDD: RDD[String], config: Config, sc: SparkContext) = {
    //kmer_gen_fun：存储一个个的k-mer序列
    val kmer_gen_fun =
      if (config.canonical_kmer) {
      (seq: String) =>  Kmer.generate_kmer(seq = seq, k = config.k)//k:k-mer的长度
    } else
      (seq: String) =>  Kmer2.generate_kmer(seq = seq, k = config.k)

    //smallKmersRDD:存储所有相同的k-mer聚合后的map键值对（kmer,kmer数量）
    val smallKmersRDD = {
      process_iteration_spark(i, readsRDD, config, kmer_gen_fun)
    }

    //rdd:是一个集合存储相同k-mer的键值对
    val rdd = smallKmersRDD.filter(_._2 > 1)//保留k-mer序列数量大于1的
    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val raw_count = smallKmersRDD.count
    val kmer_count = rdd.count
    logInfo(s"filter out ${kmer_count} kmers (count>1) from total ${raw_count} kmers")
    //((kmer,2),kmer_count)
    (rdd, kmer_count)
  }
//将所有相同的k-mer聚合改成(键值对格式)
  private def process_iteration_spark(i: Int, readsRDD: RDD[String], config: Config, kmer_gen_fun: (String) => Array[DNASeq]): RDD[(DNASeq, Int)] = {
    readsRDD.map(x => kmer_gen_fun(x)).flatMap(x => x)
      .filter(o => Utils.pos_mod(o.hashCode, config.n_iteration) == i)
      .map((_, 1)).reduceByKey(_ + _)//将所有k-mer序列进行聚合
  }


  def run(config: Config, sc: SparkContext): Unit = {
    //获取program开始时间
    val start = System.currentTimeMillis
    logInfo(new java.util.Date(start) + ": Program started ...")

    //1、获取正确的文件路径格式
    val seqFiles = Utils.get_files(config.input.trim(), config.pattern.trim()) //trim()--删除首尾空格
    logger.debug(seqFiles)

    //2、获取原文件的基因序列
    //make_reads_rdd传回的是一个（1  seq ）map键值对组成的RDD集合，但是smallrdd只接收了map中的字符串
    val smallReadsRDD = KmerMapReads2.make_reads_rdd(seqFiles, config.format, config.n_partition, -1, sc).map(_._2)
    smallReadsRDD.cache()

//******pro:为什么要迭代很多次******
    //3、value存储的是[(kmer和相同kmer的数量的键值对),k-mer总数量]
    val values = 0.until(config.n_iteration)
      .map { i =>val t = process_iteration(i, smallReadsRDD, config, sc)
             t
    }

    if (true) {
      //hdfs
     //values  第一个元素：所有k-mer键值对构成的集合(kmer,kmer数量)  第二个元素:所有k-mer数量
      //rdds:所有k-mer键值对构成的集合
      val rdds = values.map(_._1)

      KmerCounting.delete_hdfs_file(config.output)
      if (false) { //take tops

      } else { //exclude top kmers and 1 kmers
        val rdd = sc.union(rdds)//将sc和rdds中的数据求合并，且不去重
        val kmer_count = values.map(_._2).sum//计算k-mer数量
        val filteredKmerRDD = rdd.sortBy(x => (-x._2, x._1.hashCode)).map(x => x._1.to_base64 + " " + x._2.toString)
       filteredKmerRDD.saveAsTextFile(config.output)
        logInfo(s"total #kmer=${kmer_count} save results to hdfs ${config.output}")
      }

      //cleanup
      smallReadsRDD.unpersist()
      rdds.foreach(_.unpersist())
      val totalTime1 = System.currentTimeMillis
      logInfo("kmer counting time: %.2f minutes".format((totalTime1 - start).toFloat / 60000))
    }
  }
//生成kmer和reads并存储 ，并计算所有k-mer数量，并排序
  override def main(args: Array[String]) {

    //获取命令行
    val options = parse_command_line(args)
    options match {
      case Some(_) => //options有命令时，就执行下面语句
        val config = options.get
        logInfo(s"called with arguments\n${options.valueTreeString}")

        val conf = new SparkConf().setAppName("Spark Kmer Counting").set("spark.kryoserializer.buffer.max", "512m")//配置集群参数
        conf.registerKryoClasses(Array(classOf[DNASeq]))//序列化

        val sc = new SparkContext(conf)//连接集群，创建RDD
        run(config, sc)  //sc可以用来操作RDD，
        if (config.sleep > 0) Thread.sleep(config.sleep * 1000)
        sc.stop()
      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  } //main
}

/** 1.命令行：
  * args数组是main函数的形参，用于接收命令行参数
  * 在Scala里Option[T]实际上是一个容器，就像数组或是List一样，你可以把他看成是一个可能有零到一个元素的List。
    当Option里面有命令，这个List的长度是1（也就是 Some），它的长度是0（也就是 None）。
    将args接收到的命令行参数传到parse_command_line函数中

    2.sparkconf---sparkcontext：
  * 任何Spark程序都是SparkContext开始的，SparkContext的初始化需要一个SparkConf对象，SparkConf包含了Spark集群配置的各种参数。
     初始化后，就可以使用SparkContext对象所包含的各种方法来创建和操作RDD和共享变量。

     appName( ) 设置将在spark Web UI中显示的应用程序的名称。如果未设置应用程序名称，则将使用随机生成的名称。
     master(“local”) 设置要连接的master URL，例如：
     “local”在本地运行    “local[4]”以4核在本地运行    “spark://master:7077”在spark独立集群上运行

     方式1：
     val conf = new SparkConf().setMaster("master").setAppName("appName")
     val sc = new SparkContext(conf)
     方式2：
     val sc = new SparkContext("master","appName")

     通过创建SparkConf对象来配置应用，然后基于这个SparkConf创建一个SparkContext对象。
     驱动器程序通过SparkContext对象来访问Spark。
     这个对象(sc)代表对计算集群的一个连接。一旦有了SparkContext， 就可以用它来创建RDD。


     3.registerKryoClasses：
        把对象转化为可传输的字节序列过程称为序列化，为什么序列化：数据需要序列化以后才能在服务端和客户端之间传输
  */
