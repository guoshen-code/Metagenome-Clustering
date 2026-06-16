import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tools.DNASeq

import java.io.Serializable
import scala.util.control.Breaks.{break, breakable}

object SparkMLSC extends LazyLogging with Serializable {

  case class Config(
                     edge_file: String = "",
                     output: String = "",
                     min_reads_per_cluster: Int = 0,
                     sleep: Int = 0,
                     n_partition: Int = 0,
                     enable_merge: Boolean = true
                   )

  case class NodeProperty(label: VertexId, isCoreNode: Boolean = false) extends Serializable

  def parse_command_line(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("SparkMLSC_PLDLSCore_SMLSCMerge") {
      head("SparkMLSC_PLDLSCore_SMLSCMerge")

      opt[String]('i', "edge_file").required().valueName("<file>").action((x, c) =>
        c.copy(edge_file = x)).text("files of graph edges")

      opt[String]('o', "output").required().valueName("<dir>").action((x, c) =>
        c.copy(output = x)).text("output file")

      opt[Int]("wait").action((x, c) =>
        c.copy(sleep = x))
        .text("wait before stop spark session")

      opt[Int]('n', "n_partition").action((x, c) =>
        c.copy(n_partition = x))
        .text("partitions for input")

      opt[Int]("min_reads_per_cluster").action((x, c) =>
        c.copy(min_reads_per_cluster = x))
        .text("minimum reads per cluster")

      opt[Boolean]("enable_merge").action((x, c) =>
        c.copy(enable_merge = x))
        .text("whether to perform community merge step")

      help("help").text("prints this usage text")
    }
    parser.parse(args, Config())
  }

  def run(config: Config, spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    sc.setCheckpointDir("hdfs://hadoop50:8020/user/guoshen/sparkCheckpoint")

    val start = System.currentTimeMillis()
    logger.info(new java.util.Date(start) + ": Program started ...")

    val edgePartitions =
      if (config.n_partition > 0) config.n_partition
      else sc.defaultParallelism

    val graph = GraphLoader.edgeListFile(
      sc,
      config.edge_file,
      numEdgePartitions = edgePartitions
    ).cache()

    val totalExecutionTime = System.nanoTime()
    println(s"Graph loaded: ${graph.numVertices} vertices, ${graph.numEdges} edges")

    // 1. 邻居信息
    val neighborsRDD = graph.aggregateMessages[List[VertexId]](
      sendMsg = triplet => {
        triplet.sendToDst(List(triplet.srcId))
        triplet.sendToSrc(List(triplet.dstId))
      },
      mergeMsg = (x, y) => x ++ y
    ).cache()

    // 2. Spark-MLSC 节点重要性
    val kShellRDD = computeKShellFullyDistributed(graph, neighborsRDD).cache()
    val centralityRDD = computeCentralityFullyDistributed(graph, neighborsRDD, kShellRDD).cache()
    val dominanceRDD = computeDominanceFullyDistributed(graph, neighborsRDD, centralityRDD).cache()
    val importanceRDD = computeImportanceFullyDistributed(graph, neighborsRDD, dominanceRDD).cache()

    // 3. 核心选择 + 两级核心扩散
    val labelsAfterCoreDiffusion = performPLDLSCoreSelectionAndTwoLevelDiffusion(
      graph = graph,
      neighborsRDD = neighborsRDD,
      importanceRDD = importanceRDD
    ).cache()

    // 4.重要性梯度传播继续更新
    val labelsAfterPropagation = performImportanceGuidedPropagationWithInitialLabels(
      graph = graph,
      importanceRDD = importanceRDD,
      initialLabelsRDD = labelsAfterCoreDiffusion
    ).cache()

    // 5. 小社区合并
    val labelsAfterMerge =
      if (config.enable_merge) {
        println("==> Executing Spark-MLSC community merge step...")
        performCommunityMergeFullyDistributed(
          graph = graph,
          neighborsRDD = neighborsRDD,
          labelsRDD = labelsAfterPropagation,
          importanceRDD = importanceRDD
        ).cache()
      } else {
        println("==> Skipping community merge step (--enable_merge=false).")
        labelsAfterPropagation
      }

    // 6. 度为1节点延迟归并
    val finalLabelsRDD = handleDegreeOneNodesFullyDistributed(
      graph = graph,
      neighborsRDD = neighborsRDD,
      labelsRDD = labelsAfterMerge
    ).cache()

    println("*" * 100)
    println("time (total_execution_time): " + (System.nanoTime() - totalExecutionTime) / 1e9)

    // 7. 输出，兼容 CCAddSeq
    val communitiesRDD = finalLabelsRDD
      .map { case (id, label) => (label, id) }
      .groupByKey()
      .mapValues(_.toList.distinct.sorted)
      .filter { case (_, ids) => ids.size >= config.min_reads_per_cluster }
      .map { case (_, ids) => ids.mkString(",") }

    communitiesRDD.saveAsTextFile(config.output)
    sc.stop()
  }

  // =========================================================
  // Spark-MLSC: K-shell
  // =========================================================
  def computeKShellFullyDistributed(
                                     graph: Graph[Int, Int],
                                     neighborsRDD: VertexRDD[List[VertexId]]
                                   ): RDD[(VertexId, Int)] = {
    var kShellRDD: RDD[(VertexId, Int)] = neighborsRDD.mapValues(_.size)
    var changed = true
    var iteration = 0
    val maxIterations = 15

    while (changed && iteration < maxIterations) {
      val neighborKShellsRDD: RDD[(VertexId, List[Int])] = neighborsRDD.join(kShellRDD)
        .flatMap { case (_, (neighbors, myKShell)) =>
          neighbors.map(neighbor => (neighbor, myKShell))
        }
        .groupByKey()
        .mapValues(_.toList)

      val updatedRDD = kShellRDD.leftOuterJoin(neighborKShellsRDD).mapValues {
        case (currentK, neighborKsOpt) =>
          neighborKsOpt match {
            case Some(neighborKs) =>
              val sortedKs = neighborKs.sorted
              var newK = currentK
              breakable {
                for (k <- 1 to currentK) {
                  val countGEK = sortedKs.count(_ >= k)
                  if (countGEK < k) {
                    newK = k - 1
                    break
                  }
                }
              }
              newK
            case None => currentK
          }
      }.cache()

      val diffCount = kShellRDD.join(updatedRDD).filter { case (_, (a, b)) => a != b }.count()
      kShellRDD.unpersist(false)
      kShellRDD = updatedRDD
      iteration += 1
      if (diffCount == 0) changed = false
    }

    kShellRDD
  }

  // =========================================================
  // Spark-MLSC: H-index
  // =========================================================
  def computeCentralityFullyDistributed(
                                         graph: Graph[Int, Int],
                                         neighborsRDD: VertexRDD[List[VertexId]],
                                         kShellRDD: RDD[(VertexId, Int)]
                                       ): RDD[(VertexId, (Double, Int, Int))] = {
    val N = graph.numVertices.toDouble
    val maxDegree = neighborsRDD.map { case (_, neighbors) => neighbors.size }.max().toDouble

    neighborsRDD.join(kShellRDD).map { case (node, (neighbors, kShell)) =>
      val degreeCentrality = if (N > 1) neighbors.size / (N - 1) else 0.0
      val neighborDegrees = neighbors.map(_ => neighbors.size)
      val sortedDegrees = neighborDegrees.sorted(Ordering[Int].reverse)

      var h = 0
      val L = sortedDegrees.length.toDouble
      breakable {
        for (i <- sortedDegrees.indices) {
          val degree = sortedDegrees(i)
          val term = degree + (((if (maxDegree > 0) L / maxDegree else 0.0) + degreeCentrality + ((i + 1).toDouble / math.max(L, 1.0))) * degree)
          if (term >= i + 1) h = i + 1 else break
        }
      }

      (node, (degreeCentrality, kShell, h))
    }
  }

  // =========================================================
  // Spark-MLSC: dominance
  // =========================================================
  def computeDominanceFullyDistributed(
                                        graph: Graph[Int, Int],
                                        neighborsRDD: VertexRDD[List[VertexId]],
                                        centralityRDD: RDD[(VertexId, (Double, Int, Int))]
                                      ): RDD[(VertexId, Double)] = {

    val vertexDataRDD = neighborsRDD.join(centralityRDD).mapValues {
      case (neighbors, (degreeCentrality, kShell, hIndex)) =>
        val ksImproved = kShell + (kShell * degreeCentrality)
        val degree = neighbors.size
        (ksImproved, hIndex, degree, neighbors.toSet)
    }

    val richGraph = graph.outerJoinVertices(vertexDataRDD) { (_, _, attrOpt) =>
      attrOpt.getOrElse((0.0, 0, 0, Set.empty[VertexId]))
    }

    val sigmaRDD = richGraph.aggregateMessages[Double](
      ctx => {
        val srcAttr = ctx.srcAttr
        val dstAttr = ctx.dstAttr

        val srcKsImp = srcAttr._1
        val dstKsImp = dstAttr._1
        val srcDegree = srcAttr._3
        val dstDegree = dstAttr._3
        val srcNeighbors = srcAttr._4
        val dstNeighbors = dstAttr._4

        val commonNeighbors = srcNeighbors.intersect(dstNeighbors).size.toDouble

        if (srcDegree > 0) {
          val msgToSrc = dstKsImp * (1.0 + commonNeighbors / srcDegree)
          ctx.sendToSrc(msgToSrc)
        }
        if (dstDegree > 0) {
          val msgToDst = srcKsImp * (1.0 + commonNeighbors / dstDegree)
          ctx.sendToDst(msgToDst)
        }
      },
      _ + _
    )

    vertexDataRDD.leftOuterJoin(sigmaRDD).map {
      case (vid, ((ksImproved, hIndex, _, _), sumOpt)) =>
        val sigmaSum = sumOpt.getOrElse(0.0)
        val dominance = hIndex + ksImproved + sigmaSum
        (vid, dominance)
    }
  }

  // =========================================================
  // Spark-MLSC: importance
  // =========================================================
  def computeImportanceFullyDistributed(
                                         graph: Graph[Int, Int],
                                         neighborsRDD: VertexRDD[List[VertexId]],
                                         dominanceRDD: RDD[(VertexId, Double)]
                                       ): RDD[(VertexId, Double)] = {

    val vDataRDD = neighborsRDD.join(dominanceRDD).mapValues {
      case (neighbors, dom) => (neighbors.size, dom, neighbors.toSet)
    }

    val richGraph1 = graph.outerJoinVertices(vDataRDD) { (_, _, attrOpt) =>
      attrOpt.getOrElse((0, 0.0, Set.empty[VertexId]))
    }

    val neighborDominanceStats = richGraph1.aggregateMessages[(Double, Int)](
      ctx => {
        val srcDeg = ctx.srcAttr._1
        val dstDeg = ctx.dstAttr._1
        val srcDom = ctx.srcAttr._2
        val dstDom = ctx.dstAttr._2

        if (dstDeg > 1) ctx.sendToSrc((dstDom, 1))
        if (srcDeg > 1) ctx.sendToDst((srcDom, 1))
      },
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )

    val graphWithAvgDom = richGraph1.outerJoinVertices(neighborDominanceStats) {
      (_, attr, statsOpt) =>
        val (sumDom, count) = statsOpt.getOrElse((0.0, 0))
        val avgDom = if (count == 0) 0.0 else sumDom / count
        (attr._1, attr._2, attr._3, avgDom)
    }

    val importanceMsg = graphWithAvgDom.aggregateMessages[Double](
      ctx => {
        val srcDeg = ctx.srcAttr._1
        val dstDeg = ctx.dstAttr._1
        val srcDom = ctx.srcAttr._2
        val dstDom = ctx.dstAttr._2
        val srcNbrs = ctx.srcAttr._3
        val dstNbrs = ctx.dstAttr._3
        val srcAvg = ctx.srcAttr._4
        val dstAvg = ctx.dstAttr._4

        val cn = srcNbrs.intersect(dstNbrs).size
        val unionSize = srcDeg + dstDeg - cn
        val jaccard = if (unionSize > 0) cn.toDouble / unionSize else 0.0

        if (srcAvg > 0 && dstDeg > 1) {
          val term = (dstDom / (srcAvg + 1.0)) * jaccard * math.log1p(cn)
          ctx.sendToSrc(term)
        }
        if (dstAvg > 0 && srcDeg > 1) {
          val term = (srcDom / (dstAvg + 1.0)) * jaccard * math.log1p(cn)
          ctx.sendToDst(term)
        }
      },
      _ + _
    )

    graphWithAvgDom.vertices.leftOuterJoin(importanceMsg).map {
      case (vid, (attr, sumOpt)) =>
        val dominance = attr._2
        val sum = sumOpt.getOrElse(0.0)
        (vid, sum + dominance)
    }
  }

  // =========================================================
  // 核心节点选择 + 两级核心扩散
  // =========================================================
  def performPLDLSCoreSelectionAndTwoLevelDiffusion(
                                                     graph: Graph[Int, Int],
                                                     neighborsRDD: VertexRDD[List[VertexId]],
                                                     importanceRDD: RDD[(VertexId, Double)]
                                                   ): RDD[(VertexId, VertexId)] = {

    val sc = graph.vertices.sparkContext
    val degreeRDD = neighborsRDD.mapValues(_.size).cache()
    val nodeNumber = graph.numVertices.toInt

    val nodeInfoMap = importanceRDD.join(neighborsRDD).join(degreeRDD).map {
      case (vid, ((importance, neighbors), degree)) =>
        (vid, (importance, neighbors, degree))
    }.collectAsMap()

    val nodeInfoBC = sc.broadcast(nodeInfoMap)

    val averageImportance = importanceRDD.values.sum() / math.max(nodeNumber, 1)

    val grouped = importanceRDD
      .filter(_._2 >= averageImportance)
      .map { case (_, imp) => (math.round(imp).toLong, 1) }
      .reduceByKey(_ + _)

    val modeValue = grouped.map(_._2).max()
    val modes = grouped.filter(_._2 == modeValue).map(_._1)
    val finalMode =
      if (modes.count() > 1) modes.min().toDouble
      else modes.max().toDouble

    println(s"[PLDLS-core] averageImportance=$averageImportance, finalMode=$finalMode")

    val seedNodes = importanceRDD.filter(_._2 >= finalMode).cache()

    var workGraph: Graph[NodeProperty, Int] =
      graph.mapVertices { case (vid, _) => NodeProperty(vid, isCoreNode = false) }.cache()

    val coreAndMaxNeighbor = seedNodes.mapPartitions { iter =>
      val nodeInfo = nodeInfoBC.value
      iter.flatMap {
        case (vid, importance) =>
          nodeInfo.get(vid).flatMap {
            case (_, neighbors, _) =>
              val neighborCandidates = neighbors.flatMap { nbr =>
                nodeInfo.get(nbr).map(x => (nbr, x._1))
              }
              if (neighborCandidates.nonEmpty) {
                val bestNbr = neighborCandidates.maxBy(_._2)
                Some((vid, importance, bestNbr))
              } else {
                Some((vid, importance, (vid, importance)))
              }
          }
      }
    }.cache()

    val flinRDD = coreAndMaxNeighbor
      .map(x => x._3._1)
      .distinct()
      .map(v => (v, nodeInfoBC.value.get(v).map(_._2).getOrElse(List.empty[VertexId])))
      .cache()

    val coreLabelSelect = coreAndMaxNeighbor.map {
      case (vid, myImp, (bestNbr, bestNbrImp)) =>
        if (myImp > bestNbrImp) (vid, NodeProperty(vid, isCoreNode = true))
        else (vid, NodeProperty(bestNbr, isCoreNode = true))
    }.cache()

    workGraph = workGraph.outerJoinVertices(coreLabelSelect) {
      (_, oldAttr, newAttr) => newAttr.getOrElse(oldAttr)
    }.cache()

    val commonNeighbors = coreAndMaxNeighbor.flatMap {
      case (vid, _, (bestNbr, _)) =>
        val myNbrs = nodeInfoBC.value.get(vid).map(_._2).getOrElse(List.empty[VertexId]).toSet
        val bestNbrNbrs = nodeInfoBC.value.get(bestNbr).map(_._2).getOrElse(List.empty[VertexId]).toSet
        val commons = myNbrs.intersect(bestNbrNbrs)
        commons.map(cn => (cn, vid, bestNbr))
    }.map { case (commonNode, vid, bestNbr) =>
      val impVid = nodeInfoBC.value.get(vid).map(_._1).getOrElse(0.0)
      val impBestNbr = nodeInfoBC.value.get(bestNbr).map(_._1).getOrElse(0.0)
      if (impVid >= impBestNbr) (commonNode, vid) else (commonNode, bestNbr)
    }.reduceByKey { (x, y) =>
      val impX = nodeInfoBC.value.get(x).map(_._1).getOrElse(0.0)
      val impY = nodeInfoBC.value.get(y).map(_._1).getOrElse(0.0)
      if (impX >= impY) x else y
    }.map { case (node, labelOwner) =>
      (node, NodeProperty(labelOwner, isCoreNode = true))
    }.cache()

    workGraph = workGraph.outerJoinVertices(commonNeighbors) {
      (_, oldAttr, newAttr) => newAttr.getOrElse(oldAttr)
    }.cache()

    val flinNeighbors = flinRDD.join(workGraph.vertices).flatMap {
      case (flinNode, (flinNbrs, flinProp)) =>
        val flinLabel = flinProp.label
        flinNbrs.flatMap { y =>
          val maybeX = nodeInfoBC.value.get(flinNode)
          val maybeY = nodeInfoBC.value.get(y)
          (maybeX, maybeY) match {
            case (Some((impX, nbrX, degreeX)), Some((impY, nbrY, degreeY))) =>
              val intrsct = nbrX.toSet.intersect(nbrY.toSet).size.toDouble
              val unionn = degreeX.toDouble + degreeY.toDouble
              val ratio = if (unionn > 0) intrsct / unionn else 0.0

              if (impX >= impY && ratio >= 0.5) {
                Some((y, flinLabel))
              } else {
                None
              }
            case _ => None
          }
        }
    }.reduceByKey { (x, y) =>
      val impX = nodeInfoBC.value.get(x).map(_._1).getOrElse(0.0)
      val impY = nodeInfoBC.value.get(y).map(_._1).getOrElse(0.0)
      if (impX >= impY) x else y
    }.map { case (node, labelOwner) =>
      (node, NodeProperty(labelOwner, isCoreNode = true))
    }.cache()

    workGraph = workGraph.outerJoinVertices(flinNeighbors) {
      (_, oldAttr, newAttr) => newAttr.getOrElse(oldAttr)
    }.cache()

    workGraph.vertices.map { case (vid, prop) => (vid, prop.label) }
  }

  // =========================================================
  // 重要性梯度传播
  // =========================================================
  def performImportanceGuidedPropagationWithInitialLabels(
                                                           graph: Graph[Int, Int],
                                                           importanceRDD: RDD[(VertexId, Double)],
                                                           initialLabelsRDD: RDD[(VertexId, VertexId)]
                                                         ): RDD[(VertexId, VertexId)] = {

    val sc = graph.vertices.sparkContext
    val importanceMap = importanceRDD.collectAsMap()
    val importanceBC = sc.broadcast(importanceMap)

    val initializedGraph = graph.outerJoinVertices(initialLabelsRDD) {
      case (vid, _, labelOpt) => labelOpt.getOrElse(vid)
    }

    val finalGraph = initializedGraph.pregel(
      initialMsg = -1L,
      maxIterations = 8,
      activeDirection = EdgeDirection.Either
    )(
      vprog = (_, currentLabel, message) => {
        if (message == -1L) currentLabel else message
      },
      sendMsg = triplet => {
        val srcImportance = importanceBC.value.getOrElse(triplet.srcId, 0.0)
        val dstImportance = importanceBC.value.getOrElse(triplet.dstId, 0.0)

        if (srcImportance > dstImportance) {
          Iterator((triplet.dstId, triplet.srcAttr))
        } else if (dstImportance > srcImportance) {
          Iterator((triplet.srcId, triplet.dstAttr))
        } else {
          Iterator.empty
        }
      },
      mergeMsg = (msg1, msg2) => {
        val importance1 = importanceBC.value.getOrElse(msg1, 0.0)
        val importance2 = importanceBC.value.getOrElse(msg2, 0.0)
        if (importance1 >= importance2) msg1 else msg2
      }
    )

    finalGraph.vertices.map { case (vid, label) => (vid, label) }
  }

  // =========================================================
  // 社区合并
  // =========================================================
  def performCommunityMergeFullyDistributed(
                                             graph: Graph[Int, Int],
                                             neighborsRDD: VertexRDD[List[VertexId]],
                                             labelsRDD: RDD[(VertexId, VertexId)],
                                             importanceRDD: RDD[(VertexId, Double)]
                                           ): RDD[(VertexId, VertexId)] = {

    val sc = graph.vertices.sparkContext
    val degreesRDD = neighborsRDD.mapValues(_.size)

    val commSizes = labelsRDD.join(degreesRDD)
      .filter { case (_, (_, degree)) => degree > 1 }
      .map { case (_, (label, _)) => (label, 1) }
      .reduceByKey(_ + _)
      .collectAsMap()

    if (commSizes.nonEmpty) {
      val avgSize = commSizes.values.sum.toDouble / commSizes.size
      val smallCommunities = commSizes.filter(_._2 <= avgSize).keySet
      val smallCommBC = sc.broadcast(smallCommunities)

      val reps = labelsRDD.join(importanceRDD)
        .filter { case (_, (label, _)) => smallCommBC.value.contains(label) }
        .map { case (vid, (label, totalImportance)) => (label, (vid, totalImportance)) }
        .reduceByKey((a, b) => if (a._2 > b._2) a else b)
        .map(_._2._1)
        .collect().toSet
      val repsBC = sc.broadcast(reps)

      val vertexData = labelsRDD.join(degreesRDD)
      val enrichedGraph = graph.outerJoinVertices(vertexData) { (_, _, attrOpt) =>
        attrOpt.getOrElse((-1L, 0))
      }

      val mergeCandidates = enrichedGraph.aggregateMessages[(VertexId, Double)](
        ctx => {
          val srcLabel = ctx.srcAttr._1
          val dstLabel = ctx.dstAttr._1
          val srcDeg = ctx.srcAttr._2
          val dstDeg = ctx.dstAttr._2

          if (srcLabel != dstLabel && srcLabel != -1L && dstLabel != -1L) {
            if (repsBC.value.contains(ctx.srcId) && dstDeg > 1) {
              val score = 1.0 + ctx.attr + dstDeg
              ctx.sendToSrc((dstLabel, score.toDouble))
            }
            if (repsBC.value.contains(ctx.dstId) && srcDeg > 1) {
              val score = 1.0 + ctx.attr + srcDeg
              ctx.sendToDst((srcLabel, score.toDouble))
            }
          }
        },
        (a, b) => if (a._2 > b._2) a else b
      )

      val communityMergeMap = labelsRDD.join(mergeCandidates).map {
        case (_, (myLabel, (targetLabel, _))) =>
          (myLabel, targetLabel)
      }.collectAsMap()

      val mergeMapBC = sc.broadcast(communityMergeMap)

      labelsRDD.map { case (vid, currentLabel) =>
        val newLabel = mergeMapBC.value.getOrElse(currentLabel, currentLabel)
        (vid, newLabel)
      }
    } else {
      labelsRDD
    }
  }

  // 度为1节点延迟归并
  def handleDegreeOneNodesFullyDistributed(
                                            graph: Graph[Int, Int],
                                            neighborsRDD: VertexRDD[List[VertexId]],
                                            labelsRDD: RDD[(VertexId, VertexId)]
                                          ): RDD[(VertexId, VertexId)] = {

    val vertexData = neighborsRDD.join(labelsRDD).mapValues {
      case (neighbors, label) => (neighbors.size, label)
    }

    val labeledGraph = graph.outerJoinVertices(vertexData) { (vid, _, attrOpt) =>
      attrOpt.getOrElse((0, vid))
    }

    val degreeOneUpdates = labeledGraph.aggregateMessages[VertexId](
      ctx => {
        val srcDeg = ctx.srcAttr._1
        val dstDeg = ctx.dstAttr._1

        if (srcDeg == 1) ctx.sendToSrc(ctx.dstAttr._2)
        if (dstDeg == 1) ctx.sendToDst(ctx.srcAttr._2)
      },
      (a, _) => a
    )

    labeledGraph.vertices.leftOuterJoin(degreeOneUpdates).map {
      case (vid, (attr, updatedLabelOpt)) =>
        val degree = attr._1
        val currentLabel = attr._2

        if (degree == 1) {
          (vid, updatedLabelOpt.getOrElse(currentLabel))
        } else {
          (vid, currentLabel)
        }
    }
  }

  def main(args: Array[String]): Unit = {
    val APPNAME = "SparkMLSC_PLDLSCore_SMLSCMerge"

    val options = parse_command_line(args)

    options match {
      case Some(config) =>
        val conf = new SparkConf().setAppName(APPNAME)
        conf.registerKryoClasses(Array(classOf[DNASeq]))

        val spark = SparkSession
          .builder()
          .config(conf)
          .appName(APPNAME)
          .getOrCreate()

        run(config, spark)

        if (config.sleep > 0) Thread.sleep(config.sleep * 1000)
        spark.stop()

      case None =>
        println("bad arguments")
        sys.exit(-1)
    }
  }
}
