import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import GraphLPA2.logInfo

import scala.reflect.ClassTag
//标签传播算法
object MyLabelPropagation {

    /**运行静态标签传播以检测网络中的社区。
      *
      *网络中的每个节点最初都分配给自己的社区。节点将其社区从属关系发送给所有邻居，并将其状态更新为传入消息的社区从属关系模式
      *
      *graph:要计算其社区从属关系的图形
      *maxSteps :要执行的LPA超级步数。因为这是一个静态实现，所以该算法将运行如此多的超级步。每一个超级步都会执行所有的函数
      *
      * 返回具有顶点属性的图，包含社区从属关系的标签
      *
      * label就是结点属性
      * 消息：Map[VertexId, Int]
      *       ①：获取初始化顶点label后的图g
      *       ②：向相邻结点发送label，确定相邻结点和结点的label
      *       ③：将同一结点收到的邻居结点发来的label进行合并
      */
      //算法过程描述
      //第一步：先给每个节点分配对应标签，即节点1对应标签1，节点i对应标签i；
      //第二步：遍历N个节点（for i=1：N），找到对应节点邻居，获取此节点邻居标签，找到出现次数最大标签，若出现次数最多标签不止一个，则随机选择一个标签替换成此节点标签；
      //第三步：若本轮标签重标记后，节点标签不再变化（或者达到设定的最大迭代次数），则迭代停止，否则重复第二步
      //graph:存储图的数据，图的数据类型是Graph，包含顶点和边
      // 其中顶点属性的数据类型是VD,边属性的数据类型是ED,顶点和边本身值的数据类型是Long


    def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
        logInfo(s"LPA1")
      require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")
      //初始化图顶点的属性(LPA标签)，开始时每个顶点的标签为顶点的id  eg:['a':a,'b':b,'c':c...] -->'顶点':顶点id(label)
     //使用mapVertices祛除边属性，原版(vid,attr)-->(vid)
      val lpaGraph = graph.mapVertices { case (vid, _) => vid }
        logInfo(s"LPA2")
        //如何确定两个顶点为邻居：通过两个顶点和edgeattr去确定一条边，那么一条边的两个顶点之间就互为邻居
        //如何获取邻居顶点的label：向源顶点发送目标顶点的attr(dst顶点的label)，向目标顶点发送源顶点的attr（src的label)
        //输入：是一个边的三元组[源顶点，目标顶点，边属性] 其中顶点属性是vectorid,边属性是ED
        //输出：存储多个邻居接点和邻居结点的label和label数量  Iterator[自身顶点，map[邻居顶点label，邻居顶点label数量(默认为1)]]
      def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Int])] = {
        Iterator((e.srcId, Map(e.dstAttr -> 1)), (e.dstId, Map(e.srcAttr -> 1)))
      }
        logInfo(s"LPA3")
      //对结点收到的所有消息进行合并，原理：对发送而来的Map，取Key进行合并，并对相同key的值进行累加操作，这样顶点函数就可以取出邻居数量最多的label了
      //输入：map[邻居顶点label即顶点属性，邻居顶点label数量]
      //输出：map[相同label邻居顶点label，相同label的邻居顶点label数量]---消息
        def mergeMessage(count1: Map[VertexId, Int], count2: Map[VertexId, Int]): Map[VertexId, Int] = {
        (count1.keySet ++ count2.keySet).map { i =>  //两端都是Iterator，可以用++操作连接  keySet:存放key值
          val count1Val = count1.getOrElse(i, 0)
          val count2Val = count2.getOrElse(i, 0)
          i -> (count1Val + count2Val)
        }.toMap
      }
        logInfo(s"LPA4")
      //顶点函数，若消息为空，则保持不变，否则取消息中数量最多的标签，即Map中value最大的key。
      //vertexProgram则是对node的操作，这里就是简单的把收到的message标签里面的值取出出现次数最多的nodeid用来更新本个node上的值。
       //输入: //(节点id，节点属性，消息）
        //输出邻居结点中标签数量最多的顶点
        def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Int]): VertexId = {
        if (message.isEmpty) attr else message.toSeq.sortBy(u=>(-u._2,u._1)).head._1//取当前顶点邻居结点中标签最多的顶点
      }
        logInfo(s"LPA5")
      //5.initialMessage 初始化消息，作用是在Pregel第一次运行的时候，所有图中的顶点都会接收到initMessage，激活所有顶点。
      val initialMessage = Map[VertexId, Int]()
        //lpaGraph存储的是顶点和边：并且顶点属性(label)初始化为每个顶点的id
        logInfo(s"LPA6")
      MyPregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
        vprog = vertexProgram, //将函数传递至pregel中
        sendMsg = sendMessage, //将函数传递至pregel中
        mergeMsg = mergeMessage)//将函数传递至pregel中
    }//函数柯里化：把一个参数列表的多个参数，变成多个参数列表。

}

//pregel；图计算模型
object MyPregel extends Logging {

  /**执行类似Pregel的迭代顶点并行抽象。用户定义的顶点程序“vprog”在上并行执行
     每个顶点接收任何入站消息并计算该顶点的新值。然后在所有输出边上调用'sendMsg'函数，
     并用于计算到目标顶点的可选消息。“mergeMsg”函数是一个交换关联函数，用于组合发送到同一顶点的消息

    *Pregel的计算过程：
          Pregel的计算过程是由一系列被称为“超步”的迭代组成的。
          在每个超步中，每个顶点上面都会并行执行用户自定义的函数，该函数描述了一个顶点V在一个超步S中需要执行的操作。
          该函数可以读取前一个超步(S-1)中其他顶点发送给顶点V的消息，执行相应计算后，修改顶点V及其出射边的状态，
          然后沿着顶点V的出射边发送消息给其他顶点，而且，一个消息可能经过多条边的传递后被发送到任意已知ID的目标顶点上去。
          这些消息将会在下一个超步(S+1)中被目标顶点接收，然后像上述过程一样开始下一个超步(S+1)的迭代过程。
          在Pregel计算过程中，一个算法什么时候可以结束，是由所有顶点的状态决定的。在第0个超步，所有顶点处于活跃状态。
          当一个顶点不需要继续执行进一步的计算时，就会把自己的状态设置为“停机”，进入非活跃状态。
    *
    * Pregel迭代过程:
          每个顶点从上一个superstep接收入站消息
          计算顶点新的属性值
          在下一个superstep中向相邻的顶点发送消息
          当没有剩余消息时，迭代结束
    *
    * @tparam VD the vertex data type  VD是顶点数据类型
    * @tparam ED the edge data type    ED是边缘数据类型
    * @tparam A the Pregel message type A是 Pregel消息类型
    *
    * @param graph the input graph.  graph  输入图形。
    *
    * @param initialMsg 图初始化开始模型计算的时候，即在第一次迭代中，所有顶点都会收到'initialMsg'
    *
    *message：[相同label邻居顶点label，相同label的邻居顶点label数量]
    *         先由sendmsgmap找到顶点的所有邻居顶点和邻居顶点的label，在再由mergemsg合并所有相同邻居顶点和相同邻居顶点的label
    *
    * @param maxIterations 要运行的最大迭代次数
    *
    * @param activeDirection  规定了发送消息的方向（默认是出边方向：EdgeDirection.Out）
    *                         指定边的哪一端在上一轮收到消息了，这条边运行sendMsg函数。
    *                        例如，如果这是“EdgeDirection.Out`，源点收到消息，边的三元组被激活
    *                        如果这是“EdgeDirection.Int`，终点收到消息，边的三元组被激活
    *                        默认值为“EdgeDirection.Either`，源点或终点收到消息，边的三元组被激活
    *                        如果这是“EdgeDirection.Both”，`sendMsg`源点和终点同时收到消息，边的三元组被激活
    *
    * @param vprog   在每个顶点上运行，计算顶点属性VD以及合并后的消息A(顶点计算后的结果),生成新的点属性
    * 在superstep 0，这个函数会在每个节点上以初始的initialMsg为参数运行并生成新的节点值。在随后的超步中只有当节点收到信息，该函数才会运行。
    * 在初始时，以及每轮迭代后，pregel会根据上一轮使用的msg和这里的vprod函数在图上调用joinVertices方法变化每个收到消息的节点
    * 通过用户实现的函数体+message来更新顶点原来的属性值。顶点接收的message应该是sendMsg+mergeMsg函数的结果。
    *  -->输入参数： 顶点ID,该顶点对应的顶点属性值,本轮迭代收到的message
       -->输出结果： 新的顶点属性值

    * @param sendMsg ：找邻居结点，并找到邻居结点的label
    *                  //如何确定两个顶点为邻居：通过两个顶点和edgeattr去确定一条边，那么一条边的两个顶点之间就互为邻居
                      //如何获取邻居顶点的label向源顶点发送目标顶点的attr(dst顶点的label)，向目标顶点发送源顶点的attr（src的label)
    *  -->输入：是一个边的三元组[源顶点，目标顶点，边属性] 其中顶点属性是vectorid,边属性是ED
       -->输出结果： 下一迭代的消息。

    * @param mergeMsg 如果一个节点接收到多条消息，先用mergeMsg将多条消息合成一条，如果节点收到一条消息，则不调用该函数
    *                 mergeMsg函数主要是合并传递给顶点的两个message。假设message类型为A，该函数的入参是两个类型为A的message，
    * -->输入参数：当前迭代中，一个顶点收到的2个A类型的message。
      -->输出结果：A类型的消息

    * @return the resulting graph at the end of the computation
    *
    * mapReduceTriplets：计算每个节点的相邻的边缘和顶点的值，用户定义的mapFunc函数会在图的每一条边调用
    *                    产生0或者多个message发送到这条边两个顶点其中一个当中，reduceFunc函数用来合并map阶段的输出到每个节点
    */
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED],//输入图形
   initialMsg: A,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD, //(节点id，节点属性，消息）=>节点属性
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)], //向相邻结点发送消息---(边元组）=> Iterator[(目标节点id，消息)]
   mergeMsg: (A, A) => A) // 将同一结点收到的消息合并---(消息，消息) => 消息
  : Graph[VD, ED] = {
    //1、要求最大迭代数大于0，不然报错。
    logInfo(s"pregel1")
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," +
      s" but got ${maxIterations}")
    //2、第一次迭代，对每个节点用vprog函数计算。初始化图所有顶点的属性信息
    logInfo(s"pregel2")
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    ///3、根据发送、聚合信息的函数计算下次迭代用的消息，消息：map[相同label邻居顶点label，相同label的邻居顶点label数量]。
    //消息：①：获取初始化顶点label后的图g  ②：向相邻结点发送label，确定相邻结点和结点的label ③：将同一结点收到的邻居结点发来的label进行合并
    logInfo(s"pregel3")
//    var messages = GraphXUtils.mapReduceTriplets(g, sendMsg, mergeMsg).persist(StorageLevel.MEMORY_AND_DISK_SER)
    // ==========================================================由于spark版本不支持，所以此处对messages变量的处理进行了修改
    var messages = g.aggregateMessages[A](
      ctx => {
        sendMsg(ctx.toEdgeTriplet).foreach {
          case (vid, msg) =>
            if (vid == ctx.srcId) ctx.sendToSrc(msg)
            else if (vid == ctx.dstId) ctx.sendToDst(msg)
        }
      },
      mergeMsg
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //4、数一下还有多少节点活跃
    logInfo(s"pregel4")
    var activeMessages = messages.count()
    ///5、下面进入循环迭代
    logInfo(s"pregel5")
    var prevG: Graph[VD, ED] = null
    var i = 0
    //大于迭代次数或者活跃消息为0时终止迭代
    while (activeMessages > 0 && i < maxIterations) {
      //6、接受消息并更新节点信息
      logInfo(s"pregel6")
      prevG = g
      //根据上一轮使用的messages和vprod函数在图上调用joinVertices方法进行join操作，改变每个激活节点的label
      g = g.joinVertices(messages)(vprog).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val oldMessages = messages
      oldMessages.unpersist(blocking = false)//旧消息的释放
     //根据发送、聚合信息的函数计算下次迭代用的消息。消息：map[相同label邻居顶点label，相同label的邻居顶点label数量]
      //①：获取初始化顶点label后的图g  ②：向相邻结点发送消息，确定相邻结点的label ③：将同一结点收到的消息进行合并
//      messages = GraphXUtils.mapReduceTriplets(g, sendMsg, mergeMsg,None).persist(StorageLevel.MEMORY_AND_DISK_SER)
      // ==========================================================由于spark版本不支持，所以此处对messages变量的处理进行了修改
      messages = g.aggregateMessages[A](
        ctx => {
          sendMsg(ctx.toEdgeTriplet).foreach {
            case (vid, msg) =>
              if (vid == ctx.srcId) ctx.sendToSrc(msg)
              else if (vid == ctx.dstId) ctx.sendToDst(msg)
          }
        },
        mergeMsg
      ).persist(StorageLevel.MEMORY_AND_DISK_SER)


      activeMessages = messages.count()//重新统计活跃消息个数

      logInfo("Pregel finished iteration " + i)

      // Unpersist the RDDs hidden by newly-materialized RDDs
      prevG.unpersistVertices(blocking = false)//旧图的释放
      prevG.edges.unpersist(blocking = false)
      // count the iteration
      i += 1
      System.gc()
    }
    messages.unpersist(blocking = false)
    g
  } // end of apply

} // end of class Pregel
