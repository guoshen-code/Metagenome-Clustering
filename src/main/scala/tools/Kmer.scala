package tools

/**
  * Created by Lizhen Shi on 5/13/17.
  */
object Kmer {

  private def canonical_kmer(seq: String) = {
    val rc = DNASeq.reverse_complement(seq)//将DNA序列转换成反向互补序列rc
    if (rc < seq) rc else seq
  }

  // generates kmers hash values given the seq (doesn't handle 'N' and reverse complement)
  //k-mer划分核心算法---返回每一行seq处理过的k-mer的byte
  private def _generate_kmer(seq: String, k: Int) = {
    (0 until (seq.length - k + 1)).map {       //k-mer划分算法,可将seq划分成(seq.length-k+1)个长度为k的k-mer
      i =>val subseq = seq.substring(i, i + k) //substring:取i--i+k之间长度的seq转为字符串作为一个k-mer
        //subseq依次存储seq中的每个k-mer，
        DNASeq.from_bases(canonical_kmer(subseq))
    }
  }


  // generates kmers
  //readsRDD传递给形参seq  k:k-mer的长度
  //函数返回所有已处理k-mer的序列集合
  def generate_kmer(seq: String, k: Int): Array[DNASeq] = {
    seq.split("N").flatMap {
      subSeq => {//subseq一次接收一行
        _generate_kmer(subSeq, k)
      }
    }.distinct
  }

  def main(args: Array[String]) = {
    implicit class Crossable[X](xs: Traversable[X]) {
      def cross[Y](ys: Traversable[Y]) = for {x <- xs; y <- ys} yield (x, y)
    }

    def f(a1: Seq[String]): String = {
      val a = a1.map(u => "\"" + u + "\"")
      println(a.size)
      s"Array(${a.mkString(",")})"
    }

    val a = Seq("A", "T", "C", "G")
    println(f(a))

    val b = a.cross(a).map(u => u.productIterator.toSeq.map(_.asInstanceOf[String]).mkString).toSeq.distinct
    println(f(b))

    val c = a.cross(b).map(u => u.productIterator.toSeq.map(_.asInstanceOf[String]).mkString).toSeq.distinct
    println(f(c))

    val d = a.cross(c).map(u => u.productIterator.toSeq.map(_.asInstanceOf[String]).mkString).toSeq.distinct
    println(f(d))
  }
}
