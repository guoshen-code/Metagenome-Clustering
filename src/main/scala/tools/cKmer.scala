package tools

import net.sparc.sparc

/**
 * Created by Lizhen Shi on 9/27/18.
 */
object cKmer {

  net.sparc.Info.load_native()


  // generates kmers--生成kmers
  def generate_kmer(seq: String, k: Int, is_canonical: Boolean = true): Array[DNASeq] = {
    val v = sparc.generate_kmer(seq, k, 'N', is_canonical)
    (0 until v.size().toInt).map(x => DNASeq.from_bases(v.get(x))).toArray
  }

  // 修改
  //  def generate_kmer(seq: String, k: Int, is_canonical: Boolean = true): Array[DNASeq] = {
  //    if (seq.length < k) {
  //      Array.empty[DNASeq]
  //    } else {
  //      val first = seq.substring(0, k)
  //      val last = seq.substring(seq.length - k, seq.length)
  //
  //      val first_kmer = DNASeq.from_bases(if (is_canonical) canonical_kmer(first) else first)
  //      val last_kmer = DNASeq.from_bases(if (is_canonical) canonical_kmer(last) else last)
  //
  //      // 如果first和last是一样的，不重复添加
  //      if (first == last) Array(first_kmer)
  //      else Array(first_kmer, last_kmer)
  //    }
  //  }
  //
  //  def canonical_kmer(kmer: String): String = {
  //    val rc_kmer = DNASeq.reverse_complement(kmer)
  //    if (kmer < rc_kmer) kmer else rc_kmer
  //  }
  // 修改
  //生成重叠区
  def sequence_overlap(seq1: String, seq2: String, min_over_lap: Int, err_rate: Float) =
    sparc.sequence_overlap(seq1, seq2, min_over_lap, err_rate)

  //生成边
  def generate_edges(reads: Array[Int], max_degree: Int) = {
    val v = new net.sparc.intVector(reads.length)
    reads.indices.foreach(i => v.set(i, reads(i)))
    val results = sparc.generate_edges(v, max_degree);
    (0 until results.size().toInt).map {
      i =>
        val a = results.get(i)
        (a.getFirst, a.getSecond)
    }
  }
}
