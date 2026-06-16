package tools

import java.io._
import java.nio.file.Paths

import com.twitter.chill.{Input, Kryo, Output}
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import com.google.common.net.InetAddresses
//import org.spark_project.guava.net.InetAddresses
/**
  * Created by Lizhen Shi on 5/14/17.
  */
object Utils {
  //a % b
  def pos_mod(a: Int, k: Int): Int = ((a % k) + k) % k


  def VERSION = "0.6"
 // get_files:用来获取正确的文件路径
  def get_files(input: String, pattern: String): String = {
    if (input.startsWith("hdfs:") || pattern.length() == 0)
      input//如果传来的input是“hdfs:”开头，且pattern长度是0，就返回input中的字符串
    else {
      val dir: File = new File(input)//新建一个dir存放input中的文件路径字符串
      val fileFilter = new WildcardFileFilter(pattern)//过滤文件，只保留pattern格式的文件
      val files: Array[String] = dir.list(fileFilter).map(Paths.get(input, _).toString).sorted
      // Path path = Paths.get("F:", "Speed.log");---> F:\Speed.log
      files.mkString(",")//将输入的文件目录地址，用","分割
    }
  }

  def write_textfile(filename: String, lines: Iterable[String]): Unit = {

    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename)))
    for (x <- lines) {
      writer.write(x)
      writer.write("\n")
    }
    writer.close()
  }

  def kyro_object(filename: String, o: Object): String = {
    val kryo = new Kryo()
    val output = new Output(new FileOutputStream(filename))
    kryo.writeClassAndObject(output, o)
    output.close()
    filename
  }

  def unkyro_object(filename: String): AnyRef = {
    val kryo = new Kryo()
    val input = new Input(new FileInputStream(filename))
    kryo.readClassAndObject(input)
  }

  def serialize_object(filename: String, o: Object): String = {

    val fileOut: FileOutputStream = new FileOutputStream(filename)
    val out: ObjectOutputStream = new ObjectOutputStream(fileOut)
    out.writeObject(o)
    out.close()
    fileOut.close()
    filename
  }

  def unserialize_object(filename: String): AnyRef = {
    val fileIn: FileInputStream = new FileInputStream(filename)
    val in: ObjectInputStream = new ObjectInputStream(fileIn)
    val o = in.readObject
    in.close()
    fileIn.close()
    o
  }

  def parseIpPort(x: String): (String, Int) = {
    val lst = x.split(":")
    if (lst.length != 2
      || (util.Try(lst(1).toInt).isFailure || lst(1).toInt < 1)
      || !InetAddresses.isInetAddress(lst(0)))
      ("", -1)
    else {
      val lst = x.split(":")
      val ip = lst(0)
      val port = lst(1).toInt
      (ip, port)
    }
  }


  def toByteArray(value: Int): Array[Byte] = {
    Array[Byte]((value >> 24).toByte, (value >> 16).toByte, (value >> 8).toByte, value.toByte)
  }
}
