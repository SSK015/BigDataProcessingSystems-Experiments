import java.io.File
import scala.io.Source

object WordCountApp {

  def main(args: Array[String]): Unit = {
    //文件路径
    val filePath = "/data/ywxia/BigDataProcessingSystems-Experiments/lab2-spark/exp/exp2/AChristmasCarol_CharlesDickens_Dutch.txt"
    val codec = "utf-8"
    //打开文件
    val file = Source.fromFile(filePath, codec)  
    val wc = file.getLines().flatMap(_.split("\t")).toList.map((_, 1)).groupBy((_._1)).mapValues(_.size)
println(wc)
    // 关闭文件
    file.close()
  }
}



