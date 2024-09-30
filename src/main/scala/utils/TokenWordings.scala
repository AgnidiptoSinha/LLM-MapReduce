package utils

import scala.collection.mutable
import scala.io.Source

object TokenWordings {

  def getTokenWordings: (Seq[Int], mutable.Map[Int, String]) = {
    val file = Source.fromFile("./wordcount_output/part-r-00000")
    val lines = file.getLines().toList
    file.close()

    val wordingsHashmap = mutable.Map.empty[Int, String]
    lines.foreach( line => {
      val arr = line.split(" ")
        val token = arr(2).toInt
        val word = arr(0)
        wordingsHashmap.put(token, word)
    })

    (wordingsHashmap.keys.toSeq, wordingsHashmap)
  }
}
