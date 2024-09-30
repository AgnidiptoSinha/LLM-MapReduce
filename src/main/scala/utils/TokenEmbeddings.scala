package utils

import scala.collection.mutable
import scala.io.Source

object TokenEmbeddings {
  def getTokenEmbeddings: (Seq[Int], mutable.Map[Int, Seq[Float]]) = {
    val file = Source.fromFile("./embeddings_output/part-r-00000")
    val lines = file.getLines().toList
    file.close()

    val embeddingsHashmap = mutable.Map.empty[Int, Seq[Float]]
    lines.foreach( line => {
      val arr = line.split(",")
      if(arr.length == 13) {
        val token = arr(2).toInt
        val embeddings = arr.slice(3, arr.length).map(_.toFloat).toSeq
        embeddingsHashmap.put(token, embeddings)
      } else {
        val token = arr(1).toInt
        val embeddings = arr.slice(2, arr.length).map(_.toFloat).toSeq
        embeddingsHashmap.put(token, embeddings)
      }
    })

    (embeddingsHashmap.keys.toSeq, embeddingsHashmap)
  }
}
