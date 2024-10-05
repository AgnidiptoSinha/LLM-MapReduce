package utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}

object TokenEmbeddings {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getTokenEmbeddings(fs: FileSystem, dir: String): (Seq[Int], mutable.Map[Int, Seq[Float]]) = {

    val hdfsPath = new Path(s"$dir/merged_output.txt")

    if (!fs.exists(hdfsPath)) {
      throw new IllegalStateException(s"File does not exist: $hdfsPath")
    }

    val inputStream = fs.open(hdfsPath)
    val source = Source.fromInputStream(inputStream)

    try {
      val embeddingsHashmap = mutable.Map.empty[Int, Seq[Float]]

      source.getLines().foreach { line =>
        Try {
          val parts = line.split("\\s+", 2)
          if (parts.length == 2) {
            val token = parts(0).trim.toInt
            val embeddings = parts(1).trim.stripPrefix("[").stripSuffix("]").split(",").map(_.toFloat)
            embeddingsHashmap.put(token, embeddings)
          } else {
            logger.warn(s"Skipping malformed line: $line")
          }
        } match {
          case Success(_) => // Line processed successfully
          case Failure(e) => logger.warn(s"Error processing line: $line. Error: ${e.getMessage}")
        }
      }
      logger.info("Completed generating Token Embeddings Hashmap!")
      (embeddingsHashmap.keys.toSeq, embeddingsHashmap)
    } finally {
      source.close()
      inputStream.close()
    }
  }
}