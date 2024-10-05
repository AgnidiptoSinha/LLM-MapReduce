package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.io.Source

object TokenWordings {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getTokenWordings(dir:String) : (Seq[Int], mutable.Map[Int, String]) = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val hdfsPath = new Path(dir+"/merged_output.txt")

    if (!fs.exists(hdfsPath)) {
      throw new IllegalStateException(s"File does not exist: $hdfsPath")
    }

    val inputStream = fs.open(hdfsPath)
    val source = Source.fromInputStream(inputStream)

    try {
      val lines = source.getLines().toList

      val wordingsHashmap = mutable.Map.empty[Int, String]
      lines.foreach { line =>
        val arr = line.split(" ")
        if (arr.length >= 3) {
          val token = arr(2).toInt
          val word = arr(0)
          wordingsHashmap.put(token, word)
        } else {
          logger.warn(s"Skipping malformed line: $line")
        }
      }

      logger.info("Generated Token to Wordings Hashmap!")
      logger.info(s"Number of Tokens : ${wordingsHashmap.keys.toSeq.length}")
      (wordingsHashmap.keys.toSeq, wordingsHashmap)
    } finally {
      source.close()
      inputStream.close()
    }
  }
}