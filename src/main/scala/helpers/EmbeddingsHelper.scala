package helpers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedWriter, OutputStreamWriter}
import scala.util.{Failure, Success, Try}
import utils.TokenWordings

object EmbeddingsHelper {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def createShards(tokens: Seq[Int], numShards: Int): Seq[Seq[Int]] = {
    val shardSize = Math.ceil(tokens.length.toDouble / numShards).toInt
    tokens.grouped(shardSize).toSeq
  }

  def writeShardToFile(shard: Seq[Int], filename: String, fs: FileSystem): Unit = {
    val outputPath = new Path(filename)
    val outputStream = fs.create(outputPath)
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    try {
      writer.write(shard.mkString(" "))
    } finally {
      writer.close()
    }
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      logger.error("Usage: EmbeddingsHelper.main /merged_tokens /embeddings_input")
      System.exit(1)
    }

    val mergedTokens = args(0)
    val embeddingsInput = args(1)

    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    try {
      val (tokens, _) = TokenWordings.getTokenWordings(mergedTokens)
      logger.info(s"Extracted ${tokens.length} tokens")

      val numShards = 5

      val inputDir = new Path(embeddingsInput)
      if (!fs.exists(inputDir)) {
        fs.mkdirs(inputDir)
      }

      // Create shards
      val shards = createShards(tokens, numShards)

      shards.foreach { shard =>
        logger.info("Shard length", shard.length)
      }

      // Create input files for each shard
      shards.zipWithIndex.foreach { case (shard, index) =>
        val inputFile = new Path(inputDir, s"input_$index.txt")
        writeShardToFile(shard, inputFile.toString, fs)
      }

    } catch {
      case e: Exception =>
        logger.error(s"An error occurred in main: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      fs.close()
    }
  }
}