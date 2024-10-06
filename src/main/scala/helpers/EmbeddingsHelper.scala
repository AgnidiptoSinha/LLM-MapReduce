package helpers

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedWriter, OutputStreamWriter}
import org.apache.hadoop.io.IOUtils

import java.io.{BufferedReader, InputStreamReader}

object EmbeddingsHelper {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def extractAndConcatenateTokens(inputPath: String, outputPath: String, fs: FileSystem): Unit = {
    val inputDir = new Path(inputPath)
    val outputDir = new Path(outputPath)
    val outputFile = new Path(outputDir, "merged_output.txt")

    // Ensure output directory exists
    if (!fs.exists(outputDir)) {
      fs.mkdirs(outputDir)
    } else if (!fs.getFileStatus(outputDir).isDirectory) {
      throw new IllegalArgumentException(s"$outputPath exists but is not a directory")
    }

    // Check if output file already exists
    if (fs.exists(outputFile)) {
      logger.warn(s"Output file $outputFile already exists. It will be overwritten.")
      fs.delete(outputFile, false)
    }

    val outputStream = fs.create(outputFile)
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream))

    try {
      fs.listStatus(inputDir)
        .filter(_.isFile)
        .foreach { fileStatus =>
          val path = fileStatus.getPath
          val inputStream = fs.open(path)
          val reader = new BufferedReader(new InputStreamReader(inputStream))
          try {
            var line: String = null // line being read repeatedly, hence var is used here
            while ({line = reader.readLine(); line != null}) {
              line.split("\t", 2) match {
                case Array(_, tokenString) if tokenString.trim.nonEmpty =>
                  val tokens = tokenString.trim.split("\\s+").flatMap(token =>
                    try {
                      Some(token.toInt.toString)
                    } catch {
                      case _: NumberFormatException =>
                        logger.warn(s"Skipping non-integer token: $token")
                        None
                    }
                  )
                  writer.write(tokens.mkString(" "))
                  writer.write(" ")
                case _ => // Skip lines that don't match the expected format or are empty
              }
            }
          } finally {
            IOUtils.closeStream(reader)
          }
        }
    } finally {
      writer.close()
    }

    logger.info(s"Merged tokens written to $outputFile")
  }

  def createShards(inputPath: String, outputDir: String, numShards: Int, fs: FileSystem): Unit = {
    val inputFile = new Path(inputPath)
    val outputPath = new Path(outputDir)

    // Ensure output directory exists
    if (!fs.exists(outputPath)) {
      fs.mkdirs(outputPath)
    } else if (!fs.getFileStatus(outputPath).isDirectory) {
      throw new IllegalArgumentException(s"$outputDir exists but is not a directory")
    }

    val inputStream = fs.open(inputFile)
    val reader = new BufferedReader(new InputStreamReader(inputStream))

    val writers = (0 until numShards).map { i =>
      val shardPath = new Path(outputPath, s"shard_$i.txt")
      if (fs.exists(shardPath)) {
        logger.warn(s"Shard file $shardPath already exists. It will be overwritten.")
        fs.delete(shardPath, false)
      }
      val outputStream = fs.create(shardPath)
      new BufferedWriter(new OutputStreamWriter(outputStream))
    }.toArray

    try {
      var tokenCount = 0L // tokenCount is being updated as it reads the files
      val tokenBuilder = new StringBuilder()

      Iterator.continually(reader.read()).takeWhile(_ != -1).foreach { char =>
        char.toChar match {
          case ' ' =>
            if (tokenBuilder.nonEmpty) {
              val shardIndex = (tokenCount % numShards).toInt
              writers(shardIndex).write(tokenBuilder.toString())
              writers(shardIndex).write(' ')
              tokenCount += 1
              tokenBuilder.clear()
            }
          case c => tokenBuilder.append(c)
        }
      }

      // Handle the last token if exists
      if (tokenBuilder.nonEmpty) {
        val shardIndex = (tokenCount % numShards).toInt
        writers(shardIndex).write(tokenBuilder.toString())
        writers(shardIndex).write(' ')
      }

      logger.info(s"Created $numShards shards with a total of $tokenCount tokens")
    } finally {
      IOUtils.closeStream(reader)
      writers.foreach(IOUtils.closeStream)
    }
  }

  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load()
    if(args.length != 3) {
      logger.error("Usage: EmbeddingsHelper.main /tokenizer_output /merged_tokens /embeddings_input")
      System.exit(1)
    }

    val tokenizerOutput = args(0)
    val mergedTokens = args(1)
    val embeddingsInput = args(2)

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val numShards = config.getInt("myapp.numShardsEmbeddings")

    try {
      // Step 1: Extract and concatenate tokens
      logger.info(s"Extracting and concatenating tokens from $tokenizerOutput to $mergedTokens")
      extractAndConcatenateTokens(tokenizerOutput, mergedTokens, fs)

      // Step 2: Create shards
      logger.info(s"Creating $numShards shards in $embeddingsInput")
      createShards(s"$mergedTokens/merged_output.txt", embeddingsInput, numShards, fs)

      logger.info("Token extraction, merging, and sharding completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"An error occurred: ${e.getMessage}", e)
        e.printStackTrace()
        System.exit(1)
    } finally {
      fs.close()
    }
  }
}