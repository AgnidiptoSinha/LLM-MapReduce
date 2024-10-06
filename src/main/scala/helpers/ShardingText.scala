package helpers

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import scala.util.{Failure, Success, Try}

object ShardingText {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  class OrderedFileShardingHDFS(inputDir: String, outputDir: String, numShards: Int = 5) {

    private val conf = new Configuration()
    private val fs = FileSystem.get(conf)

    def shardAndUpload(): Unit = {
      val inputPath = new Path(inputDir)
      val outputPath = new Path(outputDir)

      if (!fs.exists(outputPath)) {
        fs.mkdirs(outputPath)
      }

      val inputFiles = fs.listStatus(inputPath).filter(_.isFile).map(_.getPath)
      logger.info(s"Found ${inputFiles.length} input files")

      val writers = (0 until numShards).map { i =>
        val shardPath = new Path(outputPath, s"shard_$i.txt")
        val output = fs.create(shardPath)
        new BufferedWriter(new OutputStreamWriter(output))
      }.toArray

      try {
        var globalLineNumber = 0L //var used because globalLineNumber is being updated and written into the files repeatedly
        inputFiles.foreach { inputFile =>
          val input = fs.open(inputFile)
          val reader = new BufferedReader(new InputStreamReader(input))
          try {
            var line: String = null // var used being line is being read continuously
            while ({line = reader.readLine(); line != null}) {
              val shardIndex = (globalLineNumber % numShards).toInt
              writers(shardIndex).write(s"$globalLineNumber\t$line\n")
              globalLineNumber += 1

              if (globalLineNumber % 10000 == 0) {
                logger.info(s"Processed $globalLineNumber lines")
              }
            }
          } finally {
            IOUtils.closeStream(reader)
          }
        }
        logger.info(s"Total lines processed: $globalLineNumber")
      } finally {
        writers.foreach(IOUtils.closeStream)
      }

      logger.info(s"Created $numShards shards in $outputDir")
    }
  }

  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load()

    if(args.length != 2) {
      logger.error("Invalid arguments passed to Main.")
      logger.error("ShardingText <input_path> <output_path>")
      System.exit(1)
    }

    val inputDir = args(0)
    val outputDir = args(1)
    val numShards = config.getInt("myapp.numShards")

    val sharding = new OrderedFileShardingHDFS(inputDir, outputDir, numShards)
    sharding.shardAndUpload()
  }
}