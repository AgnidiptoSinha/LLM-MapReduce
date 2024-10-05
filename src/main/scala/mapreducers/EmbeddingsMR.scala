package mapreducers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConverters._
import utils.{GetWindows, LearnEmbeddings}
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

object EmbeddingsMR {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  class EmbeddingsMapper extends Mapper[LongWritable, Text, IntWritable, Text] {
    val config: Config = ConfigFactory.load()
    private val mapperLogger: Logger = LoggerFactory.getLogger(this.getClass)
    val windowSize: Int = config.getInt("myapp.windowSize") //8
    val stride: Int = config.getInt("myapp.stride") //2
    val vocabSize: Int = config.getInt("myapp.vocabSize") //100243

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit = {
      val line = value.toString
      val tokens = line.split("\\s+").map(_.toInt).toSeq

      mapperLogger.debug(s"Mapper received: $line")
      val WindowGenerator = new GetWindows()
      val EmbeddingsGenerator = new LearnEmbeddings()

      val (windows, _) = WindowGenerator.getWindows(tokens, windowSize, stride)
      val learnedEmbeddings = EmbeddingsGenerator.getEmbeddings(windows, vocabSize)

      tokens.foreach(token => {
        val embeddings = learnedEmbeddings(token)
        val tokenInt = new IntWritable(token)
        val embeddingText = new Text(embeddings.mkString("[", ",", "]"))
        context.write(tokenInt, embeddingText)
        mapperLogger.trace(s"Mapper emitted: ($tokenInt, $embeddingText)")
      })
    }
  }

  class EmbeddingsReducer extends Reducer[IntWritable, Text, IntWritable, Text] {
    private val reducerLogger: Logger = LoggerFactory.getLogger(this.getClass)

    override def reduce(key: IntWritable, values: java.lang.Iterable[Text], context: Reducer[IntWritable, Text, IntWritable, Text]#Context): Unit = {
      val embeddings = values.asScala.toSeq flatMap { text =>
        Try {
          text.toString.trim.stripPrefix("[").stripSuffix("]").split(",").map(_.trim.toFloat)
        }.toOption
      }

      if (embeddings.nonEmpty) {
        val numDimensions = embeddings.head.length
        val summedEmbeddings = embeddings.reduce { (acc, curr) =>
          acc.zip(curr).map { case (a, b) => a + b }
        }
        val averagedEmbedding = summedEmbeddings.map(_ / embeddings.size)

        val resultText = new Text(averagedEmbedding.mkString("[", ",", "]"))
        context.write(key, resultText)
        reducerLogger.debug(s"Reducer emitted: ($key, $resultText)")
      } else {
        reducerLogger.warn(s"No valid embeddings found for key: $key")
      }
    }
  }

  def runJob(inputPath: String, outputPath: String): Boolean = {
    logger.info(s"Starting job with input path: $inputPath, output path: $outputPath")

    val configuration = new Configuration()
    configuration.set("mapreduce.job.reduces", "5")

    val job = Job.getInstance(configuration, "embeddings generator")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[EmbeddingsMapper])
    job.setReducerClass(classOf[EmbeddingsReducer])

    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    // Delete the output directory if it already exists
    val outputDir = new Path(outputPath)
    val fs = outputDir.getFileSystem(configuration)
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true)
      logger.info(s"Deleted existing output directory: $outputPath")
    }

    val success = job.waitForCompletion(true)
    logger.info(s"Job completed with success: $success")
    success
  }
}

object EmbeddingsRunner {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      logger.error("Incorrect number of arguments provided")
      logger.error("Usage: EmbeddingsRunner <input path> <output path>")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    logger.info(s"Starting EmbeddingsRunner with input path: $inputPath and output path: $outputPath")
    val success = EmbeddingsMR.runJob(inputPath, outputPath)
    if (success) {
      logger.info("Job completed successfully")
    } else {
      logger.error("Job failed")
    }
  }
}