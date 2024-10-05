package mapreducers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import utils.{Statistics, TokenEmbeddings, TokenWordings}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.{FileSystem, Path}

object SimilarityMR {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  class SimilarityMapper extends Mapper[Object, Text, IntWritable, Text] {
    private val mapperLogger: Logger = LoggerFactory.getLogger(this.getClass)
    val stats = new Statistics()
    var allTokenEmbeddings: mutable.Map[Int, Seq[Float]] = _
    var fs: FileSystem = _

    override def setup(context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val conf = context.getConfiguration
      fs = FileSystem.get(conf)
      val (_, tokenEmbeddingsMap) = TokenEmbeddings.getTokenEmbeddings(fs, "/merged_embeddings")
      mapperLogger.info("Retrieved merged embeddings")
      allTokenEmbeddings = tokenEmbeddingsMap
    }

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val line = value.toString.trim
      val parts = line.split("\t")
      if (parts.length == 2) {
        val token = parts(0).toInt
        val embeddingsString = parts(1).stripPrefix("[").stripSuffix("]")
        val embeddings = embeddingsString.split(",").map(_.trim.toFloat).toSeq

        for ((token2, embeddings2) <- allTokenEmbeddings if token != token2) {
          val similarity = stats.embeddingSimilarity(embeddings, embeddings2)
          val res = new Text(s"$token2 $similarity")
          context.write(new IntWritable(token), res)
          mapperLogger.trace(s"Mapper emitted: ($token, $res)")
        }
      } else {
        mapperLogger.warn(s"Malformed input: $line")
      }
    }
  }

  class SimilarityReducer extends Reducer[IntWritable, Text, Text, Text] {

    override def reduce(key: IntWritable, values: java.lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, Text]#Context): Unit = {
      val scalaValues = values.asScala
      val topSimilarities = mutable.PriorityQueue.empty[(Int, Float)](Ordering.by[(Int, Float), Float](_._2).reverse)

      for (value <- scalaValues) {
        val parts = value.toString.split(" ")
        val token2 = parts(0).toInt
        val similarity = parts(1).toFloat

        topSimilarities.enqueue((token2, similarity))
        if (topSimilarities.size > 5) {
          topSimilarities.dequeue()
        }
      }

      val top5List = topSimilarities.toList.sortBy(-_._2)
      val (all_tokens, tokenWordings) = TokenWordings.getTokenWordings("/merged_tokens")

      val outputStr = top5List.map { case (token2, sim) =>
        s"${tokenWordings(token2)}:$token2:$sim"
      }.mkString(" ")

      context.write(new Text(s"${tokenWordings(key.get())}:${key.get()}"), new Text(outputStr))
    }
  }

  def runJob(inputPath: String, outputPath: String): Boolean = {
    logger.info(s"Starting job with input path: $inputPath, output path: $outputPath")
    val conf = new Configuration()
    conf.set("mapreduce.job.reduces", "5")

    var fs: FileSystem = null
    try {
      fs = FileSystem.get(conf)

      val outputDir = new Path(outputPath)
      if (fs.exists(outputDir)) {
        fs.delete(outputDir, true)
        logger.info(s"Deleted existing output directory: $outputPath")
      }

      val job = Job.getInstance(conf, "Token Similarity Analysis")

      job.setJarByClass(this.getClass)
      job.setMapperClass(classOf[SimilarityMapper])
      job.setReducerClass(classOf[SimilarityReducer])

      job.setMapOutputKeyClass(classOf[IntWritable])
      job.setMapOutputValueClass(classOf[Text])
      job.setOutputKeyClass(classOf[IntWritable])
      job.setOutputValueClass(classOf[Text])

      FileInputFormat.addInputPath(job, new Path(inputPath))
      FileOutputFormat.setOutputPath(job, outputDir)

      val success = job.waitForCompletion(true)
      logger.info(s"Job completed with success: $success")
      success
    } catch {
      case exception: Exception => {
        logger.error("Job failed with exception", exception)
        false
      }
    } finally {
      if (fs != null) {
        fs.close()
      }
    }
  }
}

object SimilarityRunner {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      logger.error("Incorrect number of arguments provided")
      logger.error("Usage: SimilarityRunner <input path> <output path>")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    logger.info(s"Starting SimilarityRunner with input path: $inputPath and output path: $outputPath")
    val success = SimilarityMR.runJob(inputPath, outputPath)
    if (success) {
      logger.info("Job completed successfully")
    } else {
      logger.error("Job failed")
    }
  }
}