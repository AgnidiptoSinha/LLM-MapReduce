package mapreducers

import com.knuddels.jtokkit.api.IntArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, NLineInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import utils._
import org.slf4j.{Logger, LoggerFactory}

import java.lang.Iterable
import collection.JavaConverters._

object WordFrequencyMR {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  class TokenizerMapper extends Mapper[LongWritable, Text, IntWritable, IntWritable] {
    private val tokenId = new IntWritable()
    private val one = new IntWritable(1)
    private val Tokenizer = new Tokenize()
    private val mapperLogger: Logger = LoggerFactory.getLogger(this.getClass)

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, IntWritable]#Context): Unit = {
      mapperLogger.debug(s"Mapper received: $value")
      val words = value.toString.split("\\s+")

      for (word <- words) {
        val tokenizedString = Tokenizer.getTokens(word)
        tokenizedString.foreach(token => {
          tokenId.set(token)
          context.write(tokenId, one)
          mapperLogger.trace(s"Mapper emitted: ($tokenId, 1)")
        })
      }
    }
  }

  class IntSumReducer extends Reducer[IntWritable, IntWritable, Text, Text] {
    private val result = new Text()
    private val Tokenizer = new Tokenize()
    private val reducerLogger: Logger = LoggerFactory.getLogger(this.getClass)

    override def reduce(key: IntWritable, values: Iterable[IntWritable], context: Reducer[IntWritable, IntWritable, Text, Text]#Context): Unit = {
      var sum = 0 // var used being sum is being calculated. So has to be mutable
      val tokenId = key.get()

      values.asScala.foreach { value =>
        sum += value.get()
      }

      reducerLogger.debug(s"Reducer received: ($tokenId, $sum)")

      val tokenArrayList = new IntArrayList()
      tokenArrayList.add(tokenId)
      val word = Tokenizer.deTokenize(tokenArrayList)

      result.set(s"$sum $tokenId")
      context.write(new Text(word), result)
      reducerLogger.debug(s"Reducer emitted: ($word, $sum $tokenId)")
    }
  }

  def runJob(inputPath: String, outputPath: String): Boolean = {
    logger.info(s"Starting job with input path: $inputPath, output path: $outputPath")
    val configuration = new Configuration()
    configuration.set("mapred.textoutputformat.separator", " ")
    configuration.set("mapreduce.output.textoutputformat.separator", " ")

    val job = Job.getInstance(configuration, "word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setReducerClass(classOf[IntSumReducer])
    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job.setInputFormatClass(classOf[NLineInputFormat])
    NLineInputFormat.setNumLinesPerSplit(job, 10000)

    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

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

object WordFrequencyRunner {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      logger.error("Incorrect number of arguments provided")
      logger.error("Usage: WordFrequencyRunner <input path> <output path>")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    logger.info(s"Starting WordFrequencyRunner with input path: $inputPath and output path: $outputPath")
    val success = WordFrequencyMR.runJob(inputPath, outputPath)
    if (success) {
      logger.info("Job completed successfully")
    } else {
      logger.error("Job failed")
    }
  }
}