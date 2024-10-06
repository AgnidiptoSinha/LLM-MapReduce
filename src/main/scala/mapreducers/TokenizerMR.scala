package mapreducers

import com.knuddels.jtokkit.api.IntArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import utils._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object TokenizerMR {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  class CompositeKey(var lineNumber: Long = 0L) extends WritableComparable[CompositeKey] {
    def this() = this(0L)

    override def write(out: java.io.DataOutput): Unit = {
      out.writeLong(lineNumber)
    }

    override def readFields(in: java.io.DataInput): Unit = {
      lineNumber = in.readLong()
    }

    override def compareTo(other: CompositeKey): Int = {
      lineNumber.compareTo(other.lineNumber)
    }
  }

  class OrderedTokenizeMapper extends Mapper[LongWritable, Text, CompositeKey, Text] {
    private val Tokenizer = new Tokenize()
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, CompositeKey, Text]#Context): Unit = {
      val parts = value.toString.split("\t", 2)
      if (parts.length == 2) {
        val lineNumber = parts(0).toLong
        val line = parts(1)
        val tokens = Tokenizer.getTokens(line)
        val tokenizedText = tokens.mkString(" ")
        context.write(new CompositeKey(lineNumber), new Text(tokenizedText))
      }
    }
  }

  class OrderedPartitioner extends Partitioner[CompositeKey, Text] {
    override def getPartition(key: CompositeKey, value: Text, numPartitions: Int): Int = {
      // Use the line number to determine the partition
      (key.lineNumber % numPartitions).toInt
    }
  }

  class OrderedConcatenationReducer extends Reducer[CompositeKey, Text, LongWritable, Text] {
    override def reduce(key: CompositeKey, values: java.lang.Iterable[Text], context: Reducer[CompositeKey, Text, LongWritable, Text]#Context): Unit = {
      // There should only be one value per key, but we'll concatenate just in case
      val concatenated = values.asScala.map(_.toString).mkString(" ")
      context.write(new LongWritable(key.lineNumber), new Text(concatenated))
    }
  }

  class CompositeKeyComparator extends WritableComparator(classOf[CompositeKey], true) {
    override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
      val key1 = a.asInstanceOf[CompositeKey]
      val key2 = b.asInstanceOf[CompositeKey]
      key1.compareTo(key2)
    }
  }

  def runJob(inputPath: String, outputPath: String): Boolean = {
    logger.info(s"Starting job with input path: $inputPath, output path: $outputPath")
    val configuration = new Configuration()
    configuration.set("mapred.textoutputformat.separator", "\t")
    configuration.set("mapreduce.output.textoutputformat.separator", "\t")
//    configuration.set("mapreduce.job.reduces", "5")  // Set number of reducers to 5

    val job = Job.getInstance(configuration, "ordered tokenization")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[OrderedTokenizeMapper])
    job.setPartitionerClass(classOf[OrderedPartitioner])
    job.setReducerClass(classOf[OrderedConcatenationReducer])
    job.setMapOutputKeyClass(classOf[CompositeKey])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[Text])

    job.setInputFormatClass(classOf[TextInputFormat])
    job.setSortComparatorClass(classOf[CompositeKeyComparator])

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

object OrderedTokenizerRunner {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      logger.error("Incorrect number of arguments provided")
      logger.error("Usage: OrderedTokenizerRunner <input path> <output path>")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    logger.info(s"Starting OrderedTokenizerRunner with input path: $inputPath and output path: $outputPath")
    val success = TokenizerMR.runJob(inputPath, outputPath)
    if (success) {
      logger.info("Job completed successfully")
    } else {
      logger.error("Job failed")
    }
  }
}