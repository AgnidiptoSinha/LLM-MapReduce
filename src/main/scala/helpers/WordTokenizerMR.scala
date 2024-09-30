package helpers

import com.knuddels.jtokkit.api.IntArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import helpers.Tokenize
import utils._

import java.lang.Iterable
import collection.JavaConverters._
import java.io.{File, PrintWriter}
import java.nio.file.Files
import scala.io.Source

object WordCount {

  class TokenizerMapper extends Mapper[LongWritable, Text, IntWritable, IntWritable] {
    private val tokenId = new IntWritable()
    private val one = new IntWritable(1)
    private val Tokenizer = new Tokenize()

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, IntWritable]#Context): Unit = {
      println(s"Mapper received: $value")
      val words = value.toString.split("\\s+")

      for (word <- words) {
        val tokenizedString = Tokenizer.getTokens(word)
        tokenizedString.foreach(token => {
          tokenId.set(token)
          context.write(tokenId, one)
          println(s"Mapper emitted: ($tokenId, 1)")  // Debug log
        })
      }
    }
  }

  class IntSumReducer extends Reducer[IntWritable, IntWritable, Text, Text] {
    private val result = new Text()
    private val Tokenizer = new Tokenize()

    override def reduce(key: IntWritable, values: Iterable[IntWritable], context: Reducer[IntWritable, IntWritable, Text, Text]#Context): Unit = {
      var sum = 0
      val tokenId = key.get()

      values.asScala.foreach { value =>
        sum += value.get()
      }

      println(s"Reducer received: ($tokenId, $sum)")

      val tokenArrayList = new IntArrayList()
      tokenArrayList.add(tokenId)
      val word = Tokenizer.deTokenize(tokenArrayList)

      result.set(s"$sum $tokenId")
      context.write(new Text(word), result)
      println(s"Reducer emitted: ($word, $sum $tokenId)")
    }
  }

  def runJob(inputPath: String, outputPath: String): Boolean = {
    println(s"Input path: $inputPath, Output path: $outputPath")
    val configuration = new Configuration()
    configuration.set("mapred.textoutputformat.separator", " ")
    configuration.set("mapreduce.output.textoutputformat.separator", " ")
//    configuration.set("mapreduce.job.reduces", "1")
    val job = Job.getInstance(configuration, "word count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
//    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])
    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    // Delete the output directory if it already exists
    val outputDir = new Path(outputPath)
    val fs = outputDir.getFileSystem(configuration)
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true)
      println(s"Deleted existing output directory: $outputPath")
    }

    job.waitForCompletion(true)
  }
}

// ... (WordCountRunner remains the same)

object WordCountRunner {

  def convertToCsv(): Unit = {

    val src = Source.fromFile("./wordcount_output/part-r-00000")
    val lines = src.getLines().toList
    src.close()
    val processedLines = lines.map {line =>
      val arr = line.split(" ")
      val Tokenizer = new Tokenize()
      val detokenizerInput = new IntArrayList()
      arr.slice(2, arr.length).foreach { token =>
        detokenizerInput.add(token.toInt)
      }
      val word = Tokenizer.deTokenize(detokenizerInput)
      //      println(arr.slice(2, arr.length).mkString("") +","+arr(1)+","+word)
      arr.slice(2, arr.length).mkString("") +","+arr(1)+","+word
    }
    val outputFile = new File("./wordcount_output/output.csv")
    val writer = new PrintWriter(outputFile)
    processedLines.foreach(writer.println)
    writer.close()
    println("Output File generated")
  }

  def main(args: Array[String]): Unit = {
    val inputString = TextFile.getTextFile
    println("Text File read")  // Debug log

    try {
      // Create a temporary input file
      val inputFile = new File("wordcount_input.txt")
      val outputDir = new File("wordcount_output")
      val inputWriter = new PrintWriter(inputFile)
      inputWriter.write(inputString)
      inputWriter.close()
      println(s"Created input file: ${inputFile.getAbsolutePath}")  // Debug log

      // Create a temporary output directory
      //      val outputDir = Files.createTempDirectory("wordcount_output").toFile
      println(s"Created output directory: ${outputDir.getAbsolutePath}")  // Debug log

      // Run the MapReduce job
      val success = WordCount.runJob(inputFile.getAbsolutePath, outputDir.getAbsolutePath)
      if (success) {
        println("Job completed successfully")

        // Print the results
        val resultFile = new File(outputDir, "part-r-00000")
        if (resultFile.exists()) {
          val iterator = scala.io.Source.fromFile(resultFile).getLines()
          iterator.foreach(println)
          println("Result file generated.")
        } else {
          println(s"Result file not found: ${resultFile.getAbsolutePath}")
        }
      } else {
        println("Job failed")
      }

      // Clean up temporary files
      inputFile.delete()
      outputDir.listFiles().foreach(_.delete())
      outputDir.delete()
//      convertToCsv()
    } catch {
      case e: Exception =>
        println(s"An error occurred in main: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}