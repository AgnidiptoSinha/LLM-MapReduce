package helpers

import com.knuddels.jtokkit.api.IntArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import collection.JavaConverters._
import helpers.{GetWindows, LearnEmbeddings}

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.io.Source
import scala.util.Try

object EmbeddingsMR {

  class EmbeddingsMapper extends Mapper[Object, Text, IntWritable, Text] {

    val windowSize = 8
    val stride = 2
    val vocabSize = 100243

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      println(s"Mapper received: $value")
      val tokens = value.toString.split(" ").map(_.toInt).toSeq


      val WindowGenerator = new GetWindows()
      val (windows, _) = WindowGenerator.getWindows(tokens, windowSize, stride)

      val EmbeddingsGenerator = new LearnEmbeddings()
      val learnedEmbeddings = EmbeddingsGenerator.getEmbeddings(windows, vocabSize)

      tokens.foreach( token => {
        val embeddings = learnedEmbeddings(token)
        val tokenInt = new IntWritable(token)
        val embeddingText = new Text(embeddings.mkString("[", ",", "]"))
        context.write(tokenInt, embeddingText)
      })
    }
  }

  class AverageEmbeddingsReducer extends Reducer[IntWritable, Text, IntWritable, Text] {

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
      }
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
    job.setMapperClass(classOf[EmbeddingsMapper])
    //    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[AverageEmbeddingsReducer])
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
      println(s"Deleted existing output directory: $outputPath")
    }

    job.waitForCompletion(true)
  }
}

object EmbeddingsRunner {

  def convertToCsv(): Unit = {

    val embeddingsOutputFile = Source.fromFile("./embeddings_output/part-r-00000")
    val embeddingLines = embeddingsOutputFile.getLines().toList
    embeddingsOutputFile.close()

    val tokenizeOutputFile = Source.fromFile("./wordcount_output/part-r-00000")
    val tokenLines = tokenizeOutputFile.getLines().toList
    tokenizeOutputFile.close()

    val processedEmbeddingLines = embeddingLines.map {line =>
      val arr = line.split(" ")

      arr(0)+" "+arr(1)
    }

    // Mutable HashMap
    val mutableMap = mutable.HashMap.empty[Long, String]

    tokenLines.foreach {line =>
      val arr = line.split(" ")
//      println(arr(0), arr(2))
      mutableMap.put(arr(2).toLong, arr(0))
    }

    val processedLines = processedEmbeddingLines.map( embeddingLine => {
      val arr = embeddingLine.split(" ")
      val token = arr(0)
      val embeddings = arr(1)
      val word = mutableMap(token.toInt)

      val embeddingsArr = embeddings.split('[')(1).split(']')(0).split(",")
      (word+","+token+","+embeddingsArr.mkString(","))
    })

//    println(processedLines)

    val outputFile = new File("./embeddings_output/output.csv")
    val originalFile = new File("./embeddings_output/part-r-00000")
    val writer = new PrintWriter(outputFile)
    val originalWriter = new PrintWriter(originalFile)
    processedLines.foreach(writer.println)
    processedLines.foreach(originalWriter.println)
    writer.close()
    originalWriter.close()
    println("Output File generated")
  }

  def extractTokens() : Seq[Int] = {
    val filename = "./wordcount_output/part-r-00000"
    var lines: List[String] = List.empty

    val source = Source.fromFile(filename)
    try {
      lines = source.getLines().toList
      val tokens = lines.map( line => {
        line.split(" ").toSeq(2).toInt
      })
      tokens
    } finally {
      source.close()
    }
  }

  def main(args: Array[String]): Unit = {

    val tokens = extractTokens()
    println(tokens)

    try {
      // Create a temporary input file
      val inputFile = new File("embeddings_input.txt")
      val outputDir = new File("embeddings_output")
      val inputWriter = new PrintWriter(inputFile)
      val tokenString = tokens.map(_.toString).mkString(" ")
      inputWriter.write(tokenString)
      inputWriter.close()
      println(s"Created input file: ${inputFile.getAbsolutePath}")  // Debug log

      // Create a temporary output directory
      //      val outputDir = Files.createTempDirectory("wordcount_output").toFile
      println(s"Created output directory: ${outputDir.getAbsolutePath}")  // Debug log

      // Run the MapReduce job
      val success = EmbeddingsMR.runJob(inputFile.getAbsolutePath, outputDir.getAbsolutePath)
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
//      inputFile.delete()
      outputDir.listFiles().foreach(_.delete())
      outputDir.delete()
      convertToCsv()
    } catch {
      case e: Exception =>
        println(s"An error occurred in main: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}