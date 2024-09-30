package helpers

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer, Job}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.mutable
import scala.io.Source
import java.io.{File, PrintWriter}
import scala.collection.JavaConverters._
import utils._

object SimilarityMR {

  class SimilarityMapper extends Mapper[Object, Text, IntWritable, Text] {

    val stats = new Statistics()
    var allTokenEmbeddings: mutable.Map[Int, Seq[Float]] = _

    override def setup(context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val (_, tokenEmbeddingsMap) = TokenEmbeddings.getTokenEmbeddings
      allTokenEmbeddings = tokenEmbeddingsMap
    }

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val parts = value.toString.split(":")
      val token = parts(0).toInt
      val embeddings = parts(1).split(",").map(_.toFloat).toSeq

      for ((token2, embeddings2) <- allTokenEmbeddings if token != token2) {
        val similarity = stats.embeddingSimilarity(embeddings, embeddings2)
        val res = new Text(s"$token2 $similarity")
        context.write(new IntWritable(token), res)
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
      val (_, tokenEmbeddingsMap) = TokenEmbeddings.getTokenEmbeddings
      val (all_tokens, tokenWordings) = TokenWordings.getTokenWordings

      val outputStr = top5List.map { case (token2, sim) =>
        s"${tokenWordings(token2)}:$token2:$sim"
      }.mkString(" ")

      context.write(new Text(s"${tokenWordings(key.get())}:${key.get()}"), new Text(outputStr))
    }
  }

  def runJob(inputPath: String, outputPath: String): Boolean = {
    val conf = new Configuration()
    val job = Job.getInstance(conf, "Token Similarity Analysis")

    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[SimilarityMapper])
    job.setReducerClass(classOf[SimilarityReducer])

    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    job.waitForCompletion(true)
  }
}

object SimilarityRunner {
  def main(args: Array[String]): Unit = {
    val inputPath = "./embeddings_input.txt"
    val outputPath = "./similarity_output"

    // Create input file with token:embedding format
    val (allTokens, embeddingsHashMap) = TokenEmbeddings.getTokenEmbeddings
    val inputFile = new File(inputPath)
    val writer = new PrintWriter(inputFile)
    allTokens.foreach { token =>
      val embedding = embeddingsHashMap(token).mkString(",")
      writer.println(s"$token:$embedding")
    }
    writer.close()

    println(s"Created input file with ${allTokens.size} token:embedding pairs: $inputPath")

    // Ensure output directory doesn't exist
    val outputDir = new File(outputPath)
    if (outputDir.exists()) {
      println(s"Deleting existing output directory: $outputPath")
      outputDir.listFiles().foreach(_.delete())
      outputDir.delete()
    }

    // Run the MapReduce job
    println("Starting MapReduce job...")
    val success = SimilarityMR.runJob(inputPath, outputPath)

    if (success) {
      println("MapReduce job completed successfully")

      // Print the results
      val resultFile = new File(outputPath, "part-r-00000")
      if (resultFile.exists()) {
        println("Top 5 similarities for each token:")
        scala.io.Source.fromFile(resultFile).getLines().foreach(println)
      } else {
        println(s"Result file not found: ${resultFile.getAbsolutePath}")
      }
    } else {
      println("MapReduce job failed")
    }

    // Clean up input file (optional)
    // inputFile.delete()
  }
}