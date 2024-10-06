import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import mapreducers._
import helpers._
import org.slf4j.{Logger, LoggerFactory}

object main {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def ensureDirectoryExists(fs: FileSystem, path: String): Unit = {
    val hdfsPath = new Path(path)
    if (!fs.exists(hdfsPath)) {
      fs.mkdirs(hdfsPath)
      logger.info(s"Created directory: $path")
    }
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 9) {
      logger.error("Incorrect number of arguments provided")
      logger.error("Arguments : /input /word_count /tokenizer_input /tokenizer_output /merged_tokens /embeddings_input /embeddings_output /merged_embeddings /output")
      System.exit(1)
    }

    val input = args(0)
    val word_count = args(1)

    val tokenizer_input= args(2)
    val tokenizer_output = args(3)
    val merged_tokens = args(4)

    val embeddings_input = args(5)
    val embeddings_output = args(6)

    val merged_embeddings = args(7)
    val output = args(8)

    logger.info("Initializing Hadoop FileSystem")
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    try {

      logger.info("Ensuring all directories exist")
      Seq(input, tokenizer_output, merged_tokens, embeddings_input, embeddings_output, merged_embeddings, output)
        .foreach(dir => ensureDirectoryExists(fs, dir))

      logger.info("Running Word Frequency")
      WordFrequencyRunner.main(Array(input, word_count))
      logger.info("WordFrequency completed")

      logger.info("Sharding the Text File")
      ShardingText.main(Array(input, tokenizer_input))
      logger.info("Sharding Completed")

      logger.info("Running Tokenizer")
      OrderedTokenizerRunner.main(Array(tokenizer_input, tokenizer_output))
      logger.info("Tokenizer Completed")

      logger.info("Performing Sharding for Embeddings")
      EmbeddingsHelper.main(Array(tokenizer_output, merged_tokens, embeddings_input))
      logger.info("Sharding completed")

      logger.info("Running Token Embeddings")
      EmbeddingsRunner.main(Array(embeddings_input, embeddings_output))
      logger.info("Embeddings Generated")

      logger.info("Merging Embeddings Output")
      GeneralHelper.mergeOutputs(embeddings_output, merged_embeddings)
      logger.info("Token Embeddings Merged")

      logger.info("Running Similarity Analysis")
      SimilarityRunner.main(Array(embeddings_output, output))
      logger.info("Similarities Generated")

    } catch {
      case e: Exception =>
        logger.error("An error occurred during execution", e)
        System.exit(1)
    } finally {
      logger.info("Closing FileSystem")
      fs.close()
    }
  }
}