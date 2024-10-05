import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import utils._
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
    if(args.length != 7) {
      logger.error("Incorrect number of arguments provided")
      logger.error("Arguments : /input /tokenizer_output /merged_tokens /embeddings_input /embeddings_output /merged_embeddings /output")
      System.exit(1)
    }

    val input = args(0)
    val tokenizer_output = args(1)
    val merged_tokens = args(2)
    val embeddings_input = args(3)
    val embeddings_output = args(4)
    val merged_embeddings = args(5)
    val output = args(6)

    logger.info("Initializing Hadoop FileSystem")
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    try {
      logger.info("Ensuring all directories exist")
      Seq(input, tokenizer_output, merged_tokens, embeddings_input, embeddings_output, merged_embeddings, output)
        .foreach(dir => ensureDirectoryExists(fs, dir))

      logger.info("Running Word Tokenizer")
      WordTokenizerRunner.main(Array(input, tokenizer_output))
      logger.info("WordTokenizer completed")

      logger.info("Merging Tokenizer Outputs")
      GeneralHelper.mergeOutputs(tokenizer_output, merged_tokens)
      logger.info("Tokenizer Outputs merged")

      logger.info("Performing Sharding for Embeddings")
      EmbeddingsHelper.main(Array(merged_tokens, embeddings_input))
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