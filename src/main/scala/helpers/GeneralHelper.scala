package helpers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedWriter, OutputStreamWriter}

object GeneralHelper {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def mergeOutputs(inputDir: String, outputDir: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val hdfsIn = new Path(inputDir)

    if (!fs.exists(hdfsIn)) {
      logger.info(s"Directory $inputDir does not exist")
      return
    }

    val files = fs.listStatus(hdfsIn)
      .filter(_.getPath.getName.startsWith("part-r-"))
      .sortBy(_.getPath.getName)

    if (files.isEmpty) {
      logger.info(s"No files found in $inputDir")
      return
    }

    val hdfsOut = new Path(outputDir)

    val mergedFile = new Path(hdfsOut, "merged_output.txt")
    val outputStream = fs.create(mergedFile)
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream))

    try {
      files.foreach { fileStatus =>
        val filePath = fileStatus.getPath
        val inputStream = fs.open(filePath)
        val source = scala.io.Source.fromInputStream(inputStream)
        try {
          source.getLines().foreach(line => {
            writer.write(line)
            writer.newLine()
          })
        } finally {
          source.close()
          inputStream.close()
        }
      }
    } finally {
      writer.close()
      outputStream.close()
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      logger.error("Usage: EmbeddingsRunner <input path> <output path>")
      System.exit(1)
    }

    val tokenizer_output = args(0)
    val embeddings_output = args(1)

//    mergeOutputs(tokenizer_output)
//    mergeOutputs(embeddings_output)
  }
}