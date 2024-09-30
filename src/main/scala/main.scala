import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import helpers.{GetWindows, Tokenize, LearnEmbeddings}
import utils._

object main {

  def main(args: Array[String]): Unit = {
    // Sample input text (in a real scenario, this would be a large corpus)
    val inputText = TextFile.getTextFile.stripMargin

    // Tokenization and sliding window parameters
    val windowSize = 8
    val stride = 2

    // Step 1: Tokenization with JTokkit and sliding window
    val tokenizer = new Tokenize()
    val (encoding, tokenIds) = tokenizer.getEncodingAndTokens(inputText)

    val windowsGenerator = new GetWindows()
    val (windows, vocabSize) = windowsGenerator.getWindows(tokenIds, windowSize, stride)

    println(s"Total tokens: ${tokenIds.length}")
    println(s"Number of windows: ${windows.length}")

    println(windows)

//     Step 2: Embedding learning with TensorFlow
    val embeddingsGenerator = new LearnEmbeddings()
    val embeddings = embeddingsGenerator.getEmbeddings(windows, vocabSize)

    println(embeddings)


  }

}