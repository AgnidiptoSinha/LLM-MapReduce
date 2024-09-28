package helpers

import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, IntArrayList, ModelType}

class Tokenize {
  private val registry = Encodings.newDefaultEncodingRegistry()
  val encoding = registry.getEncodingForModel(ModelType.GPT_4)

  def getEncodingAndTokens(input: String): (Encoding, Seq[Int]) = {

    val tokenIds = encoding.encode(input).toArray.toSeq
    (encoding, tokenIds)
  }

  def getTokens(input: String) : String = {

    val tokenIds = encoding.encode(input).toArray.toSeq
    tokenIds.head.toString
  }

  def deTokenize(input: IntArrayList) : String = {

    val word = encoding.decode(input)
    word
  }

}
