package helpers

import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, ModelType}

class Tokenize {

  def getTokens(input: String): (Encoding, Seq[Int]) = {
    val registry = Encodings.newDefaultEncodingRegistry()
    val encoding = registry.getEncodingForModel(ModelType.GPT_4)

    val tokenIds = encoding.encode(input).toArray.toSeq
    (encoding, tokenIds)
  }
}
