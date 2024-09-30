package helpers

import com.knuddels.jtokkit.api.IntArrayList
import helpers.Tokenize

object Test {
  class TestClass {

    def main(): Unit = {

      val Tokenizer = new Tokenize()
      val tokens = new IntArrayList()
      tokens.add(0)
      println(Tokenizer.deTokenize(tokens))
    }
  }
}
