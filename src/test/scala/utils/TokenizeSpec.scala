import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import utils.Tokenize
import com.knuddels.jtokkit.api.IntArrayList

class TokenizeSpec extends AnyFunSpec with Matchers {
  describe("Tokenize") {
    val tokenizer = new Tokenize()

    describe("getEncodingAndTokens") {
      it("should return encoding and tokens for a given input") {
        val input = "Hello, world!"
        val (encoding, tokens) = tokenizer.getEncodingAndTokens(input)

        encoding should not be null
        tokens should not be empty
        tokens.length should be > 0
      }

      it("should return different tokens for different inputs") {
        val input1 = "Hello"
        val input2 = "World"

        val (_, tokens1) = tokenizer.getEncodingAndTokens(input1)
        val (_, tokens2) = tokenizer.getEncodingAndTokens(input2)

        tokens1 should not equal tokens2
      }
    }

    describe("getTokens") {
      it("should return tokens for a given input") {
        val input = "Test input"
        val tokens = tokenizer.getTokens(input)

        tokens should not be empty
        tokens.length should be > 0
      }

      it("should return an empty sequence for an empty input") {
        val input = ""
        val tokens = tokenizer.getTokens(input)

        tokens shouldBe empty
      }
    }

    describe("deTokenize") {
      it("should correctly decode tokens back to a string") {
        val input = "Hello, world!"
        val tokens = tokenizer.getTokens(input)
        val intArrayList = new IntArrayList(tokens.length)
        tokens.foreach(intArrayList.add)

        val decodedString = tokenizer.deTokenize(intArrayList)

        decodedString shouldEqual input
      }

      it("should return an empty string for empty tokens") {
        val emptyIntArrayList = new IntArrayList(0)
        val decodedString = tokenizer.deTokenize(emptyIntArrayList)

        decodedString shouldBe empty
      }
    }
  }
}