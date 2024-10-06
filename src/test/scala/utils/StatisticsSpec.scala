import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import utils.Statistics
import breeze.linalg.DenseVector

class StatisticsSpec extends AnyFunSpec with Matchers {
  describe("Statistics") {
    val statistics = new Statistics()

    describe("seqToVector") {
      it("should convert a sequence of floats to a DenseVector of doubles") {
        val input = Seq(1.0f, 2.0f, 3.0f)
        val result = statistics.seqToVector(input)

        result shouldBe a[DenseVector[Double]]
        result.length should be(3)
        result(0) should be(1.0 +- 0.00001)
        result(1) should be(2.0 +- 0.00001)
        result(2) should be(3.0 +- 0.00001)
      }

      it("should handle an empty sequence") {
        val input = Seq.empty[Float]
        val result = statistics.seqToVector(input)

        result shouldBe a[DenseVector[Double]]
        result.length should be(0)
      }
    }

    describe("cosineSimilarity") {
      it("should calculate cosine similarity correctly for orthogonal vectors") {
        val vec1 = DenseVector(1.0, 0.0)
        val vec2 = DenseVector(0.0, 1.0)
        val result = statistics.cosineSimilarity(vec1, vec2)

        result should be(0.0 +- 0.00001)
      }

      it("should calculate cosine similarity correctly for identical vectors") {
        val vec = DenseVector(1.0, 2.0, 3.0)
        val result = statistics.cosineSimilarity(vec, vec)

        result should be(1.0 +- 0.00001)
      }

      it("should calculate cosine similarity correctly for opposite vectors") {
        val vec1 = DenseVector(1.0, 2.0, 3.0)
        val vec2 = DenseVector(-1.0, -2.0, -3.0)
        val result = statistics.cosineSimilarity(vec1, vec2)

        result should be(-1.0 +- 0.00001)
      }
    }

    describe("embeddingSimilarity") {
      it("should calculate embedding similarity correctly") {
        val embedding1 = Seq(1.0f, 0.0f)
        val embedding2 = Seq(0.0f, 1.0f)
        val result = statistics.embeddingSimilarity(embedding1, embedding2)

        result should be(0.0 +- 0.00001)
      }

      it("should return 1.0 for identical embeddings") {
        val embedding = Seq(1.0f, 2.0f, 3.0f)
        val result = statistics.embeddingSimilarity(embedding, embedding)

        result should be(1.0 +- 0.00001)
      }

      it("should handle empty embeddings") {
        val emptyEmbedding = Seq.empty[Float]
        val result = statistics.embeddingSimilarity(emptyEmbedding, emptyEmbedding)

        result.isNaN should be(true)
      }
    }
  }
}