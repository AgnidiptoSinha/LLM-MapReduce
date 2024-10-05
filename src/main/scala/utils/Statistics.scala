package utils

import breeze.linalg.{DenseVector, norm}

class Statistics {

  def seqToVector(seq: Seq[Float]): DenseVector[Double] = {
    DenseVector(seq.map(_.toDouble).toArray)
  }

  def cosineSimilarity(vec1: DenseVector[Double], vec2: DenseVector[Double]): Double = {
    val dotProduct = vec1 dot vec2
    val magnitudeProduct = norm(vec1) * norm(vec2)
    dotProduct / magnitudeProduct
  }

  def embeddingSimilarity(embedding1: Seq[Float], embedding2: Seq[Float]): Double = {
    val vec1 = seqToVector(embedding1)
    val vec2 = seqToVector(embedding2)
    cosineSimilarity(vec1, vec2)
  }

  def embeddingAnalogy(embedding1: Seq[Float], embedding2: Seq[Float], embedding3: Seq[Float]): DenseVector[Double] = {
    val vec1 = seqToVector(embedding1)
    val vec2 = seqToVector(embedding2)
    val vec3 = seqToVector(embedding3)

    // Compute the analogy vector
    vec2 - vec1 + vec3
  }

  // Helper function to find the most similar embedding
  def findMostSimilarEmbedding(target: DenseVector[Double], candidates: Seq[Seq[Float]]): Seq[Float] = {
    candidates.maxBy(candidate => cosineSimilarity(target, seqToVector(candidate)))
  }

}
