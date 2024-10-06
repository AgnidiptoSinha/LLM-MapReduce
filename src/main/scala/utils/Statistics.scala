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

}
