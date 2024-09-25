package helpers

class GetWindows {

  def getWindows(tokenIds: Seq[Int], windowSize: Int, stride: Int): (Seq[Seq[Int]], Int) = {
    val windows = tokenIds.sliding(windowSize, stride).toSeq
    val actualVocabSize = tokenIds.max + 1

    (windows, actualVocabSize)
  }
}
