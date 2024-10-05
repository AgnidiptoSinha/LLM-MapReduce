package utils

import org.slf4j.{Logger, LoggerFactory}
import org.tensorflow.ndarray.Shape
import org.tensorflow.op.Ops
import org.tensorflow.op.core.{Placeholder, Variable}
import org.tensorflow.types.{TFloat32, TInt32}
import org.tensorflow.{Graph, Operand, Session}
import com.typesafe.config.ConfigFactory

import java.util
import scala.util.Random

class LearnEmbeddings {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getEmbeddings(windows: Seq[Seq[Int]], vocabSize: Int): Seq[Seq[Float]] = {
    val config = ConfigFactory.load()
    val embeddingDim = config.getInt("myapp.embeddingDim") //500 // Arbitrary embedding size, adjust as needed
    val initialLearningRate = config.getDouble("myapp.initialLearningRate").toFloat //0.01f
    val decayRate =  config.getDouble("myapp.decayRate").toFloat //0.96f
    val decaySteps = config.getInt("myapp.decaySteps") //1000

    val graph = new Graph()
    val session = new Session(graph)

    try {
      val tf = Ops.create(graph)

      // Initialize random embeddings
      val randomEmbeddings = tf.variable(tf.random.randomUniform(
        tf.constant(Array(vocabSize.toLong, embeddingDim.toLong)),
        classOf[TFloat32]
      ))

      // Create placeholders for input data
      val centerWordsPh = tf.placeholder(classOf[TInt32], Placeholder.shape(Shape.scalar()))
      val contextWordsPh = tf.placeholder(classOf[TInt32], Placeholder.shape(Shape.scalar()))
      val negativeWordsPh = tf.placeholder(classOf[TInt32], Placeholder.shape(Shape.scalar()))
      val learningRatePh = tf.placeholder(classOf[TFloat32], Placeholder.shape(Shape.scalar()))


      // Define model and loss function
      def skipGramModel(centerWord: Operand[TInt32], contextWord: Operand[TInt32]): Operand[TFloat32] = {
        val centerEmbedding = tf.gather(randomEmbeddings, centerWord, tf.constant(0))
        val contextEmbedding = tf.gather(randomEmbeddings, contextWord, tf.constant(0))
        tf.reduceSum(tf.math.mul(centerEmbedding, contextEmbedding), tf.constant(0))
      }

      def negativeSamplingLoss(positivePair: Operand[TFloat32], negativePairs: Operand[TFloat32]): Operand[TFloat32] = {
        val positiveLoss = tf.math.log(tf.math.sigmoid(positivePair))
        val negativeLoss = tf.math.log(tf.math.sigmoid(tf.math.neg(negativePairs)))
        tf.math.neg(tf.math.add(positiveLoss, negativeLoss))
      }

      val positivePair = skipGramModel(centerWordsPh, contextWordsPh)
      val negativePair = skipGramModel(centerWordsPh, negativeWordsPh)
      val loss = negativeSamplingLoss(positivePair, negativePair)

      // Compute gradients
      val gradients = tf.gradients(loss, util.Arrays.asList(randomEmbeddings))

      // Set up training
      val trainOp = tf.train.applyGradientDescent(randomEmbeddings, learningRatePh, gradients.dy(0).asInstanceOf[Operand[TFloat32]])

      // Training loop
      val numEpochs = config.getInt("myapp.epoch")  //500
      val (centerWords, contextWords, negativeWords) = createTrainingData(windows, vocabSize)

      var bestLoss = Float.MaxValue
      var patienceCounter = 0
      val patience = config.getInt("myapp.patience") //10 // Number of epochs to wait for improvement

      for (epoch <- 1 to numEpochs) {

        // Calculate decayed learning rate
        val learningRate = initialLearningRate * Math.pow(decayRate, epoch / decaySteps).toFloat

        // Run the training operation
        session.runner()
          .feed(centerWordsPh.asOutput(), TInt32.scalarOf(centerWords(epoch % centerWords.length)))
          .feed(contextWordsPh.asOutput(), TInt32.scalarOf(contextWords(epoch % contextWords.length)))
          .feed(negativeWordsPh.asOutput(), TInt32.scalarOf(negativeWords(epoch % negativeWords.length)))
          .feed(learningRatePh.asOutput(), TFloat32.scalarOf(learningRate))
          .addTarget(trainOp)
          .run()

        if (epoch % 100 == 0) {
          val lossValue = session.runner()
            .feed(centerWordsPh.asOutput(), TInt32.scalarOf(centerWords(epoch % centerWords.length)))
            .feed(contextWordsPh.asOutput(), TInt32.scalarOf(contextWords(epoch % contextWords.length)))
            .feed(negativeWordsPh.asOutput(), TInt32.scalarOf(negativeWords(epoch % negativeWords.length)))
            .fetch(loss)
            .run()
            .get(0)
            .asInstanceOf[TFloat32]
          val currentLoss = lossValue.getFloat()
          logger.info(f"Epoch $epoch, Loss: $currentLoss%.6f, Learning Rate: $learningRate%.6f")

          // Early stopping check
          if (currentLoss < bestLoss) {
            bestLoss = currentLoss
            patienceCounter = 0
          } else {
            patienceCounter += 1
          }

          if (patienceCounter >= patience) {
            logger.info(s"Early stopping at epoch $epoch")
            extractEmbeddings(session, randomEmbeddings, vocabSize, embeddingDim)
          }
        }
      }

      extractEmbeddings(session, randomEmbeddings, vocabSize, embeddingDim)

    } finally {
      session.close()
      graph.close()
    }
  }

  def createTrainingData(windows: Seq[Seq[Int]], vocabSize: Int): (Seq[Int], Seq[Int], Seq[Int]) = {
    val centerWords = windows.flatMap(window => window.init)
    val contextWords = windows.flatMap(window => window.tail)
    val negativeWords = centerWords.map(_ => Random.nextInt(vocabSize))
    (centerWords, contextWords, negativeWords)
  }

  def extractEmbeddings(session: Session, randomEmbeddings: Variable[TFloat32], vocabSize: Int, embeddingDim: Int): Seq[Seq[Float]] = {
    val learnedEmbeddings = session.runner().fetch(randomEmbeddings).run().get(0).asInstanceOf[TFloat32]

    // Print the shape
    println(learnedEmbeddings.shape())
    logger.info("Vocab Size ", vocabSize)
    logger.info("Dimension ", embeddingDim)
    // Print a few learned embeddings
    logger.info("Learned embeddings for first 3 tokens:")
    for (i <- 0 until math.min(3, vocabSize)) {
      val embedding = for (j <- 0 until math.min(5, embeddingDim)) yield {
        learnedEmbeddings.getFloat(i.toLong, j.toLong)
      }
      logger.info(s"Token $i: ${embedding.mkString("[", ", ", ", ...")}")
    }

    val tokenEmbeddings = for (i <- 0 until vocabSize) yield {
      val embedding = for (j <- 0 until embeddingDim) yield {
        learnedEmbeddings.getFloat(i.toLong, j.toLong)
      }
      embedding.toSeq
    }

    tokenEmbeddings
  }
}
