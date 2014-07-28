import scala.collection.mutable.ListBuffer

package object core {
  def printTuples(t: Array[String]) {
    t.foreach(field => print(field + ","))
    println()
  }

  def matchPattern(attributes: Array[String], pattern: Array[String]): Boolean = {
    for (i <- 0 until attributes.length) {
      if (pattern(i) != "*" && !(pattern(i) equals attributes(i)))
        return false
    }
    true
  }

  def calculateDiff(avgP: Float, avgQ: Float, count: Float): Double = {
    (avgP, avgQ) match {
      case (0.0, 1.0) => count
      case (0.0, _) => count * Math.log(1.0 / (1.0 - avgQ)) / Math.log(2)
      case (1.0, 0.0) => count
      case (1.0, _) => count * Math.log(1.0 / avgQ) / Math.log(2)
      case (_, 0.0) => count * avgP * 9 + count * (1 - avgP) * Math.log(1 - avgP) / Math.log(2)
      case (_, 1.0) => count * avgP * Math.log(avgP) / Math.log(2) + count * (1 - avgP) * 9
      case (_, _) => count * avgP * Math.log(avgP / avgQ) / Math.log(2) + count * (1 - avgP) * Math.log((1 - avgP) / (1 - avgQ)) / Math.log(2)
    }
  }

  def generatePattern(left: Array[String], right: Array[String]): Array[String] = {
    (left, right).zipped map (
      (left_a, right_a) => {
        if (left_a equals right_a) left_a else "*"
      }
      )
  }

  def generateAncestors(source: Array[String], idx: Int): ListBuffer[Pattern] = {
    if (idx equals source.length) {
      ListBuffer[Pattern]()
    } else {
      // TODO: Verify the algo here
      val buffer = generateAncestors(source, idx + 1)

      if (buffer.isEmpty)
        buffer += Pattern(source)

      if (source(idx) == "*") {
        buffer
      } else {
        val ancestorBuffer = ListBuffer[Pattern]()
        for (item <- buffer) {
          val p = new Array[String](item.content.length)
          item.content.copyToArray(p)
          p(idx) = "*"
          ancestorBuffer += Pattern(p)
        }
        ancestorBuffer ++ buffer
      }
    }
  }

  def calculateGain(p: Double, q: Double, count: Long, numSampleMatch: Int): Double = {
    assert(numSampleMatch != 0 && count != 0)
    val corrected_diff = if (Math.abs(count - q) < 0.00001) 0.00001 else count - q
    val corrected_q = if (Math.abs(q) < 0.00001) 0.00001 else q
    if (p / count equals q / count) {
      0.0
    } else if (p / count equals 0.0) {
      (count / numSampleMatch) * (1 - p / count) * Math.log((count - p) / corrected_diff) / Math.log(2)
    } else if (p / count equals 1.0) {
      (count / numSampleMatch) * (p / count) * Math.log(p / corrected_q) / Math.log(2)
    } else {
      (count / numSampleMatch) * (1 - p / count) * Math.log((count - p) / corrected_diff) / Math.log(2) +
        (count / numSampleMatch) * (p / count) * Math.log(p / corrected_q) / Math.log(2)
    }
  }

  def calculateMultiplier(p: Double, q: Double): Double = {
    if (p equals q) {
      0.0
    } else if (p == 0.0) {
      -9
    } else {
      Math.log(p / q) / Math.log(2)
    }
  }

  def scaleMultiplier(p: Double, q: Double, multiplier: Double): Double = {
    if (p == q) {
      multiplier
    } else if (p == 0.0) {
      multiplier - 9
    } else if (q == 1.0) {
      multiplier - 9
    } else if (p == 1.0) {
      multiplier + 9
    } else if (q == 1.0) {
      multiplier + 9
    } else {
      multiplier + Math.log(p / q) / Math.log(2) + Math.log((1 - q) / (1 - p)) / Math.log(2)
    }
  }
}
