package core

trait Tuple extends Serializable {
  def flatten: String
}

case class Pattern (content: Array[String]) extends Tuple {
  override def flatten: String = content.mkString(", ")
}
//type Pattern = (String, String, String, String, String, String, String, String, String)

case class DataTuple (id: Long, p: Short, numDataFields: Int) extends Tuple {
  var attributes: Array[String] = new Array[String](numDataFields)
  private var _ancestors: List[Long] = null

  override def flatten: String = {
    (attributes :+ id.toString :+p.toString :+ numDataFields).mkString(", ")
  }

  def ancestors_= (value: List[Long]):Unit = _ancestors = value

  def ancestors = _ancestors
}

object DataTuple {
  def apply(flatten: String): DataTuple = {
    val fields = flatten.split(", ")
    val length = fields.length
    val attributes = fields.slice(0, length - 3)
    val id = fields(length - 3).toLong
    val p = fields(length - 2).toShort
    val numDataFields = fields(length - 1).toInt
    val ret = DataTuple(id, p, numDataFields)
    ret.attributes = attributes
    ret
  }

  def apply(flatten: String, ancestors: List[Long]): DataTuple = {
    val tuple = DataTuple(flatten)
    tuple.ancestors = ancestors
    tuple
  }
}

case class SummaryTuple
(id: Int,
 numDataFields: Int,
 support: Double = 0,
 observation: Double = 0,
 var multiplier: Double = 0,
 gain: Double = 0,
 kl: Double = 1.0 / 0.0) extends Tuple
{
  val pattern: Array[String] = new Array[String](numDataFields)

  override def flatten: String = {
    (pattern :+ id :+ support :+ observation :+ multiplier :+ gain :+ kl).mkString(", ")
  }
}

case class EstimateTuple (dataTuple: DataTuple, q: Float) extends Tuple {
  override def flatten: String = dataTuple.flatten + ", " + q
}

case class RichSummaryTuple
(pattern: Array[String],
 id: Int,
 multiplier: Double,
 p: Double,
 q: Double,
 count: Long,
 diff: Double) extends Tuple {
  override def flatten = (pattern :+ id :+ multiplier :+ p :+ q :+ count :+ diff).mkString(", ")
}

case class LCATuple
(pattern: Array[String],
 count: Long,
 p: Double,
 q: Double) extends Tuple {
  override def flatten = {
    val all = pattern :+ count.toString :+ p :+ q
    all.mkString(",")
  }
}

case class CorrectedTuple
(pattern: Array[String],
 count: Long,
 p: Double,
 q: Double,
 gain: Double,
 multiplier: Double,
 numMatch: Int) extends Tuple {
  override def flatten = {
    val all = pattern :+ count.toString :+ p :+ q :+ gain :+ multiplier
    all.mkString(",")
  }
}

case class EncodedCorrectedTuple
(patternHash: Long,
 count: Long,
 p: Double,
 q: Double,
 gain: Double,
 multiplier: Double,
 numMatch: Int) extends Tuple {
  override def flatten = {
    val all = patternHash.toString :+ count.toString :+ p :+ q :+ gain :+ multiplier
    all.mkString(",")
  }
}
