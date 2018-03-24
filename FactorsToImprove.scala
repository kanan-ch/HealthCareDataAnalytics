package healthcare.diabetes

import org.apache.log4j._
import org.apache.spark.SparkContext

object FactorsToImprove {
  val RR_INSULIN_DOSE = 33
  val NPH_INSULIN_DOSE = 34
  val ULTRALENTE_INSULIN_DOSE = 35

  val PRE_BREAKFAST_BG_LEVEL = 58
  val POST_BREAKFAST_BG_LEVEL = 59
  val PRE_LUNCH_BG_LEVEL = 60
  val POST_LUNCH_BG_LEVEL = 61
  val PRE_SUPPER_BG_LEVEL = 62
  val POST_SUPPER_BG_LEVEL = 63
  val PRE_SNACK_BG_LEVEL = 64
  val INVALID = -1

  //parsing input file into rdd comprising of user id and Data code(Insulin Level or blood Glucose level) and code's value pair
  def parseLine(line: String) = {
    val fields = line.split("\t");
   // val date = fields(0)
    val code = fields(2).toInt;
    //try { Some(s.toDouble) } catch { case _ => None }
    val value = try{if (fields(3).length() > 0) fields(3).toFloat else 0} catch {case _ => -1}
    val id = fields(4).toInt
    (id, (code, value))
  }

  type UserCodeValuePair = (Int, (Int, Float))
  //gives insulin code and Value & blood glucose code and blood glucose level pairs of a user
  def getInsulin_BG_values(userCodeValuePair: UserCodeValuePair) = {
    val insulin_code = getInsulinCode(userCodeValuePair._2._1)
    val insulin_Value = if (insulin_code == -1) -1 else userCodeValuePair._2._2
    val bg_code = getBGLevelCode(userCodeValuePair._2._1)
    val bg_level = if (bg_code == -1) -1 else userCodeValuePair._2._2
    (userCodeValuePair._1, ((insulin_code, insulin_Value), (bg_code, bg_level)))
  }

  //gives Insulin code or Invalid
  def getInsulinCode(choice: Int): Int = choice match {
    case RR_INSULIN_DOSE         => RR_INSULIN_DOSE
    case NPH_INSULIN_DOSE        => NPH_INSULIN_DOSE
    case ULTRALENTE_INSULIN_DOSE => ULTRALENTE_INSULIN_DOSE
    case _                       => INVALID
  }

  //gives blood glucose code or Invalid
  def getBGLevelCode(choice: Int): Int = choice match {
    case PRE_BREAKFAST_BG_LEVEL  => PRE_BREAKFAST_BG_LEVEL
    case POST_BREAKFAST_BG_LEVEL => POST_BREAKFAST_BG_LEVEL
    case PRE_LUNCH_BG_LEVEL      => PRE_LUNCH_BG_LEVEL
    case POST_LUNCH_BG_LEVEL     => POST_LUNCH_BG_LEVEL
    case PRE_SUPPER_BG_LEVEL     => PRE_SUPPER_BG_LEVEL
    case POST_SUPPER_BG_LEVEL    => POST_SUPPER_BG_LEVEL
    case PRE_SNACK_BG_LEVEL      => PRE_SNACK_BG_LEVEL
    case _                       => INVALID
  }
  type UserInsulinBGValuePair = (Int, ((Int, Float), (Int, Float)))
  //filter invalid vcode-value pairs
  def filterCode(userInsulinBGValuePair: UserInsulinBGValuePair): Boolean = {
    val insulin_code = userInsulinBGValuePair._2._1._1
    val bg_code = userInsulinBGValuePair._2._2._1
    return insulin_code != INVALID || bg_code != INVALID
  }
  type InsulinBGValuePair = ((Int, Float), (Int, Float))
  //give max Blood glucose value and insulen level of a User
  def getMaxInsulinBGValue(insulinBGValuePair1: InsulinBGValuePair, insulinBGValuePair2: InsulinBGValuePair) = {
    val insulinCode1 = insulinBGValuePair1._1._1
    val insulinValue1 = insulinBGValuePair1._1._2

    val bgCode1 = insulinBGValuePair1._2._1
    val bgValue1 = insulinBGValuePair1._2._2

    val insulinCode2 = insulinBGValuePair2._1._1
    val insulinValue2 = insulinBGValuePair2._1._2

    val bgCode2 = insulinBGValuePair2._2._1
    val bgValue2 = insulinBGValuePair2._2._2

    val insulinCode = if (insulinValue1 > insulinValue2) insulinCode1 else insulinCode2
    val insulinValue = if (insulinValue1 > insulinValue2) insulinValue1 else insulinValue2

    val bgCode = if (bgValue1 > bgValue2) bgCode1 else bgCode2
    val bgValue = if (bgValue1 > bgValue2) bgValue1 else bgValue2
 
    ((insulinCode, insulinValue), (bgCode, bgValue))
  }
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "DiabetesFactorToImprove")
//    val lines = sc.textFile("../diabetes-data.csv")
    val lines = sc.textFile(args(0))
    val rdd = lines.map(parseLine)
    val insulin_BGRDD = rdd.map(getInsulin_BG_values)
    val insulin_BG_RDD = insulin_BGRDD.filter(filterCode)
    val max_insulin_BG_RDD = insulin_BG_RDD.reduceByKey(getMaxInsulinBGValue).sortByKey(true, 1).map(x=> (x._1, x._2._1._1, x._2._1._2, x._2._2._1, x._2._2._2))
//    max_insulin_BG_RDD.saveAsTextFile("../diabetes/factors-to-improve.csv")
     max_insulin_BG_RDD.saveAsTextFile(args(1))
  }
}
