import org.apache.spark.sql.SparkSession

object Problem8 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("HiveConn")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("Use project1")
    spark.sql("Select beverage, count(branch) branches from " +
      "(select * from brancha as a union select * from branchb as b union select * from branchc asc) abc " +
      "group by beverage " +
      "order by branches desc").show()
  }
}
