import org.apache.spark.sql.SparkSession

object Problem5 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("HiveConn")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("Use project1")
    spark.sql("Alter table counta Set TBLPROPERTIES('comment' = 'No branch information')")
    spark.sql("Alter table counta Set TBLPROPERTIES('notes' = 'No branch information')")
    println("Describe counta:")
    spark.sql("Desc counta").show()
    println("Show table properties:")
    spark.sql("Show TBLPROPERTIES counta").show()
  }
}
