import org.apache.spark.sql.SparkSession

object Problem6 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("HiveConn")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("Use project1")
    spark.sql("Drop table if exists brancha_copy")
    spark.sql("Create table brancha_copy as select * from brancha")
    spark.sql("Alter table brancha_copy Set TBLPROPERTIES('transactional'='true')")
    val DF = spark.sql("Select * from brancha_copy")
    println("There are " + DF.count() + " records in table brancha_copy")
    println("Deleting records for branch 1 ...")
    spark.sql("Insert overwrite brancha_copy select * from brancha_copy where branch != 'Branch1'")
    println("There are " + DF.count() + " records in table brancha_copy")
  }
}
