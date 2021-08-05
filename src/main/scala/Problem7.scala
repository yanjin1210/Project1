import org.apache.spark.sql.SparkSession

object Problem7 {
  def getUniqueBranchs(spark: SparkSession): Array[String] = {
    val branchesDF = spark.sql("select * from (select distinct branch from brancha as a union select distinct branch from branchb as b union select distinct branch from branchc as c)").toDF()
    branchesDF.select("branch").rdd.map(r => r(0).toString).collect()
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("HiveConn")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("Use project1")
    spark.sql("Drop table if exists branchbev")
    spark.sql("Create table branchbev (Branch string, beverage int)")
    for (branch<-getUniqueBranchs(spark)) {
      spark.sql(f"Insert into branchbev select '$branch', count(distinct beverage) from $branch")
    }
    spark.sql("Select * from branchbev order by beverage").show()
  }
}
