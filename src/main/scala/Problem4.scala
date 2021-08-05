import org.apache.spark.sql.SparkSession

object Problem4 {
  def createView(branch: String, spark: SparkSession): Unit = {
    spark.sql(f"Create view if not exists bev_$branch as select * from " +
      f"(select beverage from part_brancha as a where branch = '$branch' union " +
      f"select beverage from part_branchb as b where branch = '$branch' union " +
      f"select beverage from part_branchc as c where branch = '$branch') abc")
  }

  def showBev(branch: String, spark: SparkSession): Unit = {
    createView(branch, spark)
    val branchDF = spark.sql(f"Select * from bev_$branch")
    println("There are " + branchDF.count() + f" beverages available in $branch:")
    branchDF.show()
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("HiveConn")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("Use project1")
    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
    for (suffix<-"abc") {
      spark.sql(f"Drop table if exists part_branch$suffix")
      spark.sql(f"Create table if not exists part_branch$suffix partitioned by (Branch) as select Beverage, Branch from branch$suffix")
      println(f"Partitions on partitioned table part_branch$suffix")
      spark.sql(f"Show partitions part_branch$suffix").show()
    }

    showBev("Branch1", spark)
    showBev("Branch8", spark)
    showBev("Branch10", spark)
    createView("Branch4", spark)
    createView("Branch7", spark)
    spark.sql("create or replace view commBev as " +
      "select bev_branch4.beverage from bev_branch4, bev_branch7 " +
      "where bev_branch4.beverage = bev_branch7.beverage")
    val commBevDF = spark.sql("Select * from commBev")
    println("There are " + commBevDF.count() + " common beverages in Branch4 and Branch7:")
    commBevDF.show()
  }
}