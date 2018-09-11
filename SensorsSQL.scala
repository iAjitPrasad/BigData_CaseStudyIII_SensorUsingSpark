package SQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object SensorsSQL {
  val ManualSchemaHVAC = new StructType(Array(new StructField("Date", StringType, true),
    new StructField("Time", StringType, false),
    new StructField("TargetTemp", LongType, true),
    new StructField("ActualTemp", LongType, false),
    new StructField("System", LongType, false),
    new StructField("SystemAge", LongType, false),
    new StructField("BuildingID", LongType, false)))

  val ManualSchemaBuilding = new StructType(Array(new StructField("BuildingID", LongType, true),
    new StructField("BuildingMgr", StringType, false),
    new StructField("BuildingAge", LongType, true),
    new StructField("HVACproduct", StringType, false),
    new StructField("Country", StringType, false)))

  def main(args: Array[String]): Unit = {
    println("hey Scala, this is Use Case Session 1")

    //Let us create a spark session object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL Use Case 1 ")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //Set the log level as warning
    spark.sparkContext.setLogLevel("WARN")

    println("Spark Session Object Created")

    //Loading the HVAC.csv data into a Temporary Table
    val HVAC = spark.read.format("CSV")
      .option("header", true)
      .schema(ManualSchemaHVAC)
      .load("D:\\AcadGild\\ScalaCaseStudies\\Datasets\\Sensor\\HVAC.csv")
    HVAC.show()
    HVAC.registerTempTable("HVAC_table")
    println("HVAC table registered!\n\n")

    //Add a new column, tempChange - set to 1, if there is a change of greater than +/-5 between actual and target temperature
    val filterHVAC = spark.sql("select *, IF((TargetTemp-ActualTemp)> 5 ,'1', IF((TargetTemp-ActualTemp)< -5 ,'1',0)) as TempChange from HVAC_table")
    filterHVAC.show()
    filterHVAC.registerTempTable("HVACTempChange")
    println("Data Frame Registered as HVACTempChange table!\n\n")

    //Loading the building.csv data into a Temporary Table
    val buildings = spark.read.format("CSV")
      .option("header", true)
      .schema(ManualSchemaBuilding)
      .load("D:\\AcadGild\\ScalaCaseStudies\\Datasets\\Sensor\\building.csv")
    buildings.show()
    buildings.registerTempTable("building_table")
    println("buildings table registered!\n\n")

    //Joining both the tables here
    val joinExpression = filterHVAC.col("BuildingID") ===
      buildings.toDF().col("BuildingID")
    val HVACJOBUILD = filterHVAC.join(buildings,joinExpression)
    HVACJOBUILD.show()
    HVACJOBUILD.registerTempTable("HVACJBUILD")
    println("Tables joined!\n\n")

    //select tempchange and country column, then filter the rows where tempchange is 1 and count the number of occurrence for each country
    val selective = spark.sql("""select TempChange, Country from HVACJBUILD WHERE TempChange = 1""").toDF()
    //Saving the selected fields to variable “selective” and registering the above joined table to temptable “newSelective”
    selective.registerTempTable("newSelective")
    //Count the temperature difference across each country
    spark.sql("""select Country, count(TempChange) from newSelective Group by Country""").show()
    println("Temperature Difference occurrence across each Country counted!\n\n")

  }
}
