// DATAFRAME PROJECT
//import Spark librarie
// Use the Netflix_2011_2016.csv file to Answer and complete
// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
var spark = SparkSession.builder().getOrCreate()
// Load the Netflix Stock CSV File, have Spark infer the data types.
val df  = spark.read.option("header","true").option("infershema","true").csv("Netflix_2011_2016.csv")
// What are the column names?
df.columns

// What does the Schema look like?
df.printSchema()
// Print out the first 5 columns.
df.show(5)
// Use describe() to learn about the DataFrame.
df.describe().show()
// Create a new dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.
val df2 = df.withColumn("HV Ratio",df("High")/df("Volume"))

// What day had the Peak High in Price?
//val MaxHigh = df.groupBy("Date").max()
//MaxHigh.select($"Date",$"max(High)").show()
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
val df_format = df.withColumn("High", col("High").cast("float")).withColumn("Open", col("Open").cast("float")).withColumn("Low", col("Low").cast("float")).withColumn("Close", col("Close").cast("float")).withColumn("Volume", col("Volume").cast("float")).withColumn("Date", col("Date").cast("timestamp"))
//df3.printSchema()
df_format.orderBy($"High".desc).show(1)

//df.select(max("High")).show()
// What is the mean of the Close column?
//df.select(mean("Close")).show()
// What is the max and min of the Volume column?
df_format.select(min("Volume"),max("Volume"),mean("Close") ).show()
// For Scala/Spark $ Syntax
import spark.implicits._
// How many days was the Close lower than $ 600?
df_format.filter($"Close"<600).count()
// What percentage of the time was the High greater than $500 ?
(df_format.filter($"High">500).count()*1.0 / df_format.count())*100.0
// What is the Pearson correlation between High and Volume?
df_format.select(corr("High","Volume")).show()
// What is the max High per year?
val df_year = df_format.withColumn("Year",year(df_format("Date")))
val df_high = df_year.groupBy("Year").max()
df_high.select("Year","max(High)").show()
// What is the average Close for each Calender Month?
val df_month = df_format.withColumn("Month",month(df_format("Date")))
val df_min = df_month.groupBy("Month").mean()
df_min.select("Month","avg(Close)").orderBy("Month").show()
