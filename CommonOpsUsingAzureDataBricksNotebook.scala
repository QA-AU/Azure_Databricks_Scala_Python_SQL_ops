// Databricks notebook source
// MAGIC %md
// MAGIC # Common Scala Data Operations using data frames
// MAGIC

// COMMAND ----------

// establishing a connection using sas token in order to mount a storage container where emplloyee and department csv are loaded
val containerName = "filelanding"
val storageAccountName = "dbstrgeacdb1"
val sas = "[Redracted]"
val url = "wasbs://" +containerName+"@"+storageAccountName + ".blob.core.windows.net/"
val config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net" 

// COMMAND ----------

// mouting the container with folder
dbutils.fs.mount(
  source = url,
  mountPoint = "/mnt/filelanding",
  extraConfigs = Map(config ->sas))

// COMMAND ----------

// unmounting if required
// dbutils.fs.unmount("/mnt/filelanding")

// COMMAND ----------



// COMMAND ----------

// declaring the column attributes of data frame

import org.apache.spark.sql.types._


val customSchema = StructType(
List(
StructField("Employee_id", IntegerType, true),
StructField("First_Name", StringType, true),
StructField("Last_Name", StringType, true),
StructField("Gender", StringType, true),
StructField("Salary", IntegerType, true),
StructField("Date_of_Birth", StringType, true),
StructField("Age", IntegerType, true),
StructField("Country", StringType, true),
StructField("Department_id", IntegerType, true),
StructField("Date_of_Joining", StringType, true),
StructField("Manager_id", IntegerType, true),
StructField("Currency", StringType, true),
StructField("End_Date", StringType, true)
)
)

// COMMAND ----------

// verifying if the container is mounted
 %fs ls "/mnt/filelanding"

// COMMAND ----------

// mounting the container where a Employee csv file is present
val df = spark.read
.option("header","true")
.schema(customSchema)
.csv("/mnt/filelanding/Employee.csv")

// COMMAND ----------

 

// COMMAND ----------

// show data frame
df.show

// COMMAND ----------

// simply display data frame
display(df)

// COMMAND ----------

// Filter example 1 with tripple =
df.filter($"Department_id" === 1).show()

// COMMAND ----------

//filter example filter with double equal
df.filter("Department_id != 1").show()

// COMMAND ----------

// simple filter
df.filter($"Department_id" !== 1).show()

// COMMAND ----------

// using between filter condition
df.filter($"Department_id" > 1 and $"Department_id" < 10).show()

// COMMAND ----------

// where clause 
df.where($"Department_id" >1)
.where($"Department_id" <10).show

// COMMAND ----------

// select * statement
import  org.apache.spark.sql.functions._
df.select(col("*")).show()

// COMMAND ----------

// selecting specifci columns
df.select("Employee_id","First_name","last_name","Gender").show()

// COMMAND ----------

// Adding a new temp column and renaming a column
df.withColumn("New Column",df.col("gender"))
.withColumnRenamed("New Column","Gender_new")
.show()

// COMMAND ----------

display(df)

// COMMAND ----------

// casting a string to an integer
val dfConv = df
.withColumn("Department_id", df("Department_id").cast(IntegerType))
.show()

// COMMAND ----------

// dropping a column from data frame
df
.drop("Currency")
.show()

// COMMAND ----------

// printing schema of a data frame
df.printSchema

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from employee_data_qa

// COMMAND ----------

//  distinct
df.select(countDistinct("department_id")).show()

// COMMAND ----------

// load sql result in  df
val dfEmployee = spark.sql("Select * from employee_data_qa")
dfEmployee.describe()

// COMMAND ----------

// using sql result for further  
dfEmployee.select("Employee_id", "Salary")
.show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from department

// COMMAND ----------

// loading another table in a dataframe
val dfDepartment = spark.read.table("department")
.show()

// COMMAND ----------

 // load sql result into dfDepartment
 val dfDepartment = spark.sql("Select * from department")

// COMMAND ----------

dfDepartment.describe()

// COMMAND ----------

dfEmployee.describe()

// COMMAND ----------

// MAGIC %scala
// MAGIC // LEFT JOIN
// MAGIC import org.apache.spark.sql.functions._
// MAGIC dfEmployee.join(dfDepartment, dfEmployee.col("Department_id") === dfDepartment("Department_id"), "left")
// MAGIC .select(dfEmployee.col("Employee_id"),dfEmployee.col("Department_id"), dfEmployee.col("First_Name")).show()
// MAGIC
// MAGIC

// COMMAND ----------

// creating temp tables
dfEmployee.createOrReplaceTempView("EMP")

// COMMAND ----------

// MAGIC %md 
// MAGIC # Common commands in Python
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC dfEmployee = spark.read.format("csv") \
// MAGIC .options(header='true', delimiter = ',') \
// MAGIC .load("/mnt/filelanding/Employee.csv")   

// COMMAND ----------

// MAGIC %python
// MAGIC dfDep = spark.read.format("csv") \
// MAGIC .options(header='true', delimiter = ',') \
// MAGIC .load("/mnt/filelanding/Department.csv")   

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC dfEmployee.join(dfDep,dfEmployee.Department_id == dfDep.Department_id,"left")\
// MAGIC .select(col("Employee_id"),dfEmployee.Department_id,dfDep.Department_id,dfEmployee.First_Name).show()

// COMMAND ----------

// MAGIC %python
// MAGIC dfEmployee.createOrReplaceTempView("EMP")
// MAGIC dfDep.createOrReplaceTempView("DEP")

// COMMAND ----------

// MAGIC %python
// MAGIC query1 = spark.sql("Select * from Emp e Left JOIN Dep d on e.department_id = d.department_id")
// MAGIC query1.show()

// COMMAND ----------

// MAGIC %python
// MAGIC  dfEmployee\
// MAGIC   .where(col("Country")=="Australia") \
// MAGIC   .select(max("Salary")) \
// MAGIC   .show()

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.window import Window
// MAGIC from pyspark.sql.functions import *
// MAGIC
// MAGIC windowFuncCoun1 = Window.partitionBy("Country").orderBy(desc("Salary"))
// MAGIC windowFuncCounMin = Window.partitionBy("Country").orderBy(asc("Salary"))
// MAGIC
// MAGIC dfEmployee \
// MAGIC     .withColumn("MaxSalaryPerCountry", max("Salary").over(windowFuncCoun1)) \
// MAGIC     .withColumn("MinSalaryPerCountry", max("Salary").over(windowFuncCounMin)) \
// MAGIC     .withColumn("RowNumber", row_number().over(windowFuncCoun1))\
// MAGIC     .show()

// COMMAND ----------

// MAGIC %python 
// MAGIC from pyspark.sql.window import Window
// MAGIC from pyspark.sql.functions import *
// MAGIC
// MAGIC windowFuncCoun1 = Window.partitionBy("Country").orderBy(desc("Salary"))
// MAGIC windowFuncCounMin = Window.partitionBy("Country").orderBy(asc("Salary"))
// MAGIC
// MAGIC dfEmployee \
// MAGIC     .withColumn("MaxSalaryPerCountry", max("Salary").over(windowFuncCoun1)) \
// MAGIC     .withColumn("MinSalaryPerCountry", max("Salary").over(windowFuncCounMin)) \
// MAGIC     .withColumn("RowNumber", row_number().over(windowFuncCoun1))\
// MAGIC     .where(col("RowNumber")==3)   \
// MAGIC     .show()   

// COMMAND ----------

// MAGIC %md
// MAGIC # Similar functions in SQL Notebook
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from employee_data_qa
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from 
// MAGIC (select 
// MAGIC Department_id, First_Name, Last_Name,Employee_id, Salary,
// MAGIC row_number() over(partition by Department_id Order by salary Desc) rw
// MAGIC from employee_data_qa ) d where d.rw = 1

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from EMP

// COMMAND ----------


