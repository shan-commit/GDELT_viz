// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder,StandardScaler }
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel

// COMMAND ----------


val customSchema = StructType(Array(StructField("globaleventid", IntegerType, true), StructField("day", StringType, true), StructField("eventcode", IntegerType, true), StructField("eventbasecode", IntegerType, true), StructField("eventrootcode", IntegerType, true), StructField("actiongeo_fullname", StringType, true), StructField("actiongeo_countrycode", StringType, true), StructField("actiongeo_lat", FloatType, true), StructField("actiongeo_long", FloatType, true), StructField("sourceurl", StringType, true), StructField("avgtone", FloatType, true), StructField("mentions_cnt", IntegerType, true), StructField("STATEFP", IntegerType, true), StructField("CD116FP", IntegerType, true), StructField("GEOID", IntegerType, true),StructField("LSAD", StringType, true), StructField("party", StringType, true)))

// COMMAND ----------

//
//  Read data from database file
//
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/202002.csv") // UPDATE this line with your filepath. Refer Databricks Setup Guide Step 3.
   //.load("/FileStore/tables/2020_csv-2a214.gz") // UPDATE this line with your filepath. Refer Databricks Setup Guide Step 3.
   .withColumn("PartyDR", when($"party"=== "Republican", 0).otherwise(1)) 
   //.withColumn("day", from_unixtime(unix_timestamp(col("day"), "MM/dd/yyyy HH:mm")))
   .drop($"sourceurl")
   .drop($"actiongeo_fullname")
   .drop($"day")
   .drop($"party")
   .drop($"CD116FP")
   .drop($"STATEFP")
   .drop($"LSAD")
   .drop($"globaleventid")
   .drop($"actiongeo_lat")
   .drop($"actiongeo_long")
   .drop($"eventbasecode")
   .drop($"eventrootcode")
   .drop($"LSAD")
   .drop($"actiongeo_countrycode")

//df.union(test)



// COMMAND ----------

var df1 = df.na.drop
//df1.show(5)
//
// fileter for null values
//
var filtereddf = df1.filter("eventcode is null")

filtereddf.show(5)


// COMMAND ----------

//
// Define input attributes for the model 
//
//val cols = Array("eventcode","eventrootcode", "actiongeo_lat", "avgtone", "actiongeo_long", "GEOID")

//var cols=newdata.columns
//val cols_ohe = Array("eventcode","eventrootcode", "actiongeo_lat", "avgtone", "actiongeo_long", "GEOID")

//import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder,StandardScaler, OneHotEncoderEstimator, VectorIndexer }
//import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

//val cateListContains = udf((cateList: String, item: String) => if (cateList.contains(item)) 1 else 0)
// val

val dfx = df1.select("eventcode").rdd.map(r => r(0)).collect.toList 

//Remove duplicate
val result = dfx.distinct

//Convert it to Seq
val categories = result.toSeq

//Create the column
categories.foreach {col1 => 
  df1 = df1.withColumn(col1.toString, (when((col("eventcode") === col1.toString.toInt), lit("1").cast(IntegerType)).otherwise(lit("0").cast(IntegerType))))
}

//df1.show(5)

var cols = df1.columns
//var cols_wpdr = df1.columns
//print(cols)
//cols_wpdr = cols_wpdr.filterNot(elm => elm == "PartyDR") //seq("GEOID")
cols = cols.filterNot(elm => elm == "PartyDR") //seq("GEOID")
cols = cols.filterNot(elm => elm == "eventcode") 
cols = cols.filterNot(elm => elm == "GEOID") 

print(cols)

df1.drop($"eventcode")

var df2 = df1
val assembler = new VectorAssembler()
                    .setInputCols(cols)
                    .setOutputCol("features")

// val assembler_wpdr = new VectorAssembler()
//                     .setInputCols(cols_wpdr)
//                     .setOutputCol("features")

var featureDf  = assembler.transform(df2)


// var featureDf_wpdr  = assembler_wpdr.transform(df1)
featureDf.show(5)
// featureDf_wpdr.show(5)



// COMMAND ----------

//
// Define prediction label
//
val indexer = new StringIndexer()
  .setInputCol("PartyDR")
  .setOutputCol("label")

val labelDf = indexer.fit(featureDf).transform(featureDf)



// val labelDf_wpdr = indexer.fit(featureDf_wpdr).transform(featureDf_wpdr)
//labelDf.printSchema()

labelDf.show(1)

//var standardscaler = new StandardScaler().setInputCol("features").setOutputCol("Scaled_features")
//raw_data=standardscaler.fit(raw_data).transform(raw_data)
//raw_data.select("features","Scaled_features").show(5)
//raw_data.columns
//var tt, test = raw_data.randomSplit(Array(0.3, 0.3), seed = 10000)

// COMMAND ----------

//
// Define model and fit the training data
//
val seed = 5043
//
val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

// val Array(trainingData_wpdr, testData_wpdr) = labelDf_wpdr.randomSplit(Array(0.6, 0.4), seed)
// train logistic regression model with training data set
val logisticRegression = new LogisticRegression()
  .setMaxIter(50)
  .setRegParam(0.02)
  //.setElasticNetParam(0.8)
  //.setFamily("multinomial")
  .setFeaturesCol("features")
  .setLabelCol("label")

val model = logisticRegression.fit(trainingData)


//println(s"Multinomial coefficients: ${model.coefficientMatrix}")
//println(s"Multinomial intercepts: ${model.interceptVector}")



// COMMAND ----------

println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

//print(model.coefficients)



// COMMAND ----------


//
// Save model
//
model.write.overwrite()
            .save("/FileStore/tables/model-save")

//
// The applcaition has to laod this model from the follwoing code and do predictions 
//
// load model
//
val lrLoaded = LogisticRegressionModel.load("/FileStore/tables/model-save")

// predict using created model
//val predictionDf = model.transform(testData)

//
// Predict using the mode laoded from file
//
val predictionDf = lrLoaded.transform(testData)
predictionDf.show(5)

// evaluate model with area under ROC
val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setRawPredictionCol("prediction")
  .setMetricName("areaUnderROC")
// print()
// measure the accuracy
val accuracy = evaluator.evaluate(predictionDf)
println(accuracy)
println("############################################################################\n")
//println("Coefficients: ${model.coefficients} Intercept: ${model.intercept}")




// COMMAND ----------

//val columnNames = Seq("PartyDR","prediction","probability","rawPrediction","features","label")
val columnNames = Seq("features")

// using the string column names:
//val result = predictionDf.select(columnNames.head, columnNames.tail: _*)
val result = featureDf.select(columnNames.head, columnNames.tail: _*)

result.show(100, false)

// COMMAND ----------

println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

// COMMAND ----------


