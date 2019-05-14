# Working-With-CERN-Root-files
The idea with this project is to learn the basis about how to use spark with generated ROOT files from LHC experiment.

These are notes about how to work with ROOT files collected from a CERN high energy physics project.

To read:

https://es.wikipedia.org/wiki/Muon

aironman@MacBook-Pro-Retina-de-Alonso ~> spark-shell --packages org.diana-hep:root4j:0.1.6 --jars /Users/aironman/Downloads/spark-root_2.11-0.1.17.jar
Ivy Default Cache set to: /Users/aironman/.ivy2/cache
The jars for the packages stored in: /Users/aironman/.ivy2/jars
:: loading settings :: url = jar:file:/usr/local/Cellar/apache-spark/2.4.1/libexec/jars/ivy-2.4.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
org.diana-hep#root4j added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-5af6fba9-0ea8-4b99-adc1-ca8fdc11e3d1;1.0
  confs: [default]
  found org.diana-hep#root4j;0.1.6 in central
  found org.apache.bcel#bcel;5.2 in central
  found jakarta-regexp#jakarta-regexp;1.4 in spark-list
  found org.tukaani#xz;1.2 in local-m2-cache
:: resolution report :: resolve 233ms :: artifacts dl 5ms
  :: modules in use:
  jakarta-regexp#jakarta-regexp;1.4 from spark-list in [default]
  org.apache.bcel#bcel;5.2 from central in [default]
  org.diana-hep#root4j;0.1.6 from central in [default]
  org.tukaani#xz;1.2 from local-m2-cache in [default]
  ---------------------------------------------------------------------
  |                  |            modules            ||   artifacts   |
  |       conf       | number| search|dwnlded|evicted|| number|dwnlded|
  ---------------------------------------------------------------------
  |      default     |   4   |   0   |   0   |   0   ||   4   |   0   |
  ---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-5af6fba9-0ea8-4b99-adc1-ca8fdc11e3d1
  confs: [default]
  0 artifacts copied, 4 already retrieved (0kB/7ms)
19/05/07 14:56:07 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark context Web UI available at http://192.168.1.36:4040
Spark context available as 'sc' (master = local[*], app id = local-1557233773635).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.1
      /_/
         
Using Scala version 2.11.12 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_172)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.dianahep.sparkroot.experimental._
import org.dianahep.sparkroot.experimental._

scala> val dfGamma = spark.read.root("/Users/aironman/Downloads/complete_set_of_ATLAS_open_data_samples_July_2016/Data/DataEgamma.root")
dfGamma: org.apache.spark.sql.DataFrame = [runNumber: int, eventNumber: int ... 44 more fields]

scala> val dfMuons = spark.read.root("/Users/aironman/Downloads/complete_set_of_ATLAS_open_data_samples_July_2016/Data/DataMuons.root")
dfMuons: org.apache.spark.sql.DataFrame = [runNumber: int, eventNumber: int ... 44 more fields]

scala> dfMuons.show(10,false)
19/05/07 15:03:25 WARN Utils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.debug.maxToStringFields' in SparkEnv.conf.
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+-------------+------------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+---------+----------+-----+--------+-----------+-----------+-----------+-----------+----------+-----------+------------+----------------+-------+-------------+
|runNumber|eventNumber|channelNumber|mcWeight|pvxp_n|vxp_z     |scaleFactor_PILEUP|scaleFactor_ELE|scaleFactor_MUON|scaleFactor_BTAG|scaleFactor_TRIGGER|scaleFactor_JVFSF|scaleFactor_ZVERTEX|trigE|trigM|passGRL|hasGoodVertex|lep_n|lep_truthMatched|lep_trigMatched|lep_pt     |lep_eta      |lep_phi     |lep_E      |lep_z0        |lep_charge|lep_type|lep_flag   |lep_ptcone30|lep_etcone20|lep_trackd0pvunbiased|lep_tracksigd0pvunbiased|met_et   |met_phi   |jet_n|alljet_n|jet_pt     |jet_eta    |jet_phi    |jet_E      |jet_m     |jet_jvf    |jet_trueflav|jet_truthMatched|jet_SV0|jet_MV1      |
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+-------------+------------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+---------+----------+-----+--------+-----------+-----------+-----------+-----------+----------+-----------+------------+----------------+-------+-------------+
|207490   |17281852   |207490       |0.0     |15    |-12.316585|0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[3]            |[40531.855]|[0.288244]   |[1.3469992] |[42227.465]|[-0.045446984]|[-1.0]    |[13]    |[568344575]|[0.0]       |[94.18325]  |[-0.04912882]        |[0.0152232405]          |94215.1  |-1.3943559|0    |0       |[]         |[]         |[]         |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
|207490   |17282007   |207490       |0.0     |15    |22.651913 |0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[3]            |[37172.49] |[-0.21478392]|[-2.1380057]|[38033.36] |[0.22578493]  |[1.0]     |[13]    |[568344575]|[0.0]       |[65.88673]  |[-0.032381147]       |[0.025347514]           |30354.057|0.3259549 |1    |1       |[27929.969]|[-2.170531]|[1.42328]  |[124052.25]|[4573.633]|[0.9523457]|[-99]       |[0]             |[0.0]  |[0.055551887]|
|207490   |17282941   |207490       |0.0     |14    |67.00033  |0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[3]            |[41404.363]|[-1.2001014] |[1.3576007] |[74975.45] |[-0.030548852]|[1.0]     |[13]    |[568344575]|[0.0]       |[628.7983]  |[-0.0059319637]      |[0.018147442]           |54632.633|-2.053428 |0    |0       |[]         |[]         |[]         |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
|207490   |17283582   |207490       |0.0     |14    |25.114586 |0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[3]            |[36330.36] |[1.6244663]  |[-0.0825191]|[95780.08] |[0.016617686] |[1.0]     |[13]    |[568344575]|[0.0]       |[78.632385] |[-0.012363502]       |[0.017884906]           |18974.707|2.3157902 |0    |0       |[]         |[]         |[]         |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
|207490   |17284798   |207490       |0.0     |13    |5.419942  |0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[1]            |[29865.918]|[1.9333806]  |[-2.166267] |[105389.39]|[-0.008121626]|[1.0]     |[13]    |[568344575]|[0.0]       |[228.17558] |[-0.0022841152]      |[0.017784366]           |18013.09 |0.86960316|0    |0       |[]         |[]         |[]         |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
|207490   |17285935   |207490       |0.0     |5     |-29.495798|0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[3]            |[39808.71] |[-2.2543075] |[-0.8124007]|[191751.39]|[0.032703664] |[-1.0]    |[13]    |[568344575]|[0.0]       |[-95.009285]|[-0.027823856]       |[0.016839173]           |36319.457|2.1266718 |0    |0       |[]         |[]         |[]         |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
|207490   |17285705   |207490       |0.0     |16    |-41.16512 |0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[1]            |[25943.613]|[-0.6525827] |[2.1666195] |[31666.87] |[0.0037387605]|[-1.0]    |[13]    |[551557119]|[2617.5457] |[2193.5383] |[-0.005797062]       |[0.014282377]           |16567.258|0.5757681 |1    |1       |[34763.277]|[1.9791657]|[-1.325527]|[128388.18]|[7173.404]|[0.8387679]|[-99]       |[0]             |[0.0]  |[0.05587074] |
|207490   |17286780   |207490       |0.0     |11    |-34.865147|0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[3]            |[38542.887]|[-0.6452386] |[1.7496207] |[46848.598]|[0.010180376] |[-1.0]    |[13]    |[568344575]|[0.0]       |[719.70996] |[-0.007343145]       |[0.01543821]            |46948.73 |-1.0433749|0    |0       |[]         |[]         |[]         |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
|207490   |17288279   |207490       |0.0     |13    |-34.4249  |0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[1]            |[32593.084]|[2.394099]   |[-2.8295686]|[180069.89]|[-0.04895938] |[1.0]     |[13]    |[568344575]|[0.0]       |[408.78912] |[0.03346716]         |[0.02250342]            |26812.076|0.6027184 |0    |0       |[]         |[]         |[]         |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
|207490   |17286783   |207490       |0.0     |9     |-43.56932 |0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[1]            |[32656.75] |[-2.0470903] |[0.6496125] |[128576.88]|[0.06338643]  |[1.0]     |[13]    |[568344575]|[0.0]       |[-1059.2249]|[-0.0042003784]      |[0.01684502]            |56296.965|-2.1432886|0    |0       |[]         |[]         |[]         |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+-------------+------------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+---------+----------+-----+--------+-----------+-----------+-----------+-----------+----------+-----------+------------+----------------+-------+-------------+
only showing top 10 rows

# Creating parquet files...
scala> val muonsParquetFile = dfMuons.write.parquet("dfMuons.parquet")
muonsParquetFile: Unit = ()

# a little error creating gamma parquet file...
scala> val gammaParquetFile = dfGamma.write.parquet("dfGammam.parquet")
gammaParquetFile: Unit = ()                                                     

# Reading created parquet files...
scala> val muonsParquetFile = spark.read.parquet("dfMuons.parquet")
muonsParquetFile: org.apache.spark.sql.DataFrame = [runNumber: int, eventNumber: int ... 44 more fields]

scala> val gammaParquetFile = spark.read.parquet("dfGammam.parquet")
gammaParquetFile: org.apache.spark.sql.DataFrame = [runNumber: int, eventNumber: int ... 44 more fields]

# Caching Dataframes...
scala> dfMuons.cache()
res4: dfMuons.type = [runNumber: int, eventNumber: int ... 44 more fields]

scala> dfGamma.cache()
res5: dfGamma.type = [runNumber: int, eventNumber: int ... 44 more fields]

scala> dfMuons.count
res2: Long = 7028084                                                            

scala> dfGamma.count
res3: Long = 7917590                                                            

scala> import spark.implicits._
import spark.implicits._

scala> dfMuons.printSchema
root
 |-- runNumber: integer (nullable = true)
 |-- eventNumber: integer (nullable = true)
 |-- channelNumber: integer (nullable = true)
 |-- mcWeight: float (nullable = true)
 |-- pvxp_n: integer (nullable = true)
 |-- vxp_z: float (nullable = true)
 |-- scaleFactor_PILEUP: float (nullable = true)
 |-- scaleFactor_ELE: float (nullable = true)
 |-- scaleFactor_MUON: float (nullable = true)
 |-- scaleFactor_BTAG: float (nullable = true)
 |-- scaleFactor_TRIGGER: float (nullable = true)
 |-- scaleFactor_JVFSF: float (nullable = true)
 |-- scaleFactor_ZVERTEX: float (nullable = true)
 |-- trigE: boolean (nullable = true)
 |-- trigM: boolean (nullable = true)
 |-- passGRL: boolean (nullable = true)
 |-- hasGoodVertex: boolean (nullable = true)
 |-- lep_n: integer (nullable = true)
 |-- lep_truthMatched: array (nullable = true)
 |    |-- element: boolean (containsNull = true)
 |-- lep_trigMatched: array (nullable = true)
 |    |-- element: short (containsNull = true)
 |-- lep_pt: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_eta: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_phi: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_E: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_z0: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_charge: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_type: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- lep_flag: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- lep_ptcone30: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_etcone20: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_trackd0pvunbiased: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_tracksigd0pvunbiased: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- met_et: float (nullable = true)
 |-- met_phi: float (nullable = true)
 |-- jet_n: integer (nullable = true)
 |-- alljet_n: integer (nullable = true)
 |-- jet_pt: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_eta: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_phi: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_E: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_m: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_jvf: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_trueflav: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- jet_truthMatched: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- jet_SV0: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_MV1: array (nullable = true)
 |    |-- element: float (containsNull = true)



scala> dfGamma.printSchema
root
 |-- runNumber: integer (nullable = true)
 |-- eventNumber: integer (nullable = true)
 |-- channelNumber: integer (nullable = true)
 |-- mcWeight: float (nullable = true)
 |-- pvxp_n: integer (nullable = true)
 |-- vxp_z: float (nullable = true)
 |-- scaleFactor_PILEUP: float (nullable = true)
 |-- scaleFactor_ELE: float (nullable = true)
 |-- scaleFactor_MUON: float (nullable = true)
 |-- scaleFactor_BTAG: float (nullable = true)
 |-- scaleFactor_TRIGGER: float (nullable = true)
 |-- scaleFactor_JVFSF: float (nullable = true)
 |-- scaleFactor_ZVERTEX: float (nullable = true)
 |-- trigE: boolean (nullable = true)
 |-- trigM: boolean (nullable = true)
 |-- passGRL: boolean (nullable = true)
 |-- hasGoodVertex: boolean (nullable = true)
 |-- lep_n: integer (nullable = true)
 |-- lep_truthMatched: array (nullable = true)
 |    |-- element: boolean (containsNull = true)
 |-- lep_trigMatched: array (nullable = true)
 |    |-- element: short (containsNull = true)
 |-- lep_pt: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_eta: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_phi: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_E: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_z0: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_charge: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_type: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- lep_flag: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- lep_ptcone30: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_etcone20: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_trackd0pvunbiased: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_tracksigd0pvunbiased: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- met_et: float (nullable = true)
 |-- met_phi: float (nullable = true)
 |-- jet_n: integer (nullable = true)
 |-- alljet_n: integer (nullable = true)
 |-- jet_pt: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_eta: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_phi: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_E: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_m: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_jvf: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_trueflav: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- jet_truthMatched: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- jet_SV0: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_MV1: array (nullable = true)
 |    |-- element: float (containsNull = true)

# Cannot write Dataframes to csv files. Only to parquet files, why?

scala> dfGamma.write.csv("gamma.csv")
org.apache.spark.sql.AnalysisException: CSV data source does not support array<boolean> data type.;
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$$anonfun$verifySchema$1.apply(DataSourceUtils.scala:49)
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$$anonfun$verifySchema$1.apply(DataSourceUtils.scala:47)
  at scala.collection.Iterator$class.foreach(Iterator.scala:891)
  at scala.collection.AbstractIterator.foreach(Iterator.scala:1334)
  at scala.collection.IterableLike$class.foreach(IterableLike.scala:72)
  at org.apache.spark.sql.types.StructType.foreach(StructType.scala:99)
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$.verifySchema(DataSourceUtils.scala:47)
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$.verifyWriteSchema(DataSourceUtils.scala:32)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:100)
  at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:159)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:104)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:102)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.doExecute(commands.scala:122)
  at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(SparkPlan.scala:131)
  at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(SparkPlan.scala:127)
  at org.apache.spark.sql.execution.SparkPlan$$anonfun$executeQuery$1.apply(SparkPlan.scala:155)
  at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
  at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:152)
  at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:127)
  at org.apache.spark.sql.execution.QueryExecution.toRdd$lzycompute(QueryExecution.scala:80)
  at org.apache.spark.sql.execution.QueryExecution.toRdd(QueryExecution.scala:80)
  at org.apache.spark.sql.DataFrameWriter$$anonfun$runCommand$1.apply(DataFrameWriter.scala:668)
  at org.apache.spark.sql.DataFrameWriter$$anonfun$runCommand$1.apply(DataFrameWriter.scala:668)
  at org.apache.spark.sql.execution.SQLExecution$$anonfun$withNewExecutionId$1.apply(SQLExecution.scala:78)
  at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:125)
  at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:73)
  at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:668)
  at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:276)
  at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:270)
  at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:228)
  at org.apache.spark.sql.DataFrameWriter.csv(DataFrameWriter.scala:656)
  ... 53 elided

scala> 


scala> dfMuons.write.csv("muons.csv")
org.apache.spark.sql.AnalysisException: CSV data source does not support array<boolean> data type.;
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$$anonfun$verifySchema$1.apply(DataSourceUtils.scala:49)
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$$anonfun$verifySchema$1.apply(DataSourceUtils.scala:47)
  at scala.collection.Iterator$class.foreach(Iterator.scala:891)
  at scala.collection.AbstractIterator.foreach(Iterator.scala:1334)
  at scala.collection.IterableLike$class.foreach(IterableLike.scala:72)
  at org.apache.spark.sql.types.StructType.foreach(StructType.scala:99)
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$.verifySchema(DataSourceUtils.scala:47)
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$.verifyWriteSchema(DataSourceUtils.scala:32)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:100)
  at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:159)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:104)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:102)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.doExecute(commands.scala:122)
  at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(SparkPlan.scala:131)
  at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(SparkPlan.scala:127)
  at org.apache.spark.sql.execution.SparkPlan$$anonfun$executeQuery$1.apply(SparkPlan.scala:155)
  at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
  at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:152)
  at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:127)
  at org.apache.spark.sql.execution.QueryExecution.toRdd$lzycompute(QueryExecution.scala:80)
  at org.apache.spark.sql.execution.QueryExecution.toRdd(QueryExecution.scala:80)
  at org.apache.spark.sql.DataFrameWriter$$anonfun$runCommand$1.apply(DataFrameWriter.scala:668)
  at org.apache.spark.sql.DataFrameWriter$$anonfun$runCommand$1.apply(DataFrameWriter.scala:668)
  at org.apache.spark.sql.execution.SQLExecution$$anonfun$withNewExecutionId$1.apply(SQLExecution.scala:78)
  at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:125)
  at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:73)
  at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:668)
  at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:276)
  at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:270)
  at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:228)
  at org.apache.spark.sql.DataFrameWriter.csv(DataFrameWriter.scala:656)
  ... 53 elided

scala> 



scala> dfMuons.schema
res9: org.apache.spark.sql.types.StructType = StructType(StructField(runNumber,IntegerType,true), StructField(eventNumber,IntegerType,true), StructField(channelNumber,IntegerType,true), StructField(mcWeight,FloatType,true), StructField(pvxp_n,IntegerType,true), StructField(vxp_z,FloatType,true), StructField(scaleFactor_PILEUP,FloatType,true), StructField(scaleFactor_ELE,FloatType,true), StructField(scaleFactor_MUON,FloatType,true), StructField(scaleFactor_BTAG,FloatType,true), StructField(scaleFactor_TRIGGER,FloatType,true), StructField(scaleFactor_JVFSF,FloatType,true), StructField(scaleFactor_ZVERTEX,FloatType,true), StructField(trigE,BooleanType,true), StructField(trigM,BooleanType,true), StructField(passGRL,BooleanType,true), StructField(hasGoodVertex,BooleanType,true), StructField(...

scala> dfMuons.select("met_et").show()
+---------+
|   met_et|
+---------+
|  94215.1|
|30354.057|
|54632.633|
|18974.707|
| 18013.09|
|36319.457|
|16567.258|
| 46948.73|
|26812.076|
|56296.965|
|47373.312|
| 22700.36|
| 66713.07|
|27822.385|
|40274.438|
| 42998.26|
|  26655.3|
|26368.762|
| 49253.22|
|19660.633|
+---------+
only showing top 20 rows


scala> dfMuons.select("met_et").count
res11: Long = 7028084                                                           

scala> dfMuons.select($"met_et",$"jet_jvf").count
res12: Long = 7028084                                                           

scala> dfMuons.select($"met_et",$"jet_jvf").show(5,false)
+---------+-----------+
|met_et   |jet_jvf    |
+---------+-----------+
|94215.1  |[]         |
|30354.057|[0.9523457]|
|54632.633|[]         |
|18974.707|[]         |
|18013.09 |[]         |
+---------+-----------+
only showing top 5 rows

scala> dfMuons.createOrReplaceTempView("MUONS")

scala> val sqlDFMuons = spark.sql("SELECT * FROM MUONS").show(5,false)
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+-------------+------------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+---------+----------+-----+--------+-----------+-----------+---------+-----------+----------+-----------+------------+----------------+-------+-------------+
|runNumber|eventNumber|channelNumber|mcWeight|pvxp_n|vxp_z     |scaleFactor_PILEUP|scaleFactor_ELE|scaleFactor_MUON|scaleFactor_BTAG|scaleFactor_TRIGGER|scaleFactor_JVFSF|scaleFactor_ZVERTEX|trigE|trigM|passGRL|hasGoodVertex|lep_n|lep_truthMatched|lep_trigMatched|lep_pt     |lep_eta      |lep_phi     |lep_E      |lep_z0        |lep_charge|lep_type|lep_flag   |lep_ptcone30|lep_etcone20|lep_trackd0pvunbiased|lep_tracksigd0pvunbiased|met_et   |met_phi   |jet_n|alljet_n|jet_pt     |jet_eta    |jet_phi  |jet_E      |jet_m     |jet_jvf    |jet_trueflav|jet_truthMatched|jet_SV0|jet_MV1      |
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+-------------+------------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+---------+----------+-----+--------+-----------+-----------+---------+-----------+----------+-----------+------------+----------------+-------+-------------+
|207490   |17281852   |207490       |0.0     |15    |-12.316585|0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[3]            |[40531.855]|[0.288244]   |[1.3469992] |[42227.465]|[-0.045446984]|[-1.0]    |[13]    |[568344575]|[0.0]       |[94.18325]  |[-0.04912882]        |[0.0152232405]          |94215.1  |-1.3943559|0    |0       |[]         |[]         |[]       |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
|207490   |17282007   |207490       |0.0     |15    |22.651913 |0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[3]            |[37172.49] |[-0.21478392]|[-2.1380057]|[38033.36] |[0.22578493]  |[1.0]     |[13]    |[568344575]|[0.0]       |[65.88673]  |[-0.032381147]       |[0.025347514]           |30354.057|0.3259549 |1    |1       |[27929.969]|[-2.170531]|[1.42328]|[124052.25]|[4573.633]|[0.9523457]|[-99]       |[0]             |[0.0]  |[0.055551887]|
|207490   |17282941   |207490       |0.0     |14    |67.00033  |0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[3]            |[41404.363]|[-1.2001014] |[1.3576007] |[74975.45] |[-0.030548852]|[1.0]     |[13]    |[568344575]|[0.0]       |[628.7983]  |[-0.0059319637]      |[0.018147442]           |54632.633|-2.053428 |0    |0       |[]         |[]         |[]       |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
|207490   |17283582   |207490       |0.0     |14    |25.114586 |0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[3]            |[36330.36] |[1.6244663]  |[-0.0825191]|[95780.08] |[0.016617686] |[1.0]     |[13]    |[568344575]|[0.0]       |[78.632385] |[-0.012363502]       |[0.017884906]           |18974.707|2.3157902 |0    |0       |[]         |[]         |[]       |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
|207490   |17284798   |207490       |0.0     |13    |5.419942  |0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[1]            |[29865.918]|[1.9333806]  |[-2.166267] |[105389.39]|[-0.008121626]|[1.0]     |[13]    |[568344575]|[0.0]       |[228.17558] |[-0.0022841152]      |[0.017784366]           |18013.09 |0.86960316|0    |0       |[]         |[]         |[]       |[]         |[]        |[]         |[]          |[]              |[]     |[]           |
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+-------------+------------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+---------+----------+-----+--------+-----------+-----------+---------+-----------+----------+-----------+------------+----------------+-------+-------------+
only showing top 5 rows

sqlDFMuons: Unit = ()

scala> val sqlDFMuons = spark.sql("SELECT * FROM MUONS").show(1,false)
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+----------+-----------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+-------+----------+-----+--------+------+-------+-------+-----+-----+-------+------------+----------------+-------+-------+
|runNumber|eventNumber|channelNumber|mcWeight|pvxp_n|vxp_z     |scaleFactor_PILEUP|scaleFactor_ELE|scaleFactor_MUON|scaleFactor_BTAG|scaleFactor_TRIGGER|scaleFactor_JVFSF|scaleFactor_ZVERTEX|trigE|trigM|passGRL|hasGoodVertex|lep_n|lep_truthMatched|lep_trigMatched|lep_pt     |lep_eta   |lep_phi    |lep_E      |lep_z0        |lep_charge|lep_type|lep_flag   |lep_ptcone30|lep_etcone20|lep_trackd0pvunbiased|lep_tracksigd0pvunbiased|met_et |met_phi   |jet_n|alljet_n|jet_pt|jet_eta|jet_phi|jet_E|jet_m|jet_jvf|jet_trueflav|jet_truthMatched|jet_SV0|jet_MV1|
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+----------+-----------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+-------+----------+-----+--------+------+-------+-------+-----+-----+-------+------------+----------------+-------+-------+
|207490   |17281852   |207490       |0.0     |15    |-12.316585|0.0               |0.0            |0.0             |0.0             |0.0                |0.0              |0.0                |false|true |true   |true         |1    |[false]         |[3]            |[40531.855]|[0.288244]|[1.3469992]|[42227.465]|[-0.045446984]|[-1.0]    |[13]    |[568344575]|[0.0]       |[94.18325]  |[-0.04912882]        |[0.0152232405]          |94215.1|-1.3943559|0    |0       |[]    |[]     |[]     |[]   |[]   |[]     |[]          |[]              |[]     |[]     |
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+----------+-----------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+-------+----------+-----+--------+------+-------+-------+-----+-----+-------+------------+----------------+-------+-------+
only showing top 1 row

sqlDFMuons: Unit = ()

scala> val sqlDFMuons = spark.sql("SELECT * FROM MUONS").show(1,true)
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+----------+-----------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+-------+----------+-----+--------+------+-------+-------+-----+-----+-------+------------+----------------+-------+-------+
|runNumber|eventNumber|channelNumber|mcWeight|pvxp_n|     vxp_z|scaleFactor_PILEUP|scaleFactor_ELE|scaleFactor_MUON|scaleFactor_BTAG|scaleFactor_TRIGGER|scaleFactor_JVFSF|scaleFactor_ZVERTEX|trigE|trigM|passGRL|hasGoodVertex|lep_n|lep_truthMatched|lep_trigMatched|     lep_pt|   lep_eta|    lep_phi|      lep_E|        lep_z0|lep_charge|lep_type|   lep_flag|lep_ptcone30|lep_etcone20|lep_trackd0pvunbiased|lep_tracksigd0pvunbiased| met_et|   met_phi|jet_n|alljet_n|jet_pt|jet_eta|jet_phi|jet_E|jet_m|jet_jvf|jet_trueflav|jet_truthMatched|jet_SV0|jet_MV1|
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+----------+-----------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+-------+----------+-----+--------+------+-------+-------+-----+-----+-------+------------+----------------+-------+-------+
|   207490|   17281852|       207490|     0.0|    15|-12.316585|               0.0|            0.0|             0.0|             0.0|                0.0|              0.0|                0.0|false| true|   true|         true|    1|         [false]|            [3]|[40531.855]|[0.288244]|[1.3469992]|[42227.465]|[-0.045446984]|    [-1.0]|    [13]|[568344575]|       [0.0]|  [94.18325]|        [-0.04912882]|          [0.0152232405]|94215.1|-1.3943559|    0|       0|    []|     []|     []|   []|   []|     []|          []|              []|     []|     []|
+---------+-----------+-------------+--------+------+----------+------------------+---------------+----------------+----------------+-------------------+-----------------+-------------------+-----+-----+-------+-------------+-----+----------------+---------------+-----------+----------+-----------+-----------+--------------+----------+--------+-----------+------------+------------+---------------------+------------------------+-------+----------+-----+--------+------+-------+-------+-----+-----+-------+------------+----------------+-------+-------+
only showing top 1 row

sqlDFMuons: Unit = ()
scala> spark.sqlContext.setConf("parquet.filter.statistics.enabled","true")

scala> spark.sqlContext.setConf("parquet.filter.dictionary.enabled","true")

scala> spark.sqlContext.setConf("spark.sql.parquet.filterPushdown","true")

scala> spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet","true")

scala> spark.sqlContext.setConf("spark.sql.hive.convertMetastoreParquet.mergeSchema","false")

scala> spark.sqlContext.setConf("spark.sql.parquet.mergeSchema","false")

scala> val sqlContext = spark.sqlContext
sqlContext: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@39e621bf

import org.dianahep.sparkroot.experimental._

scala> val ds = dfMuons.as[Seq[Seq[Double]]]
org.apache.spark.sql.AnalysisException: Try to map struct<runNumber:int,eventNumber:int,channelNumber:int,mcWeight:float,pvxp_n:int,vxp_z:float,scaleFactor_PILEUP:float,scaleFactor_ELE:float,scaleFactor_MUON:float,scaleFactor_BTAG:float,scaleFactor_TRIGGER:float,scaleFactor_JVFSF:float,scaleFactor_ZVERTEX:float,trigE:boolean,trigM:boolean,passGRL:boolean,hasGoodVertex:boolean,lep_n:int,lep_truthMatched:array<boolean>,lep_trigMatched:array<smallint>,lep_pt:array<float>,lep_eta:array<float>,lep_phi:array<float>,lep_E:array<float>,lep_z0:array<float>,lep_charge:array<float>,lep_type:array<int>,lep_flag:array<int>,lep_ptcone30:array<float>,lep_etcone20:array<float>,lep_trackd0pvunbiased:array<float>,lep_tracksigd0pvunbiased:array<float>,met_et:float,met_phi:float,jet_n:int,alljet_n:int,jet_pt:array<float>,jet_eta:array<float>,jet_phi:array<float>,jet_E:array<float>,jet_m:array<float>,jet_jvf:array<float>,jet_trueflav:array<int>,jet_truthMatched:array<int>,jet_SV0:array<float>,jet_MV1:array<float>> to Tuple1, but failed as the number of fields does not line up.;
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveDeserializer$.org$apache$spark$sql$catalyst$analysis$Analyzer$ResolveDeserializer$$fail(Analyzer.scala:2417)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveDeserializer$.org$apache$spark$sql$catalyst$analysis$Analyzer$ResolveDeserializer$$validateTopLevelTupleFields(Analyzer.scala:2434)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveDeserializer$$anonfun$apply$36$$anonfun$applyOrElse$10.applyOrElse(Analyzer.scala:2395)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveDeserializer$$anonfun$apply$36$$anonfun$applyOrElse$10.applyOrElse(Analyzer.scala:2387)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$2.apply(TreeNode.scala:256)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$2.apply(TreeNode.scala:256)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:255)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsDown$1.apply(QueryPlan.scala:83)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsDown$1.apply(QueryPlan.scala:83)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$1.apply(QueryPlan.scala:105)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$1.apply(QueryPlan.scala:105)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpression$1(QueryPlan.scala:104)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$1(QueryPlan.scala:116)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$2.apply(QueryPlan.scala:126)
  at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:187)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.mapExpressions(QueryPlan.scala:126)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionsDown(QueryPlan.scala:83)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressions(QueryPlan.scala:74)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveDeserializer$$anonfun$apply$36.applyOrElse(Analyzer.scala:2387)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveDeserializer$$anonfun$apply$36.applyOrElse(Analyzer.scala:2383)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$$anonfun$resolveOperatorsUp$1$$anonfun$apply$1.apply(AnalysisHelper.scala:90)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$$anonfun$resolveOperatorsUp$1$$anonfun$apply$1.apply(AnalysisHelper.scala:90)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$$anonfun$resolveOperatorsUp$1.apply(AnalysisHelper.scala:89)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$$anonfun$resolveOperatorsUp$1.apply(AnalysisHelper.scala:86)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$.allowInvokingTransformsInAnalyzer(AnalysisHelper.scala:194)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$class.resolveOperatorsUp(AnalysisHelper.scala:86)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.resolveOperatorsUp(LogicalPlan.scala:29)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveDeserializer$.apply(Analyzer.scala:2383)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveDeserializer$.apply(Analyzer.scala:2382)
  at org.apache.spark.sql.catalyst.rules.RuleExecutor$$anonfun$execute$1$$anonfun$apply$1.apply(RuleExecutor.scala:87)
  at org.apache.spark.sql.catalyst.rules.RuleExecutor$$anonfun$execute$1$$anonfun$apply$1.apply(RuleExecutor.scala:84)
  at scala.collection.LinearSeqOptimized$class.foldLeft(LinearSeqOptimized.scala:124)
  at scala.collection.immutable.List.foldLeft(List.scala:84)
  at org.apache.spark.sql.catalyst.rules.RuleExecutor$$anonfun$execute$1.apply(RuleExecutor.scala:84)
  at org.apache.spark.sql.catalyst.rules.RuleExecutor$$anonfun$execute$1.apply(RuleExecutor.scala:76)
  at scala.collection.immutable.List.foreach(List.scala:392)
  at org.apache.spark.sql.catalyst.rules.RuleExecutor.execute(RuleExecutor.scala:76)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.org$apac

# 
scala> dfMuons.printSchema
root
 |-- runNumber: integer (nullable = true)
 |-- eventNumber: integer (nullable = true)
 |-- channelNumber: integer (nullable = true)
 |-- mcWeight: float (nullable = true)
 |-- pvxp_n: integer (nullable = true)
 |-- vxp_z: float (nullable = true)
 |-- scaleFactor_PILEUP: float (nullable = true)
 |-- scaleFactor_ELE: float (nullable = true)
 |-- scaleFactor_MUON: float (nullable = true)
 |-- scaleFactor_BTAG: float (nullable = true)
 |-- scaleFactor_TRIGGER: float (nullable = true)
 |-- scaleFactor_JVFSF: float (nullable = true)
 |-- scaleFactor_ZVERTEX: float (nullable = true)
 |-- trigE: boolean (nullable = true)
 |-- trigM: boolean (nullable = true)
 |-- passGRL: boolean (nullable = true)
 |-- hasGoodVertex: boolean (nullable = true)
 |-- lep_n: integer (nullable = true)
 |-- lep_truthMatched: array (nullable = true)
 |    |-- element: boolean (containsNull = true)
 |-- lep_trigMatched: array (nullable = true)
 |    |-- element: short (containsNull = true)
 |-- lep_pt: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_eta: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_phi: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_E: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_z0: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_charge: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_type: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- lep_flag: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- lep_ptcone30: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_etcone20: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_trackd0pvunbiased: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- lep_tracksigd0pvunbiased: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- met_et: float (nullable = true)
 |-- met_phi: float (nullable = true)
 |-- jet_n: integer (nullable = true)
 |-- alljet_n: integer (nullable = true)
 |-- jet_pt: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_eta: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_phi: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_E: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_m: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_jvf: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_trueflav: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- jet_truthMatched: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- jet_SV0: array (nullable = true)
 |    |-- element: float (containsNull = true)
 |-- jet_MV1: array (nullable = true)
 |    |-- element: float (containsNull = true)


scala> 


val ds = dfMuons.as[Seq[Seq[Double]]]
 
