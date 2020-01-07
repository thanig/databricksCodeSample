# Load to sql table, normal load

 FactPurchasing.write.mode("overwrite").format("jdbc") \
 .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
 .option("truncate", "true") \
 .option("url", jdbcUrl) \
 .option("dbtable", "dbo.test2") \
 .option("user", jdbcUsername) \
 .option("batchsize","100000") \
 .option("password", jdbcPassword).save()

# Load with spark bulk sql connector.

FactPurchasing.createOrReplaceTempView('testbulk')

%scala
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.connect._
val bulkCopyConfig = Config(Map(
  "url"               -> "ba-dev-asqlserver.database.windows.net",
  "databaseName"      -> "ba-dev-asqldb",
  "user"              -> "sql_user",
  "password"          -> "LaerdalTest#123",
  "dbTable"           -> "dbo.test2",
  "bulkCopyBatchSize" -> "15000",
  "bulkCopyTableLock" -> "true",
  "bulkCopyTimeout"   -> "600"
))

spark.table("testbulk").bulkCopyToSqlDB(bulkCopyConfig)
# df.bulkCopyToSqlDB(bulkCopyConfig, bulkCopyMetadata) if metadata is specified 
# df.bulkCopyToSqlDB(bulkCopyConfig) if no metadata is specified.



#Print lists in Python (4 Different Ways)
#Printing a list in python can be done is following ways:

#Using for loop : Traverse from 0 to len(list) and print all elements of the list one by one uisng a for loop, this is the standard practice of doing it.
#filter_none
#edit
#play_arrow

#brightness_4
# Python program to print list 
# using for loop 
a = [1, 2, 3, 4, 5] 
  
# printing the list using loop 
for x in range(len(a)): 
    print a[x], 
