This is a example of Apache Hudi. Here we inserted user records. You can see all the records using spark-shell or any other way. Main purpose of this demo to see if update to a row works or not. 

## Build

```shell
mvn clean install
```

## Run

```shell
spark-shell --packages org.apache.hudi:hudi-spark-bundle_2.12:0.7.0,org.apache.spark:spark-avro_2.12:3.0.1 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
```

Spark shell command

```shell
val tripsSnapshotDF = spark.read.format("hudi").load("file:///tmp/user_data/iteration_2/user_data/*")
tripsSnapshotDF.createOrReplaceTempView("user_data")
spark.sql("select * from  user_data where userId='30'").show()
```

### Update data and is should be visible
```java
Go to com.devlibx.data.source.DataGenerator

Uncomment "// TODO - Make changes to user 30"
This will update data for user=30
```

Again run and you will see modified data (rides will be "{updated_1_30_30...")
```shell
val tripsSnapshotDF = spark.read.format("hudi").load("file:///tmp/user_data/iteration_2/user_data/*")
tripsSnapshotDF.createOrReplaceTempView("user_data")
spark.sql("select * from  user_data where userId='30'").show()
```




