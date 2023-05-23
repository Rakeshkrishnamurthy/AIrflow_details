import subprocess
from pyspark.sql import functions as f
from operator import add
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructField, StringType, StructType

def sparkwithhiveone():
    sparkwithhive = getsparkwithhive()
    try:
        assert (sparkwithhive.conf.get("spark.sql.catalogImplementation") == "hive")
        sparkwithhive.sql("DROP TABLE IF EXISTS mydb.src")
        sparkwithhive.sql("CREATE TABLE IF NOT EXISTS mydb.src (key INT, value STRING)")
        sparkwithhive.sql("LOAD DATA LOCAL INPATH '/usr/local/spark-2.4.6-bin-hadoop2.7/examples/src/main/resources"
                          "/kv1.txt' INTO TABLE mydb.src")
        sparkwithhive.sql("SELECT * FROM mydb.src").limit(10).show()
        sparkwithhive.sql("SELECT COUNT(*) FROM mydb.src").show()
        # You can also use DataFrames to create temporary views within a SparkSession.
        record: Row = Row("key", "value")
        recordsdf = sparkwithhive.createDataFrame([record(j, "val_" + str(j)) for j in range(1, 101)])
        recordsdf.createOrReplaceTempView("records")
        # Queries can then join DataFrame data with data stored in Hive.
        sparkwithhive.sql("SELECT * FROM records r JOIN mydb.src s ON r.key = s.key").show()
    except Exception as e:
        print("Unexpected error:", e)
        raise
    finally:
        sparkwithhive.stop()
        print("finally block")


def getsparkwithhive():
    sparkwithhive = SparkSession.builder \
        .master("spark://thanoojubuntu-Inspiron-3521:7077") \
        .config("spark.sql.hive.metastore.version", "2.3.0") \
        .config("spark.sql.warehouse.dir", "hdfs://user/hive/warehouse") \
        .config("spark.sql.hive.metastore.jars", "/home/hduser/softwares/apache-hive-2.3.3-bin/lib/*") \
        .config("spark.sql.catalogImplementation=hive") \
        .enableHiveSupport().getOrCreate()
    return sparkwithhive


def schema_inference_example(sparkobj):
    # Load a text file and convert each line to a Row.
    # HDFS DFS
    lines = sparkobj.sparkContext.textFile("people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
    # Infer the schema, and register the DataFrame as a table.
    schemapeople = sparkobj.createDataFrame(people)
    schemapeople.createOrReplaceTempView("people")
    sparkobj.sql("SELECT * FROM people").show()
    sparkobj.sql("SELECT count(*) FROM people").show()
    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = sparkobj.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    # The results of SQL queries are Dataframe objects.
    # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
    teenagers.write.option("compression", "snappy").mode("overwrite").parquet("teenagers")
    teennames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    for name in teennames:
        print(name)


def programmatic_schema_example(sparkobj):
    sc = sparkobj.sparkContext
    # Load a text file and convert e-ach line to a Row.
    lines = sc.textFile("people.txt")
    parts = lines.map(lambda l: l.split(","))
    # Each line is converted to a tuple.
    people = parts.map(lambda p: (p[0], p[1].strip()))
    # The schema is encoded in a string.
    schemastring = "name age"
    fields = [StructField(field_name, StringType(), True) for field_name in schemastring.split()]
    schema = StructType(fields)
    print(fields)
    print(schema)
    # Apply the schema to the RDD.
    schemapeople = sparkobj.createDataFrame(people, schema)
    subprocess.call(["hdfs", "dfs", "-rm", "-f", "-r", "namesAndAges.parquet"])
    # snappy vs zlib
    # 'Zlib' achieves a lot more compression and is correspondingly less performant.
    # 'Snappy' aims for 'aims for very high speeds and reasonable compression'.
    sparkobj.sql("set spark.sql.parquet.compression.codec=gzip")
    schemapeople.write.parquet("namesAndAges.parquet")
    # Creates a temporary view using the DataFrame
    schemapeople.createOrReplaceTempView("people")
    # SQL can be run over DataFrames that have been registered as a table.
    results = sparkobj.sql("SELECT name FROM people")
    results.show()
    results.write.option("compression", "snappy").mode("overwrite").parquet("people")
    squaresdf = sparkobj.createDataFrame(sparkobj.sparkContext.parallelize(range(1, 6))
                                         .map(lambda j: Row(single=j, double=j ** 2)))
    # subprocess.call(["hdfs", "dfs", "-rm", "-f", "-r", "test_table"])
    squaresdf.write.mode("overwrite").parquet("test_table")


def datasourceone(sparkds):
    df = sparkds.read.load("people.json", format="json")
    some_path = "namesAndAges.parquet"
    subprocess.call(["hdfs", "dfs", "-rm", "-f", "-r", some_path])
    df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")
    df = sparkds.read.load("namesAndAges.parquet", format="parquet")
    df.createOrReplaceTempView("namesages")
    sparkds.sql("select * from namesages").show()


def dodfactions(sparkobj):
    tuplelist = [('Alice', 1)]
    c1 = sparkobj.createDataFrame(tuplelist).collect()
    print(c1)
    d = [{'name': 'Alice', 'age': 1}]
    c2 = spark.createDataFrame(d).collect()
    print(c2)
    rdd = sparkobj.sparkContext.parallelize(tuplelist)
    c3 = spark.createDataFrame(rdd).collect()
    print(c3)
    df = spark.createDataFrame(rdd, ['name', 'age'])
    c4 = df.collect()
    print(c4)
    personrow = Row('name', 'age')
    person = rdd.map(lambda r: personrow(r))
    df2 = spark.createDataFrame(person)
    c5 = df2.collect()
    print(c5)
    print(df.select("name", "age").collect())


def getspark():
    sparkobj: SparkSession = SparkSession.builder \
        .master("spark://thanoojubuntu-Inspiron-3521:7077") \
        .getOrCreate()
    return sparkobj


def dowithcolumn(sparkobj):
    data = [Row("Smith", "12-06-2004", "M", "3000"),
            Row("Michael", "02-05-2004", "M", "4000"),
            Row("Robert ", "12-02-2002", "M", "4000"),
            Row("Maria ", "29-01-1999", "F", "4000")]
    schema = StructType([StructField("name", StringType()),
                         StructField("dob", StringType()),
                         StructField("gender", StringType()),
                         StructField("salary", StringType())])
    df = sparkobj.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show(truncate=False)
    df = df.withColumn("salary", f.col("salary").cast("Integer"))
    df.printSchema()
    df = df.withColumn("salary", f.col("salary") * 100)
    df = df.withColumn("copiedsalary", f.col("salary") * -1)
    df = df.withColumnRenamed("gender", "sex")
    df.printSchema()
    df = df.drop("copiedsalary")
    df.printSchema()
    df.select(f.col("name"), f.col("dob")).show()
    df.selectExpr("name", "dob").show()


def otheractions(sparkobj):
    listrdd = sparkobj.sparkContext.parallelize([1, 2, 3, 4, 5, 3, 2])
    print("Count : " + str(listrdd.count()))
    print("countApprox : " + str(listrdd.countApprox(1200)))
    print("countApproxDistinct : " + str(listrdd.countApproxDistinct()))
    print("first :  " + str(listrdd.first()))
    print("top : " + str(listrdd.top(2)))
    print("min :  " + str(listrdd.min()))
    print("min :  " + str(listrdd.max()))
    print("takeOrdered : " + str(listrdd.takeOrdered(2)))
    sampledata = [("James", "Sales", "NY", 90000, 34, 10000),
                  ("Michael", "Sales", "NY", 86000, 56, 20000),
                  ("Robert", "Sales", "CA", 81000, 30, 23000),
                  ("Maria", "Finance", "CA", 90000, 24, 23000),
                  ("Raman", "Finance", "CA", 99000, 40, 24000),
                  ("Scott", "Finance", "NY", 83000, 36, 19000),
                  ("Jen", "Finance", "NY", 79000, 53, 15000),
                  ("Jeff", "Marketing", "CA", 80000, 25, 18000),
                  ("Kumar", "Marketing", "NY", 91000, 50, 21000)]
    columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
    df = sparkobj.createDataFrame(data=sampledata, schema=columns)
    df.printSchema()
    df.show(truncate=False)
    df.sort("department", "state").show(truncate=False)
    df.orderBy("department", "state").show(truncate=False)
    df.sort(df.department.asc(), df.state.asc()).show(truncate=False)
    df.sort(f.col("department").asc(), f.col("state").asc()).show(truncate=False)
    df.orderBy(f.col("department").asc(), f.col("state").asc()).show(truncate=False)
    redres = listrdd.reduce(add)
    print(redres)


def doreadjson(sparkobj):
    df = sparkobj.read.json("people.json")
    df.printSchema()
    df.createOrReplaceTempView("peoplejson")
    peopleval = sparkobj.sql("select * from peoplejson")
    peopleval.show()
    peopleval.write.option("compression", "snappy").mode("overwrite").parquet("peoplejson")


def doreducebykey(sparkobj):
    pass


def dogroupbykey(sparkobj):
    pass


if __name__ == '__main__':
    # sparkwithhiveobj = getsparkwithhive()

    # sparkwithhiveone()

    spark = getspark()
    # doreadjson(spark)
    # dodfactions(spark)
    # otheractions(spark)
# dogroupbykey(spark)
# doreducebykey(spark)
    dowithcolumn(spark)
# schema_inference_example(spark)
# programmatic_schema_example(spark)
# datasourceone(spark)
# spark.stop()

"""$ hdfs dfs -ls namesAndAges.parquet Found 3 items -rw-r--r--   1 hduser hadoop          0 2020-11-21 19:06 
namesAndAges.parquet/_SUCCESS -rw-r--r--   1 hduser hadoop        609 2020-11-21 19:06 
namesAndAges.parquet/part-00000-a4d26348-5c0c-4e58-bc27-2ec8d5d2f9e1-c000.snappy.parquet -rw-r--r--   1 hduser hadoop 
       646 2020-11-21 19:06 namesAndAges.parquet/part-00001-a4d26348-5c0c-4e58-bc27-2ec8d5d2f9e1-c000.snappy.parquet 
       $ hdfs dfs -ls namesAndAges.parquet Found 3 items -rw-r--r--   1 hduser hadoop          0 2020-11-21 19:20 
       namesAndAges.parquet/_SUCCESS -rw-r--r--   1 hduser hadoop        639 2020-11-21 19:20 
       namesAndAges.parquet/part-00000-106d5c23-d9d1-49cd-a468-7182a9b20569-c000.gz.parquet -rw-r--r--   1 hduser 
       hadoop        679 2020-11-21 19:20 namesAndAges.parquet/part-00001-106d5c23-d9d1-49cd-a468-7182a9b20569-c000
       .gz.parquet """