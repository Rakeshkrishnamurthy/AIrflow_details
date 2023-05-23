import subprocess
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from operator import add
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructField, StringType, StructType

def sparkwithhiveone():
    sparkwithhive = getsparkwithhive()
    try:
        assert (sparkwithhive.conf.get("spark.sql.catalogImplementation") == "hive")
        sparkwithhive.sql("DROP TABLE IF EXISTS prd_us_npd_pos_bea_ust.supprsn_bea_pos_ust_corporationus_nc")
        sparkwithhive.sql("CREATE TABLE IF NOT EXISTS prd_us_npd_pos_bea_ust.supprsn_bea_pos_ust_corporationus_nc(corporationus_key INT , pp_month INT, category_key INT, brand_key INT)")
        #sparkwithhive.sql("LOAD DATA LOCAL INPATH '/usr/local/spark-2.4.6-bin-hadoop2.7/examples/src/main/resources"
        #                  "/kv1.txt' INTO TABLE prd_us_npd_pos_bea_ust.supprsn_bea_pos_ust_corporationus_nc")
        sparkwithhive.sql("SELECT * FROM prd_us_npd_pos_bea_ust.supprsn_bea_pos_ust_corporationus_nc").limit(10).show()
        sparkwithhive.sql("SELECT COUNT(*) FROM prd_us_npd_pos_bea_ust.supprsn_bea_pos_ust_corporationus_nc").show()
        recordsdf = sparkwithhive.sql("""insert into prd_us_npd_pos_bea_ust.supprsn_bea_pos_ust_corporationus_nc (select supp_result.corporationus_key , supp_result.pp_month, category_key, brand_key from(select corporationus_key , pp_month,max(rule1_cnt) AS rule1_cnt , max(s_o_1137452989), max(s_o_1137452999), max(s_o_1137453014) from(select corporationus_key , pp_month, outletdivservice_key,(DENSE_RANK() OVER (PARTITION BY corporationus_key , pp_month,pp_month ORDER BY outletdivservice_key ASC ) + DENSE_RANK() OVER (PARTITION BY corporationus_key , pp_month,pp_month ORDER BY outletdivservice_key DESC) - 1) AS rule1_cnt ,case when round(sum(edited_sales) OVER (PARTITION BY corporationus_key , pp_month),2) > 0 then (sum(case when outletdivservice_key = 1137452989 then edited_sales else 0 end) OVER (PARTITION BY corporationus_key , pp_month, outletdivservice_key) / sum(edited_sales) OVER (PARTITION BY corporationus_key , pp_month))*100 else 0 end AS s_o_1137452989,case when round(sum(edited_sales) OVER (PARTITION BY corporationus_key , pp_month),2) > 0 then (sum(case when outletdivservice_key = 1137452999 then edited_sales else 0 end) OVER (PARTITION BY corporationus_key , pp_month, outletdivservice_key) / sum(edited_sales) OVER (PARTITION BY corporationus_key , pp_month))*100 else 0 end AS s_o_1137452999,case when round(sum(edited_sales) OVER (PARTITION BY corporationus_key , pp_month),2) > 0 then (sum(case when outletdivservice_key = 1137453014 then edited_sales else 0 end) OVER (PARTITION BY corporationus_key , pp_month, outletdivservice_key) / sum(edited_sales) OVER (PARTITION BY corporationus_key , pp_month))*100 else 0 end AS s_o_1137453014 from prd_us_npd_pos_bea_ust.supprsn_beauty_fact where category_key = 1137289971) main_query GROUP BY corporationus_key , pp_month
HAVING max(rule1_cnt) < 3  OR max(s_o_1137452989) > 80  OR max(s_o_1137452999) > 90  OR max(s_o_1137453014) > 90) supp_result inner join (select distinct corporationus_key , pp_month,category_key, brand_key from prd_us_npd_pos_bea_ust.supprsn_beauty_fact where category_key = 1137289971) cross_dim on supp_result.corporationus_key = cross_dim. corporationus_key  and supp_result.pp_month = cross_dim.pp_month)""").show()
        #recordsdf.write.mode("overwrite").saveAsTable("prd_us_npd_pos_bea_ust.supprsn_bea_pos_ust_corporationus_nc")
        sparkwithhive.sql("SELECT COUNT(*) FROM prd_us_npd_pos_bea_ust.supprsn_bea_pos_ust_corporationus_nc").show()
        #recordsdf.show()
        #recordsdf.("SELECT COUNT(*) FROM prd_us_npd_pos_bea_ust.supprsn_bea_pos_ust_corporationus_nc")
        # Queries can then join DataFrame data with data stored in Hive.
        #sparkwithhive.sql("SELECT * FROM records r JOIN mydb.src s ON r.key = s.key").show()
    except Exception as e:
        print("Unexpected error:", e)
        raise
    finally:
        sparkwithhive.stop()
        print("finally block")


def getsparkwithhive():
    sparkwithhive = SparkSession.builder \
        .master("spark://lnx2517.ch3.prod.i.com:7077") \
        .config("spark.sql.hive.metastore.version", "2.3.9") \
        .config("spark.sql.warehouse.dir", "hdfs://lnx2517.ch3.prod.i.com:9000/user/hive/warehouse") \
        .config("spark.sql.hive.metastore.jars", "/opt/apache-hive-2.3.9-bin/lib/*") \
        .config("spark.sql.catalogImplementation=hive") \
        .enableHiveSupport().getOrCreate()
    return sparkwithhive

def getspark():
    sparkobj: SparkSession = SparkSession.builder \
        .master("spark://lnx2517.ch3.prod.i.com:7077") \
        .getOrCreate()
    return sparkobj
if __name__ == '__main__':
    sparkwithhiveobj = getsparkwithhive()
    sparkwithhiveone()
    #spark = getspark()
    sparkwithhiveobj.stop()