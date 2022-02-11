from pyspark.sql import SparkSession
from pyspark.sql.functions import when,lit,col
import pyspark.sql.functions as F
from pyspark.sql import Window

data = [["P1", 1, "p1_col_1", "p1_col_tgt_1", "string", 1, 1],
        ["P2", 1, "p2_col_1", "p2_col_tgt_1", "int", 1, 1],
        ["P2", 2, "p2_col_2", "p2_col_tgt_2", "string", 2, 2],
        ["P1", 2, "p1_col_2", "p1_col_tgt_2", "double", 2, 2],
        ["P3", 1, "p3_col_1", "p3_col_tgt_1", "decimal", 1, 1],
        ["P3", 3, "p3_col_3", "p3_col_tgt_3", "boolean", 3, 3],
        ["P3", 2, "p3_col_2", "p3_col_tgt_2", "string", 2, 2]
      ]

col_names = ["p_id", "seq", "src_col", "tgt_col", "dt_type", "dt_len", "dt_prec"]

spark = SparkSession.builder.appName("Tst").getOrCreate()
w = Window.partitionBy("p_id").orderBy("seq")
df = spark.createDataFrame(data, col_names)
base = df.withColumn("o", F.row_number().over(w))

# Source Columns
src_cols = base.groupBy("p_id").agg(F.sort_array(F.collect_list(F.struct("o", "src_col")))\
.alias("collected_list")) \
.withColumn("src_col_lst",col("collected_list.src_col")) \
.drop("collected_list")\
.select(F.col("p_id").alias("P_id_sc"), F.concat_ws("|", "src_col_lst").alias("src_cols"))

# Target Columns
trgt_cols = base.groupBy("p_id").agg(F.sort_array(F.collect_list(F.struct("o", "tgt_col")))\
.alias("collected_list")) \
.withColumn("tgt_col_lst",col("collected_list.tgt_col")) \
.drop("collected_list")\
.select(F.col("p_id").alias("P_id_trgt"), F.concat_ws("|", "tgt_col_lst").alias("tgt_cols"))

# Data Types
trgt_dt_types = base.groupBy("p_id").agg(F.sort_array(F.collect_list(F.struct("o", "dt_type")))\
.alias("collected_list")) \
.withColumn("dt_type_col_lst",col("collected_list.dt_type")) \
.drop("collected_list")\
.select(F.col("p_id").alias("P_id_dtype"), F.concat_ws("|", "dt_type_col_lst").alias("target_dt"))

# DT Length
dt_len = base.groupBy("p_id").agg(F.sort_array(F.collect_list(F.struct("o", "dt_len")))\
.alias("collected_list")) \
.withColumn("dt_len_col_lst", col("collected_list.dt_len")) \
.drop("collected_list")\
.select(F.col("p_id").alias("P_id_dt_len"), F.concat_ws("|", "dt_len_col_lst").alias("target_dt_len"))

dt_precision = base.groupBy("p_id").agg(F.sort_array(F.collect_list(F.struct("o", "dt_prec")))\
.alias("collected_list")) \
.withColumn("dt_prec_col_lst", col("collected_list.dt_prec")) \
.drop("collected_list")\
.select(F.col("p_id").alias("P_id_dt_prec"), F.concat_ws("|", "dt_prec_col_lst").alias("target_dt_precision"))

# DT Precision
src_cols.show(20, False)
trgt_cols.show(20, False)
trgt_dt_types.show(20, False)
dt_len.show(20, False)
dt_precision.show(20, False)

cols_join_df = src_cols.join(trgt_cols, src_cols.P_id_sc == trgt_cols.P_id_trgt, "inner")\
                       .join(trgt_dt_types, trgt_cols.P_id_trgt == trgt_dt_types.P_id_dtype, "inner")\
                       .join(dt_len, trgt_dt_types.P_id_dtype == dt_len.P_id_dt_len, "inner")\
                       .join(dt_precision, dt_len.P_id_dt_len == dt_precision.P_id_dt_prec, "inner")\
                       .drop('P_id_trgt', 'P_id_dtype', 'P_id_dt_len', 'P_id_dt_prec') \
                       .withColumnRenamed('P_id_sc', 'pid_cols')

cols_join_df.show(20, False)




























