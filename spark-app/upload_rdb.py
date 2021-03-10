import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os
import sys
spark = (SparkSession
    .builder
    .getOrCreate()
)
postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]


df_session_ts = spark.read.option("header", "true").csv(os.path.join(os.path.dirname(__file__),"raw_data/channels.csv"))
df_session_tx = spark.read.option("header", "true").csv(os.path.join(os.path.dirname(__file__),'raw_data/session_transactions.csv'))
df_channel = spark.read.option("header", "true").csv(os.path.join(os.path.dirname(__file__),'raw_data/session_timestamps.csv'))
df_usc = spark.read.option("header", "true").csv(os.path.join(os.path.dirname(__file__),'raw_data/user_session_channels.csv'))

def write_postgres(df,table_name):
    df.write.format("jdbc").\
        option("url", f"jdbc:postgresql://localhost:5439/{postgres_db}").\
        option("dbtable",table_name).\
        option("user", postgres_user).\
        option("password", postgres_pwd).\
        mode("overwrite").\
        save()

write_postgres(df_session_ts,'session_timestamps')
write_postgres(df_session_tx,'session_transactions')
write_postgres(df_channel,'channels')
write_postgres(df_usc,'user_session_channels')
print('success write')
