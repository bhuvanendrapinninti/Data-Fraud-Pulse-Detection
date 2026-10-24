import os
from pyspark.sql import SparkSession, functions as F, types as T

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
CHECKPOINT = os.getenv("CHECKPOINT_LOCATION", "/checkpoints/fraud-stream")
THRESHOLD = float(os.getenv("FRAUD_AMOUNT_THRESHOLD", "50000"))

schema = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("merchant_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_time", T.StringType())
])

def write_batch(batch_df, batch_id):
    # Upsert by transaction_id: retrying a micro-batch cannot create duplicate rows.
    from psycopg import connect
    rows = batch_df.collect()
    if not rows:
        return
    upsert = """
      INSERT INTO transactions
      (transaction_id,customer_id,amount,currency,merchant_id,event_type,event_time,
       is_suspicious,risk_reasons,source_topic,source_partition,source_offset)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
      ON CONFLICT (transaction_id) DO UPDATE SET
       is_suspicious=EXCLUDED.is_suspicious,
       risk_reasons=EXCLUDED.risk_reasons,
       source_topic=EXCLUDED.source_topic,
       source_partition=EXCLUDED.source_partition,
       source_offset=EXCLUDED.source_offset
    """
    alert = """INSERT INTO fraud_alerts(transaction_id,customer_id,amount,event_time,reasons)
      VALUES (%s,%s,%s,%s,%s) ON CONFLICT(transaction_id) DO NOTHING"""
    with connect(host=os.getenv("DB_HOST","postgres"), port=int(os.getenv("DB_PORT","5432")),
        dbname=os.getenv("DB_NAME","frauddb"), user=os.getenv("DB_USER","frauduser"),
        password=os.getenv("DB_PASSWORD","fraudpass"), connect_timeout=10) as conn:
        with conn.cursor() as cur:
            for r in rows:
                d = r.asDict()
                values = (d["transaction_id"], d["customer_id"], d["amount"],
                    d.get("currency") or "INR", d["merchant_id"], d.get("event_type") or "payment",
                    d["event_time"], d["is_suspicious"], d["risk_reasons"],
                    d["source_topic"], d["source_partition"], d["source_offset"])
                cur.execute(upsert, values)
                if d["is_suspicious"]:
                    cur.execute(alert, (d["transaction_id"], d["customer_id"], d["amount"],
                                        d["event_time"], d["risk_reasons"]))

def main():
    spark = SparkSession.builder.appName("DataPulseFraudStream").config(
        "spark.sql.shuffle.partitions","2").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    raw = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false").load())
    parsed = (raw.select(F.col("value").cast("string").alias("json"),
                         F.col("topic").alias("source_topic"),
                         F.col("partition").alias("source_partition"),
                         F.col("offset").alias("source_offset"))
        .select(F.from_json("json", schema).alias("e"),"source_topic","source_partition","source_offset")
        .select("e.*","source_topic","source_partition","source_offset")
        .withColumn("event_time",F.to_timestamp("event_time"))
        .withColumn("amount",F.col("amount").cast("double"))
        .filter(F.col("transaction_id").isNotNull() & F.col("customer_id").isNotNull()
                & F.col("merchant_id").isNotNull() & F.col("event_time").isNotNull()
                & F.col("amount").isNotNull() & (F.col("amount") >= 0)))
    enriched = (parsed.withWatermark("event_time","2 minutes")
        .dropDuplicates(["transaction_id"])
        .withColumn("is_suspicious",F.col("amount") >= F.lit(THRESHOLD))
        .withColumn("risk_reasons",F.when(F.col("amount") >= F.lit(THRESHOLD),
            F.array(F.lit("HIGH_AMOUNT"))).otherwise(F.array().cast("array<string>")))
        .select("transaction_id","customer_id","amount","currency","merchant_id","event_type",
            "event_time","is_suspicious","risk_reasons","source_topic","source_partition","source_offset"))
    query = (enriched.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation",CHECKPOINT)
        .trigger(processingTime="5 seconds").start())
    query.awaitTermination()

if __name__ == "__main__":
    main()
