from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import from_unixtime, col, window, avg, min, max, stddev, sum, when

# Crea l'app SSS (spark structured streaming) per analizzare i dati di bitcoin in tempo reale


def process_bitcoin_stream():
    # Creazione della SparkSession
    # La SparkSession è il punto di ingresso per programmare Spark con l'API DataFrame
    # 'local[2]' esegue Spark localmente con 2 core
    # uno per ricevere i dati e uno per elaborarli
    spark = SparkSession \
        .builder \
        .appName("RealTimeBitcoinAnalysis") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[2]") \
        .getOrCreate()

    # Livello di log a ERROR per avere un output più pulito
    spark.sparkContext.setLogLevel("ERROR")

    print("--- SparkSession creata. In attesa di dati dallo stream... ---")

    # Definizione dello Schema dei Dati
    # Definire uno schema esplicito (best practice per lo streaming - evita inferenza dello schema che rallenta)
    schema = StructType([
        StructField("Timestamp", DoubleType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", DoubleType(), True)
    ])

    # Lettura dello Stream
    # Leggiamo i dati come uno stream dalla cartella specificata
    # Spark monitorerà questa cartella e leggerà i nuovi file CSV man mano che arrivano
    streaming_df = spark \
        .readStream \
        .format("csv") \
        .schema(schema) \
        .option("header", "true") \
        .option("maxFilesPerTrigger", 1) \
        .load("streaming_data")

    # Trasformazione dei Dati
    # Convertiamo il timestamp Unix in un formato di data/ora leggibile -> 'event_time'
    transformed_df = streaming_df.withColumn(
        "event_time",
        from_unixtime(col("Timestamp").cast("long")).cast("timestamp")
    )

    # Calcolo delle Aggregazioni su Finestre Temporali
    # Raggruppiamo i dati in finestre temporali "mobili" (tumbling windows) di 10 minuti
    # Per ogni finestra, calcoliamo il prezzo medio, minimo e massimo di chiusura

    # Il watermark di 5 minuti dice a Spark di attendere 5 min per i dati in ritardo
    # prima di finalizzare i calcoli per una finestra
    windowed_agg_df = transformed_df \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(
            window(col("event_time"), "10 minutes")
        ) \
        .agg(
            avg("Close").alias("avg_price"),
            min("Close").alias("min_price"),
            max("Close").alias("max_price"),
            stddev("Close").alias("volatility"),
            sum("Volume").alias("total_volume")
        )

    # Logica di Alerting
    # Creiamo una nuova colonna 'alert' basata su una regola
    # Se il prezzo massimo è maggiore della media + 2 volte la deviazione standard,
    # segnaliamo un'anomalia, else lo stato è 'Nominal'
    alerts_df = windowed_agg_df.na.fill(0, subset=['volatility']).withColumn(
        "alert",
        when(
            col("max_price") > (col("avg_price") * 1.02),
            "Spike di Prezzo > 2%"
        ).otherwise("Nominal")
    )

    # Scrittura degli Stream di Output su Parquet

    # Sink 1: Scrive tutti i dati aggregati su file Parquet
    # Usiamo la modalità 'append' che è standard per i sink su file
    # 'checkpointLocation' serve per la fault-tolerance dello stream
    query_data = alerts_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "results_data") \
        .option("checkpointLocation", "checkpoints/data_checkpoint") \
        .trigger(processingTime='3 seconds') \
        .start()

    # Sink 2: Filtra solo gli alert e li scrive in una cartella separata
    query_alerts = alerts_df \
        .filter(col("alert") != "Nominal") \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "results_alerts") \
        .option("checkpointLocation", "checkpoints/alerts_checkpoint") \
        .trigger(processingTime='3 seconds') \
        .start()

    print("--- Query avviate. Salvataggio dei risultati in 'results_data/' e degli alert in 'results_alerts/'. ---")

    # Attesa della Terminazione
    # L'applicazione attenderà qui la terminazione di una delle query
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    process_bitcoin_stream()
