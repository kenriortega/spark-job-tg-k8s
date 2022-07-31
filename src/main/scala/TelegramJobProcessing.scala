import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.Source

object TelegramJobProcessing {

  var cfg: Map[String, String] = Source.fromFile("application.dev.properties").getLines().filter(line => line.contains("=")).map { line =>
    val tokens = line.split("=")
    if (tokens.size == 1) {
      (tokens(0) -> "")
    } else {
      (tokens(0) -> tokens(1))
    }
  }.toMap

  val spark: SparkSession = SparkSession.builder()
    .appName(cfg.getOrElse("name", "spark-job"))
    .config("spark.master", "local[*]")
    .getOrCreate()

  def readTable(tableName: String): DataFrame =
    spark.read
      .format("jdbc")
      .options(Map(
        "driver" -> "org.postgresql.Driver",
        "url" -> cfg.getOrElse("url", "").replace("\"", ""),
        "user" -> cfg.getOrElse("user", "").replace("\"", ""),
        "password" -> cfg.getOrElse("password", "").replace("\"", ""),
      ))
      .option("dbtable", s"public.$tableName")
      .load()

  def saveTable(tableName: String, df: sql.DataFrame, mode: SaveMode = SaveMode.Overwrite): Unit = {

    df
      .write
      .mode(mode)
      .format("jdbc")
      .options(Map(
        "driver" -> "org.postgresql.Driver",
        "url" -> cfg.getOrElse("url", "").replace("\"", ""),
        "user" -> cfg.getOrElse("user", "").replace("\"", ""),
        "password" -> cfg.getOrElse("password", "").replace("\"", ""),
      ))
      .option("dbtable", s"public.$tableName")
      .save()
  }


  case class Message(from_peer_name: String, space_peer_name: String, timestamp_unix: Long, message_content: String)

  case class MessageResult(from_peer_name: String, space_peer_name: String, timestamp_unix: Long, message_content: String, category: String)


  def extract_hashtag(message: String, space_peer_name: String): Option[String] =
    if (space_peer_name == """Ofertas de Empleo ( 🇨🇺 )""") {
      """#.*?(?=\s|$)""".r.findFirstIn(message)
    } else None

  val CUSTOMS_STOP_WORDS: Array[String] = Array[String](
    "interesados", "favor", "contactar", "privado", "inscripción",
    "escribir", " envío", "detalles", ".", ",", "buen", "día", "opciones", "hablamos.",
  )

  def main(args: Array[String]): Unit = {

    val recordsTgDF = readTable("telegram_job")
    val messagesStringDF = recordsTgDF.select("data")
    val schema = StructType(Array(
      StructField("from_peer", StructType(Array(
        StructField("id", LongType),
        StructField("name", StringType),
      ))),
      StructField("timestamp", DateType),
      StructField("message_id", LongType),
      StructField("space_peer", StructType(Array(
        StructField("id", LongType),
        StructField("name", StringType),
      ))),
      StructField("timestamp_unix", LongType),
      StructField("message_content", StringType),
    ))

    // Convert JSON column to multiple columns
    val messagesJsonDF = messagesStringDF.withColumn("data", from_json(col("data"), schema))
      .select("data.*")

    // clean data from messages json df
    val messagesDF = messagesJsonDF.select(
      col("from_peer.name") as "from_peer_name",
      col("space_peer.name") as "space_peer_name",
      col("timestamp_unix"),
      col("message_content"),
    )
      .na.fill("unknown", List("from_peer_name"))

    import spark.implicits._

    val messagesDS = messagesDF.na.fill("unknown").as[Message]
    val messagesResultDF = messagesDS.map(m => MessageResult(
      m.from_peer_name,
      m.space_peer_name,
      m.timestamp_unix,
      m.message_content,
      extract_hashtag(m.message_content, m.space_peer_name).getOrElse("not category")
    )).toDF()

    // ML ops
    val tokenizer = new Tokenizer().setInputCol("message_content").setOutputCol("words")
    val countTokens = udf { (words: Seq[String]) => words.length }
    val tokenized = tokenizer.transform(messagesResultDF)
    tokenized.select("message_content", "words")
      .withColumn("tokens", countTokens(col("words")))

    val stopWordList = StopWordsRemover.loadDefaultStopWords(language = "spanish")
    val remover = new StopWordsRemover()
      .setStopWords(stopWordList ++ CUSTOMS_STOP_WORDS)
      .setInputCol("words")
      .setOutputCol("filtered")
    val resultDF = remover.transform(tokenized)
    // end ML ops

    // Post Processing
    val dfToDB = resultDF.select(
      col("from_peer_name"),
      col("space_peer_name"),
      col("timestamp_unix"),
      col("message_content"),
      col("category"),
      col("filtered")
    )
    saveTable(tableName = "telegram_job_analytics_v2", df = dfToDB)

    val analyticsTgDF = readTable("telegram_job_analytics_v2")
    println(s"Jobs Offers: ${analyticsTgDF.count}")
  }
}
