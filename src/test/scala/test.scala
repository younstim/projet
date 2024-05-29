import org.apache.spark.sql.SparkSession
import sda.reader.JsonReader
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Nom_de_ton_application")
      .master("local[*]")
      .getOrCreate()

    val jsonReader = JsonReader("src/main/resources/DataforTest/data.json")
    val dataframe = jsonReader.read()(spark)

    // Faire des opérations supplémentaires sur le DataFrame ici
    dataframe.show()

    // Arrêter la session Spark lorsque tu as fini
    spark.stop()
  }
}

