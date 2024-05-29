package sda.traitement

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter()= {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

    def calculTTC () : DataFrame ={
      val ttcDataFrame = dataFrame
        .withColumn("TTC", round(col("HTT") + col("TVA") * col("HTT"), 2))
        .drop("TVA", "HTT")
      ttcDataFrame
    }

    def extractDateEndContratVille(): DataFrame = {
      val schema_MetaTransaction = new StructType()
        .add("Ville", StringType, nullable=false)
        .add("Date_End_contrat", StringType, nullable= false)
      val schema = new StructType()
        .add("MetaTransaction", ArrayType(schema_MetaTransaction), nullable = true)
      dataFrame.select(
        from_json(col("metaData"), schema).getField("Date_End_contrat").as("Date_End_contrat"),
        from_json(col("metaData"), schema).getField("Ville").as("Ville")
      )

    }

    def contratStatus(): DataFrame = {
      val currentDate = current_date()
      val dateEndContrat = to_date(col("Date_End_contrat"))
      val statusDataFrame = dataFrame.withColumn("Contrat_Status",
        when(dateEndContrat < currentDate, "Expired")
          .otherwise("Actif")
      )
      statusDataFrame
    }


  }

}
