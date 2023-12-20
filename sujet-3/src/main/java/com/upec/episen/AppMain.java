package com.upec.episen;

import java.io.FileWriter;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Function1;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class AppMain {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties");
        properties.load(inputStream);

        SparkConf conf = new SparkConf();
        conf.setAppName("sujet");
        conf.setMaster("local[*]");

        // creer un SparkSession
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        session.sparkContext().setLogLevel("INFO");
/*
    // Cette partie permet de s'abonner au topic sujet3 qui recoit les donnée csv
    // et les stoques dans une table sujet
    // decommentez cette partie dans une 1ere utilisation
       Dataset<Row> df = session.read().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "sujet3")
                .option("encoding", "UTF-8").load();
        // Séparer les données par le séparateur ';'
        Dataset<Row> transformedDf = df.selectExpr("CAST(value AS STRING)")
                .select(
                        split(col("value"), ";").as("data"))
                .select(
                        monotonically_increasing_id().as("id"),
                        col("data").getItem(0).as("annee"),
                        col("data").getItem(1).as("code_iris"),
                        col("data").getItem(2).as("nom_iris"),
                        col("data").getItem(3).as("numero_de_voie"),
                        col("data").getItem(4).as("indice_de_repetition"),
                        col("data").getItem(5).as("type_de_voie"),
                        col("data").getItem(6).as("libelle_de_voie"),
                        col("data").getItem(7).as("code_commune"),
                        col("data").getItem(8).as("nom_commune"),
                        col("data").getItem(9).as("code_grand_secteur"),
                        col("data").getItem(10).as("code_categorie_consommation"),
                        col("data").getItem(11).as("nombre_de_sites"),
                        col("data").getItem(12).as("consommation_annuelle_totale_de_l_adresse_mwh"),
                        col("data").getItem(13).as("code_secteur_naf2"),
                        col("data").getItem(14).as("adresse"),
                        col("data").getItem(15).as("tri_des_adresses"),
                        col("data").getItem(16).as("dcp"),
                        col("data").getItem(17).as("code_epci"),
                        col("data").getItem(18).as("code_departement"),
                        col("data").getItem(19).as("code_region")
                )
                .filter(col("annee").cast("int").isNotNull());
        // enregistrer les données dans la table
        transformedDf.write()
                .format("jdbc")
                .option("url", properties.getProperty("database.url"))
                .option("user", properties.getProperty("database.username"))
                .option("password", properties.getProperty("database.password"))
                .option("dbtable", properties.getProperty("database.dbtable"))
                .option("driver", properties.getProperty("database.driver"))
                .mode(SaveMode.Overwrite)
                .save();
           transformedDf.createOrReplaceTempView("sujet");
*/
        // dans le cas ou la premiere partie est commenter, on doit creer une vue
        // sinon on recoit l'erreur no table sujet found
        Dataset<Row> df2 = session.read()
                .format("jdbc")
                .option("url", properties.getProperty("database.url"))
                .option("user", properties.getProperty("database.username"))
                .option("password", properties.getProperty("database.password"))
                .option("dbtable", "sujet")
                .option("driver", properties.getProperty("database.driver"))
                .load();
        df2.createOrReplaceTempView("sujet");
        Dataset<Row> dfQ3 = session.read()
                .format("jdbc")
                .option("url", properties.getProperty("database.url"))
                .option("user", properties.getProperty("database.username"))
                .option("password", properties.getProperty("database.password"))
                .option("dbtable", "q3")
                .option("driver", properties.getProperty("database.driver"))
                .load();
        dfQ3.createOrReplaceTempView("q3");
        dfQ3.show(0);
        final String check = properties.getProperty("database.check");
        Dataset<Row> resultCheck = session.sql(check);
        resultCheck.show(0);
        // Q3 check s'il y a des données dans q3
        if (resultCheck.count() == 0) {
            Logger.getLogger(AppMain.class).info("No data found in Q3");
        // Q3
        final String q3 = properties.getProperty("database.Q3");
        Dataset<Row> resultQ3 = session.sql(q3);
        resultQ3.show(0);
        resultQ3.show(false);
        resultQ3.write()
                .format("jdbc")
                .option("url", properties.getProperty("database.url"))
                .option("user", properties.getProperty("database.username"))
                .option("password", properties.getProperty("database.password"))
                .option("dbtable", "q3")
                .option("driver", properties.getProperty("database.driver"))
                .mode(SaveMode.Overwrite)
                .save();
        resultQ3.createOrReplaceTempView("q3");
        }
        final String count = properties.getProperty("database.count");
        Dataset<Row> resultCount = session.sql(count);
        resultCount.show(0);
        // Q3 check s'il y a des données dans q3
        String countAsString = String.valueOf(resultCount.first().get(0));
        Logger.getLogger(AppMain.class).info("Count from query: " + countAsString);
    }
}