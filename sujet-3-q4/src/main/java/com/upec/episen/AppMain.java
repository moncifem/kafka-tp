package com.upec.episen;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppMain {

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties");
        properties.load(inputStream);
        SparkConf conf = new SparkConf();
        conf.setAppName("sujet");
        conf.setMaster("local[*]");

        // Créez un SparkSession
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        session.sparkContext().setLogLevel("INFO");

        while (true) {
            // Récupérez l'adresse postale depuis le terminal
            System.out.println("Entrez l'adresse postale de l'entreprise (ou 'exit' pour quitter) : ");
            java.util.Scanner scanner = new java.util.Scanner(System.in);
            String adresse = scanner.nextLine();

            if (adresse.equalsIgnoreCase("exit")) {
                // Quittez la boucle si l'utilisateur entre "exit"
                break;
            }

            // Utilisez Spark SQL pour exécuter la requête
            String query =
                    String.format("SELECT annee, consommation_annuelle_totale FROM q3 WHERE adresse = '%s'", adresse);
            Dataset<Row> dfQ4 = session.read()
                    .format("jdbc")
                    .option("url", properties.getProperty("database.url"))
                    .option("user", properties.getProperty("database.username"))
                    .option("password", properties.getProperty("database.password"))
                    .option("dbtable", "q3")
                    .option("driver", properties.getProperty("database.driver"))
                    .load();
            dfQ4.createOrReplaceTempView("q3");
            Dataset<Row> result = session.sql(query);

            // Affichez les résultats dans la console
            result.show();
        }

        // Arrêtez la session Spark
        session.stop();
    }
}
