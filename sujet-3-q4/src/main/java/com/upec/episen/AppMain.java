package com.upec.episen;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Collections;

public class AppMain {

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties");
        properties.load(inputStream);
        SparkConf conf = new SparkConf();
        conf.setAppName("sujet");
        conf.setMaster("local[*]");

        // Create a SparkSession
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        session.sparkContext().setLogLevel("INFO");

        while (true) {
            // Get the postal address from the terminal
            System.out.println("Enter the postal address of the company (or 'exit' to quit): ");
            java.util.Scanner scanner = new java.util.Scanner(System.in);
            String address = scanner.nextLine();

            if (address.equalsIgnoreCase("exit")) {
                // Exit the loop if the user enters "exit"
                break;
            }

            // Use Spark SQL to execute the query
            String query =
                    String.format("SELECT annee, adresse, consommation_annuelle_totale FROM q3 WHERE adresse = '%s'", address);
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

            // Show the results in the console
            result.show();

            // Save the results to a single CSV file
            result
                    .coalesce(1) // Reduce the number of partitions to 1
                    .write()
                    .mode("overwrite") // Specify the overwrite mode
                    .option("header", "true") // Include the header in the output
                    .option("delimiter", ";") // Specify the delimiter as a semicolon
                    .csv("output.csv"); // Specify the output directory
        }

        // Stop the Spark session
        session.stop();
    }
}
