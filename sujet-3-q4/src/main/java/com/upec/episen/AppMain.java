package com.upec.episen;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;

import java.io.*;
import java.util.Properties;
import java.util.Scanner;

public class AppMain {

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties");
        properties.load(inputStream);
        SparkConf conf = new SparkConf();
        conf.setAppName("sujet");
        conf.setMaster("local[*]");

        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        session.sparkContext().setLogLevel("INFO");

        while (true) {
            System.out.println("Enter the postal address of the company (or 'exit' to quit): ");
            Scanner scanner = new Scanner(System.in);
            String address = scanner.nextLine();

            if (address.equalsIgnoreCase("exit")) {
                break;
            }

            String query = String.format("SELECT annee, adresse, consommation_annuelle_totale FROM q3 WHERE adresse = '%s'", address);
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

            result.show();

            result.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .option("header", "true")
                    .option("delimiter", ";")
                    .csv("output");
            dfQ4.printSchema();
            FilenameFilter csvFilter = new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".csv");
                }
            };
            File dir = new File("output");
            File[] csvFiles = dir.listFiles(csvFilter);
            String csvFileName = csvFiles[0].getName();
            System.out.println("CSV file: " + csvFileName);
            ChartHelper.createChartFromCSV(csvFileName);
        }

        session.stop();
    }


}
