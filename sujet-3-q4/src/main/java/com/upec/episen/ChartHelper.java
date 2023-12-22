package com.upec.episen;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.chart.plot.PlotOrientation;

import java.io.*;
import java.nio.file.Paths;

public class ChartHelper {

    /*public static void main(String[] args) {
        // Call the method with the path to your CSV file
        createChartFromCSV("file.csv");
    }
     */

    public static void createChartFromCSV(String csvFilePath) {
        String line;
        String cvsSplitBy = ";";
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        // Read data from CSV file
        try (BufferedReader br = new BufferedReader(new FileReader("output/" + csvFilePath))) {

            while ((line = br.readLine()) != null) {
                if (line.startsWith("annee")) continue;
                String[] data = line.split(cvsSplitBy);
                int year = Integer.parseInt(data[0]);
                double consumption = Double.parseDouble(data[2]);
                dataset.addValue(consumption, "Consommation Annuelle Totale", Integer.toString(year));
            }

        } catch (IOException e) {
            e.printStackTrace();
            return; // Exit the method if an error occurs
        }

        // Create and save the chart
        createAndSaveChart(dataset);
    }

    private static void createAndSaveChart(DefaultCategoryDataset dataset) {
        JFreeChart lineChart = ChartFactory.createLineChart(
                "Energy Consumption Chart",
                "Year", "Consumption (Total)",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);

        try {
            ChartUtils.saveChartAsPNG(Paths.get("output/EnergyConsumptionChart.png").toFile(), lineChart, 800, 600);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
