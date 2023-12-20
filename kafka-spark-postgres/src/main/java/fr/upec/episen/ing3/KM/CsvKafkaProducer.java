package fr.upec.episen.ing3.KM;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

public class CsvKafkaProducer {

    private static String KafkaBrokerEndpoint = "localhost:9092";
    private static String KafkaTopic = "sujet3";
    private static String CsvFile = "data.csv";

    private KafkaProducer<String, String> ProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        CsvKafkaProducer kafkaProducer = new CsvKafkaProducer();
        kafkaProducer.PublishMessages();
        System.out.println("Producing job successfully completed !");
    }

    private void PublishMessages() {
        final Producer<String, String> csvProducer = ProducerProperties();

        try {
            Stream<String> fileStream = new BufferedReader(new InputStreamReader(CsvKafkaProducer.class.getClassLoader().getResourceAsStream(CsvFile)))
                    .lines();
            fileStream.forEach(line -> {
                System.out.println();

                final ProducerRecord<String, String> csvRecord = new ProducerRecord<>(
                        KafkaTopic, UUID.randomUUID().toString(), line);

                csvProducer.send(csvRecord, (metadata, exception) -> {
                    if (metadata != null) {
                        System.out.println("CsvData: -> " + csvRecord.key() + " | " + csvRecord.value());
                    } else {
                        System.out.println("Error Sending Csv Record -> " + csvRecord.value());
                    }
                });
            });
        } finally {
            csvProducer.close();
        }
    }
}
