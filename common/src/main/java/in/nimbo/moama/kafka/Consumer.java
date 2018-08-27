package in.nimbo.moama.kafka;

import in.nimbo.moama.ConfigManager;
import kafka.utils.Json;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;

public class Consumer {
    private KafkaConsumer consumer;
    private Logger errorLogger = Logger.getLogger("error");

    public Consumer(String topic) {
        //TODO
        ConfigManager configManager = new ConfigManager();
        consumer = new KafkaConsumer<>(configManager.getProperties("",true));
        consumer.subscribe(Collections.singletonList(topic));
    }
    public synchronized ArrayList<String> getUrls() {
        //TODO
        ArrayList<String> result = new ArrayList<>();
        ConsumerRecords<String, String> records = consumer.poll();
        consumer.commitSync();
        for (ConsumerRecord<String, String> record : records) {
            result.add(record.value());
        }
        return result;
    }
    public synchronized ArrayList<Json> getDocuments() {
        //TODO
        ArrayList<Json> result = new ArrayList<>();
        ConsumerRecords<String, Json> records = consumer.poll();
        consumer.commitSync();
        for (ConsumerRecord<String, Json> record : records) {
            result.add(record.value());
        }
        return result;
    }
}
