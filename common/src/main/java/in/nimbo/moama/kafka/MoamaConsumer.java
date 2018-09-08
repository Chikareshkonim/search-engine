package in.nimbo.moama.kafka;

import in.nimbo.moama.configmanager.ConfigManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;

public class MoamaConsumer {
    private final KafkaConsumer<String, String> consumer;

    public MoamaConsumer(String topic,String rootAddress) {
        consumer = new KafkaConsumer<>(ConfigManager.getInstance().getProperties(rootAddress,true));
        consumer.subscribe(Collections.singletonList(topic));
    }
    public synchronized ArrayList<String> getDocuments() {
        //TODO
        ArrayList<String> result = new ArrayList<>();
        ConsumerRecords<String, String> records = consumer.poll(10000);
        consumer.commitSync();
        for (ConsumerRecord<String, String> record : records) {
            result.add(record.value());
        }
        return result;
    }
}
