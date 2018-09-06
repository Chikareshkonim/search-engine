package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.document.Link;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.util.HBasePropertyType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class KeywordFinder implements Runnable{
    private final static Logger LOGGER = Logger.getLogger(KeywordFinder.class);
    private ElasticManager elasticManager;
    private MoamaConsumer consumer;
    private HBaseManager hbaseManager;
    private String keysFamily;
    //TODO set configs
    public KeywordFinder(ConfigManager configManager) {
        elasticManager = new ElasticManager();
        hbaseManager = new HBaseManager("terms",null);
        consumer = new MoamaConsumer("crawled","kafka");
    }

    @Override
    public void run() {
        Map<String, Map<String, Double>> keywords = null;
        try {
            keywords = elasticManager.getTermVector(consumer.getDocuments().toString());
        } catch (IOException e) {
            LOGGER.error("can not get term vector");
            LOGGER.debug("collecting method ", e);
        }
        assert keywords != null;
        hbaseManager.put(createHbasePut(keywords));
    }
    private List<Put> createHbasePut(Map<String, Map<String, Double>> keywords) {
        List<Put> puts= new LinkedList<>();
        keywords.forEach((link ,words) ->{
            Put put = new Put(Bytes.toBytes(hbaseManager.generateRowKeyFromUrl(link)));
            words.forEach((word,score)-> put.addColumn(keysFamily.getBytes(),word.getBytes(),Bytes.toBytes(score)));
            puts.add(put);
        });
        return puts;
    }
}
