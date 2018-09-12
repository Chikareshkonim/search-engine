package in.nimbo.moama.keywords;

import in.nimbo.moama.HBaseManager;
import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.elasticsearch.ElasticManager;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.util.KeywordPropertyType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class KeywordFinder implements Runnable {
    private final static Logger LOGGER = LogManager.getLogger(KeywordFinder.class);
    private ElasticManager elasticManager;
    private MoamaConsumer consumer;
    private HBaseManager hbaseManager;
    private String keysFamily;
    private String tableName;
    private String crawledTopic;

    //TODO set configs
    public KeywordFinder() {
        keysFamily = ConfigManager.getInstance().getProperty(KeywordPropertyType.HBASE_FAMILY);
        tableName = ConfigManager.getInstance().getProperty(KeywordPropertyType.HBASE_TABLE);
        crawledTopic = ConfigManager.getInstance().getProperty(KeywordPropertyType.CRAWLED_TOPIC);
        elasticManager = new ElasticManager();
        hbaseManager = new HBaseManager(tableName, null);
        consumer = new MoamaConsumer(crawledTopic, "kafka.keywords.");
    }

    @Override
    public void run() {
        Map<String, Map<String, Double>> keywords = null;
        try {
            ArrayList<String> urls = consumer.getDocuments();
            if (!urls.isEmpty())
                keywords = elasticManager.getTermVector(urls, "pages");
            assert keywords != null;
            if (!keywords.values().isEmpty())
                hbaseManager.put(createHBasePut(keywords));
        } catch (Exception e) {
            LOGGER.error("can not get term vector", e);
        }
    }

    private List<Put> createHBasePut(Map<String, Map<String, Double>> keywords) {
        List<Put> puts = new LinkedList<>();
        keywords.forEach((linkHash, words) -> {
            if (!words.isEmpty()) {
                Put put = new Put(Bytes.toBytes(linkHash));
                words.forEach((word, score) -> put.addColumn(keysFamily.getBytes(), word.getBytes(), Bytes.toBytes(score)));
                puts.add(put);
            }
        });
        return puts;
    }
}
