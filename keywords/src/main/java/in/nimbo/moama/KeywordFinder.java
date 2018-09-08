package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.kafka.MoamaConsumer;
import in.nimbo.moama.util.KeywordPropertyType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.elasticsearch.common.recycler.Recycler;

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
    private String tableName;
    private String crawledTopic;
    //TODO set configs
    public KeywordFinder() {
        keysFamily = ConfigManager.getInstance().getProperty(KeywordPropertyType.HBASE_FAMILY);
        tableName = ConfigManager.getInstance().getProperty(KeywordPropertyType.HBASE_TABLE);
        crawledTopic = ConfigManager.getInstance().getProperty(KeywordPropertyType.CRAWLED_TOPIC);
        elasticManager = new ElasticManager();
        hbaseManager = new HBaseManager(tableName,null);
        consumer = new MoamaConsumer(crawledTopic,"kafka.");
    }

    @Override
    public void run() {
        Map<String, Map<String, Double>> keywords = null;
        try {
            keywords = elasticManager.getTermVector(consumer.getDocuments());
        } catch (IOException e) {
            LOGGER.error("can not get term vector",e);
        }
        assert keywords != null;
        hbaseManager.put(createHBasePut(keywords));
    }
    private List<Put> createHBasePut(Map<String, Map<String, Double>> keywords) {
        List<Put> puts= new LinkedList<>();
        keywords.forEach((link ,words) ->{
            Put put = new Put(Bytes.toBytes(hbaseManager.generateRowKeyFromUrl(link)));
            words.forEach((word,score)-> put.addColumn(keysFamily.getBytes(),word.getBytes(),Bytes.toBytes(score)));
            puts.add(put);
        });
        return puts;
    }
}
