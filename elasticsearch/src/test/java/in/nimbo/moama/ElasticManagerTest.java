package in.nimbo.moama;

import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.metrics.Metrics;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import static org.junit.Assert.*;

public class ElasticManagerTest {
    private RestHighLevelClient client;
    private IndexRequest indexRequest;
    private BulkRequest bulkRequest;

    @Test
    public void testElastic(){
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("46.4.120.138", 9200, "http")));
        indexRequest = new IndexRequest("newspages","_doc");
        bulkRequest = new BulkRequest();
        WebDocument documentTest = new WebDocument();
        documentTest.setPageLink("http://b.com");
        documentTest.setTextDoc("this is b");
        documentTest.setTitle("woww");
        documentTest.setLinks(new ArrayList<>());
        JSONObject document = new JSONObject();
        document.put("url","f.com");
        document.put("content","");
        document.put("title","yes");
        document.put("date","2015-01-01");
        System.out.println(document);
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            try {
                builder.startObject();
                {
                    Set<String> keys = document.keySet();
                    keys.forEach(key -> {
                        try {
                            if (!key.equals("outLinks")) {
                                builder.field(key, document.get(key));
                                System.out.println(key);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                            System.out.println("ERROR");                        }
                    });
                }
                System.out.println(builder.toString());
                builder.endObject();
                indexRequest.source(builder);
                bulkRequest.add(indexRequest);
                indexRequest = new IndexRequest("test","_doc");
                if (bulkRequest.estimatedSizeInBytes() >= 1 ||
                        bulkRequest.numberOfActions() >= 1) {
                        System.out.println(bulkRequest.numberOfActions());
                        client.bulk(bulkRequest);
                        bulkRequest = new BulkRequest();
                        System.out.println("added                     ");
//                    jmxManager.markNewAddedToElastic();
                    }
                } catch (IOException e) {
                System.out.println("ERROR");            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void putTest(){
        ElasticManager elasticManager = new ElasticManager();
        JSONObject document = new JSONObject();
        document.put("url","me.com");
        document.put("content","gdsfghshgssjsjfsjsfj");
        document.put("title","yes");
        document.put("date","2015-01-01");
        elasticManager.put(document,null);
    }

}