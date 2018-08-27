package in.nimbo.moama.database;

import com.fasterxml.jackson.databind.JsonNode;
import in.nimbo.moama.database.webdocumet.WebDocument;
import in.nimbo.moama.metrics.Metrics;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.common.Strings;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class ElasticWebDaoImp implements WebDao {
    public static final String HOSTNAME = "94.23.214.93";
    public static final int PORT = 9200;
    public static final String HTTP = "http";
    private String index = "pages";
    private Logger errorLogger = Logger.getLogger("error");
    private IndexRequest indexRequest;
    private BulkRequest bulkRequest;
    private static int added = 0;
    private static final Integer sync = 0;
    private static final int ELASTIC_FLUSH_SIZE_LIMIT = 2;
    private static final int ELASTIC_FLUSH_NUMBER_LIMIT = 193;

    public ElasticWebDaoImp() {
        indexRequest = new IndexRequest(index);
        bulkRequest = new BulkRequest();
    }
    //TODO
    public void getTermVector() {
        Settings settings = Settings.builder()
                .put("cluster.name", "moama").put("client.transport.sniff", true).build();
        TransportClient termVectorClient = null;
        try {
            termVectorClient = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("s1"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        TermVectorsRequest termVectorsRequest = new TermVectorsRequest("test","_doc","1");
        termVectorsRequest.fieldStatistics(true);
        termVectorsRequest.termStatistics(true);
        assert termVectorClient != null;
        TermVectorsResponse termVectorsResponse = termVectorClient.termVectors(termVectorsRequest).actionGet();
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            termVectorsResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String data = Strings.toString(builder);
            JSONObject json = new JSONObject(data);
            System.out.println(json);
        } catch (IOException e) {
          //TODO
        }
    }

    @Override
    public boolean createTable() {
        return false;
    }

    @Override
    public void put(WebDocument document) {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME, PORT, HTTP)));
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            try {
                builder.startObject();
                {
                    builder.field("pageLink", document.getPagelink());
                    builder.field("pageText", document.getTextDoc());
                }

                builder.endObject();
                added++;
            } catch (IOException e) {
                errorLogger.error("ERROR! couldn't add " + document.getPagelink() + " to elastic");
            }
            if (bulkRequest.estimatedSizeInBytes() / 1000000 >= ELASTIC_FLUSH_SIZE_LIMIT ||
                    bulkRequest.numberOfActions() >= ELASTIC_FLUSH_NUMBER_LIMIT) {
                synchronized (sync) {
                    bulkRequest.add(indexRequest);
                    client.bulk(bulkRequest);
                    indexRequest = new IndexRequest(index);
                    bulkRequest = new BulkRequest();
                    Metrics.numberOfPagesAddedToElastic = added;
                }
            }
        } catch (IOException e) {
            errorLogger.error("ERROR! Couldn't add the document for " + document.getPagelink());
        }
    }

}
