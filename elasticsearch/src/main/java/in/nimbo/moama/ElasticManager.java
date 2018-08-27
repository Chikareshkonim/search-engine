package in.nimbo.moama;

import in.nimbo.moama.metrics.Metrics;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;

import static in.nimbo.moama.util.Constants.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class ElasticManager {
    public static final String HOSTNAME = "94.23.214.93";
    public static final int PORT = 9200;
    public static final String HTTP = "http";
    private RestHighLevelClient client;
    private String index = "pages";
    private Logger errorLogger = Logger.getLogger("error");
    private IndexRequest indexRequest;
    private BulkRequest bulkRequest;
    private static int added = 0;
    private static final Integer sync = 0;
    private static final int ELASTIC_FLUSH_SIZE_LIMIT = 2;
    private static final int ELASTIC_FLUSH_NUMBER_LIMIT = 193;

    public ElasticManager() {

        client = new RestHighLevelClient(RestClient.builder(new HttpHost(ELASTIC_HOSTNAME, ELASTIC_PORT, HTTP)));
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
    public void put(WebDocument document) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            try {
                builder.startObject();
                {
                    builder.field("pageLink", document.getPagelink());
                    builder.field("pageText", document.getTextDoc());
                }

                builder.endObject();
                bulkRequest.add(indexRequest);
                indexRequest = new IndexRequest(index);
                added++;
            } catch (IOException e) {
                errorLogger.error("ERROR! couldn't add " + document.getPagelink() + " to elastic");
            }
            if (bulkRequest.estimatedSizeInBytes() / 1000000 >= ELASTIC_FLUSH_SIZE_LIMIT ||
                    bulkRequest.numberOfActions() >= ELASTIC_FLUSH_NUMBER_LIMIT) {
                synchronized (sync) {
                    client.bulk(bulkRequest);
                    bulkRequest = new BulkRequest();
                    Metrics.numberOfPagesAddedToElastic = added;
                }
            }
        } catch (IOException e) {
            errorLogger.error("ERROR! Couldn't add the document for " + document.getPagelink());
        }
    }
    public Map<String, Float> search(ArrayList<String> necessaryWords, ArrayList<String> preferredWords, ArrayList<String> forbiddenWords) {
        Map<String, Float> results = new HashMap<>();
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types("_doc");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for(String necessaryWord:necessaryWords) {
            boolQueryBuilder.must(QueryBuilders.matchQuery("pageLink", necessaryWord));
        }
        for(String preferredWord:preferredWords) {
            boolQueryBuilder.should(QueryBuilders.matchQuery("pageText", preferredWord));
        }
        for(String forbiddenWord:forbiddenWords) {
            boolQueryBuilder.mustNot(QueryBuilders.matchQuery("pageText", forbiddenWord));
        }
        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(20);
        sourceBuilder.timeout(new TimeValue(5, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = runSearch(searchRequest);
        SearchHit[] hits = searchResponse.getHits().getHits();
        int i = 1;
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            results.put((String) sourceAsMap.get("pageLink"), hit.getScore());
        }
        return SortResults.sortByValues(results);
    }

    public Map<String, Float> findSimilar(String text) {
        Map<String, Float> results = new HashMap<>();
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String[] fields = {"pageText"};
        String[] texts = {text};
        searchSourceBuilder.query(QueryBuilders.moreLikeThisQuery(fields, texts, null).minTermFreq(1));
        searchSourceBuilder.size(20);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = runSearch(searchRequest);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            results.put((String) sourceAsMap.get("pageLink"), hit.getScore());
        }
        return SortResults.sortByValues(results);
    }

    private SearchResponse runSearch(SearchRequest searchRequest){
        boolean searchStatus = false;
        SearchResponse searchResponse = new SearchResponse();
        while (!searchStatus) {
            try {
                searchResponse = client.search(searchRequest);
                searchStatus = true;
            } catch (IOException e) {
                System.out.println("Elastic connection timed out! Trying again...");
                searchStatus = false;
            }
        }
        return searchResponse;
    }

}
