package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.document.WebDocument;
import in.nimbo.moama.metrics.Metrics;
import in.nimbo.moama.util.PropertyType;
import kafka.utils.json.JsonObject;
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
import org.elasticsearch.common.collect.Tuple;
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
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static in.nimbo.moama.configmanager.ConfigManager.FileType.PROPERTIES;


public class ElasticManager {
    private RestHighLevelClient client;
    private String index;
    private String test;
    private Logger errorLogger = Logger.getLogger("error");
    private IndexRequest indexRequest;
    private BulkRequest bulkRequest;
    private static int added = 0;
    private static final Integer sync = 0;
    private static int elasticFlushSizeLimit = 0;
    private static int elasticFlushNumberLimit = 0;
    private static String textColumn;
    private static String linkColumn;
    private static String server1;
    private static String server2;
    private static String server3;
    private static String clientPort;
    private static String vectorPort;
    private static String clusterName;
    private ConfigManager configManager;

    public ElasticManager() {
        try {
            configManager = new ConfigManager(new File(getClass().getClassLoader().getResource("config.properties").getFile()).getAbsolutePath(), PROPERTIES);
        } catch (IOException e) {
            errorLogger.error("Loading properties failed");
        }
        elasticFlushSizeLimit = Integer.parseInt(configManager.getProperty(PropertyType.ELASTIC_FLUSH_SIZE_LIMIT));
        elasticFlushNumberLimit = Integer.parseInt(configManager.getProperty(PropertyType.ELASTIC_FLUSH_NUMBER_LIMIT));
        index = configManager.getProperty(PropertyType.ELASTIC_PAGES_TABLE);
        test = configManager.getProperty(PropertyType.ELASTIC_TEST_TABLE);
        textColumn = configManager.getProperty(PropertyType.Text_COLUMN);
        linkColumn = configManager.getProperty(PropertyType.LINK_COLUMN);
        server1=configManager.getProperty(PropertyType.SERVER_1);
        server2=configManager.getProperty(PropertyType.SERVER_2);
        server3=configManager.getProperty(PropertyType.SERVER_3);
        clientPort = configManager.getProperty(PropertyType.CLIENT_PORT);
        vectorPort = configManager.getProperty(PropertyType.VECTOR_PORT);
        clusterName = configManager.getProperty(PropertyType.CLUSTER_NAME);
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(server1, Integer.parseInt(clientPort), "http"),
                new HttpHost(server2, Integer.parseInt(clientPort), "http"),
                new HttpHost(server3, Integer.parseInt(clientPort), "http")));
        indexRequest = new IndexRequest(index);
        bulkRequest = new BulkRequest();
    }
    //TODO
    public void getTermVector() {
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName).put("client.transport.sniff", true).build();
        TransportClient termVectorClient = null;
        try {
            termVectorClient = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(server1), Integer.parseInt(vectorPort)));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        TermVectorsRequest termVectorsRequest = new TermVectorsRequest(test,"_doc","1");
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
    public void put(JSONObject document) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                {
                    Set<String>keys = document.keySet();
                    keys.forEach(key->{
                        try {
                            builder.field(key, document.get(key));
                        } catch (IOException e) {
                            errorLogger.error("ERROR! couldn't add " + document.get(key) + " to elastic");
                        }
                    });
                }

                builder.endObject();
                bulkRequest.add(indexRequest);
                indexRequest = new IndexRequest(index);
                added++;
            if (bulkRequest.estimatedSizeInBytes() / 1000000 >= elasticFlushSizeLimit ||
                    bulkRequest.numberOfActions() >= elasticFlushNumberLimit) {
                synchronized (sync) {
                    client.bulk(bulkRequest);
                    bulkRequest = new BulkRequest();
                    Metrics.numberOfPagesAddedToElastic = added;
                }
            }
        } catch (IOException e) {
            errorLogger.error("ERROR! Couldn't add the document for " + document.get("pageLink"));
        }
    }
    public Map<String, Float> search(ArrayList<String> necessaryWords, ArrayList<String> preferredWords, ArrayList<String> forbiddenWords) {
        Map<String, Float> results = new HashMap<>();
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types("_doc");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for(String necessaryWord:necessaryWords) {
            boolQueryBuilder.must(QueryBuilders.matchQuery(textColumn, necessaryWord));
        }
        for(String preferredWord:preferredWords) {
            boolQueryBuilder.should(QueryBuilders.matchQuery(textColumn, preferredWord));
        }
        for(String forbiddenWord:forbiddenWords) {
            boolQueryBuilder.mustNot(QueryBuilders.matchQuery(textColumn, forbiddenWord));
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
            results.put((String) sourceAsMap.get(linkColumn), hit.getScore());
        }
        return SortResults.sortByValues(results);
    }

    public Map<String, Float> findSimilar(String text) {
        Map<String, Float> results = new HashMap<>();
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String[] fields = {textColumn};
        String[] texts = {text};
        searchSourceBuilder.query(QueryBuilders.moreLikeThisQuery(fields, texts, null).minTermFreq(1));
        searchSourceBuilder.size(20);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = runSearch(searchRequest);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            results.put((String) sourceAsMap.get(linkColumn), hit.getScore());
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
