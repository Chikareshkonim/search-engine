package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.metrics.JMXManager;
import in.nimbo.moama.metrics.Metrics;
import in.nimbo.moama.util.ElasticPropertyType;
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class ElasticManager {
    private RestHighLevelClient client;
    private String index;
    private String test;
    private Logger errorLogger = Logger.getLogger(ElasticManager.class);
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

    public ElasticManager() {
        elasticFlushSizeLimit = Integer.parseInt(ConfigManager.getInstance().getProperty(ElasticPropertyType.ELASTIC_FLUSH_SIZE_LIMIT));
        elasticFlushNumberLimit = Integer.parseInt(ConfigManager.getInstance().getProperty(ElasticPropertyType.ELASTIC_FLUSH_NUMBER_LIMIT));
        index = ConfigManager.getInstance().getProperty(ElasticPropertyType.ELASTIC_PAGES_TABLE);
        test = ConfigManager.getInstance().getProperty(ElasticPropertyType.ELASTIC_TEST_TABLE);
        textColumn = ConfigManager.getInstance().getProperty(ElasticPropertyType.Text_COLUMN);
        linkColumn = ConfigManager.getInstance().getProperty(ElasticPropertyType.LINK_COLUMN);
        server1 = ConfigManager.getInstance().getProperty(ElasticPropertyType.SERVER_1);
        server2 = ConfigManager.getInstance().getProperty(ElasticPropertyType.SERVER_2);
        server3 = ConfigManager.getInstance().getProperty(ElasticPropertyType.SERVER_3);
        clientPort = ConfigManager.getInstance().getProperty(ElasticPropertyType.CLIENT_PORT);
        vectorPort = ConfigManager.getInstance().getProperty(ElasticPropertyType.VECTOR_PORT);
        clusterName = ConfigManager.getInstance().getProperty(ElasticPropertyType.CLUSTER_NAME);
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
        TermVectorsRequest termVectorsRequest = new TermVectorsRequest(test, "_doc", "1");
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

    public void put(JSONObject document, JMXManager jmxManager) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                Set<String> keys = document.keySet();
                keys.forEach(key -> {
                    try {
                        if (!key.equals("outLinks"))
                            builder.field(key, document.get(key));
                    } catch (IOException e) {
                        errorLogger.error("ERROR! couldn't add " + document.get(key) + " to elastic");
                    }
                });
            }
            builder.endObject();
            added++;
            if (bulkRequest.estimatedSizeInBytes() / 1000000 >= elasticFlushSizeLimit ||
                    bulkRequest.numberOfActions() >= elasticFlushNumberLimit) {
                synchronized (sync) {
                    bulkRequest.add(indexRequest);
                    client.bulk(bulkRequest);
                    bulkRequest = new BulkRequest();
                    indexRequest = new IndexRequest(index);
                    Metrics.numberOfPagesAddedToElastic = added;
                    jmxManager.markNewAddedToElastic();
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
        for (String necessaryWord : necessaryWords) {
            boolQueryBuilder.must(QueryBuilders.matchQuery(textColumn, necessaryWord));
        }
        for (String preferredWord : preferredWords) {
            boolQueryBuilder.should(QueryBuilders.matchQuery(textColumn, preferredWord));
        }
        for (String forbiddenWord : forbiddenWords) {
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

    private SearchResponse runSearch(SearchRequest searchRequest) {
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
