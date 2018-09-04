package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.metrics.JMXManager;
import in.nimbo.moama.util.ElasticPropertyType;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.client.*;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.search.aggregations.AggregationBuilders;

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
    private static IntMeter elasticAdded =new IntMeter("elastic Added");
    private TransportClient transportClient ;
    private RestClient restClient;
    private static int NUMBER_OF_KEYWORDS;
    public ElasticManager() {
        transportClient = null;
        NUMBER_OF_KEYWORDS = 5;
        elasticFlushSizeLimit = Integer.parseInt(ConfigManager.getInstance().getProperty(ElasticPropertyType.ELASTIC_FLUSH_SIZE_LIMIT));
        elasticFlushNumberLimit = Integer.parseInt(ConfigManager.getInstance().getProperty(ElasticPropertyType.ELASTIC_FLUSH_NUMBER_LIMIT));
        index = ConfigManager.getInstance().getProperty(ElasticPropertyType.ELASTIC_PAGES_TABLE);
        test = ConfigManager.getInstance().getProperty(ElasticPropertyType.ELASTIC_TEST_TABLE);
        textColumn = ConfigManager.getInstance().getProperty(ElasticPropertyType.TEXT_COLUMN);
        linkColumn = ConfigManager.getInstance().getProperty(ElasticPropertyType.LINK_COLUMN);
        server1 = ConfigManager.getInstance().getProperty(ElasticPropertyType.SERVER_1);
        server2 = ConfigManager.getInstance().getProperty(ElasticPropertyType.SERVER_2);
        server3 = ConfigManager.getInstance().getProperty(ElasticPropertyType.SERVER_3);
        clientPort = ConfigManager.getInstance().getProperty(ElasticPropertyType.CLIENT_PORT);
        vectorPort = ConfigManager.getInstance().getProperty(ElasticPropertyType.VECTOR_PORT);
        clusterName = ConfigManager.getInstance().getProperty(ElasticPropertyType.CLUSTER_NAME);
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(server1, Integer.parseInt(clientPort), "http"),
                new HttpHost(server2, Integer.parseInt(clientPort), "http"),
                new HttpHost(server3, Integer.parseInt(clientPort), "http"));
        restClient = restClientBuilder.build();
        client = new RestHighLevelClient(restClientBuilder);

        indexRequest = new IndexRequest(index, "_doc");
        bulkRequest = new BulkRequest();
    }

    //TODO
    public void getTermVector(String id) throws IOException {
        Map<String, String> params = Collections.emptyMap();
        String jsonString =
                "{"
                        + "\"fields\" : [\"text\"],"
                        + "\"term_statistics\" : true,"
                        + "\"field_statistics\" : true,"
                        + "\"positions\" : false,"
                        + "\"offsets\" : false,"
                        + "\"filter\": {"
                        + "\"max_num_terms\" : "
                        + NUMBER_OF_KEYWORDS
                        + "}"
                        + "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response =
                restClient.performRequest("GET", "/" + index + "/_doc/" + id + "/_termvectors", params, entity);
        // System.out.println(response.toString());
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuilder out = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            out.append(line);
        }
        System.out.println(out.toString());
        JSONObject jsonObject = new JSONObject(out.toString());
        JSONObject jsonArray = jsonObject.getJSONObject("term_vectors").getJSONObject("text").getJSONObject("terms");
        System.out.println(jsonArray.getJSONObject("lasttest1"));
        for (String key : jsonArray.keySet()) {
            System.out.println(key + "=" + jsonArray.get(key)); // to get the value
        }
    }

    public List<String> newsWordTrends(String toDate , String fromDate) throws IOException {
        Map<String, String> params = Collections.emptyMap();
        String jsonString ="{\n" +
                "\t\"aggs\":{\n" +
                "\t\t\"range\": {\n" +
                "           \"date_range\": {\n" +
                "               \"field\": \"date\",\n" +
                "               \"format\": \"yyyy-mm-dd\",\n" +
                "               \"ranges\": [\n" +
                "                   { \"to\":"+ toDate+"},\n" +
                "                   { \"from\":"+fromDate+"}\n" +
                "               ],\n" +
                "               \"keyed\": true\n" +
                "            }\n" +
                "            ,\n" +
                "           \"aggs\":{\n" +
                "\t\t\t\"categories\": {\n" +
                "        \t\t\"terms\": {\n" +
                "            \t\t\"field\": \"content\"\n" +
                "        \t\t\t}\n" +
                "        \t\t}\n" +
                "        \t}\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response =
                restClient.performRequest("POST","/test/_search",params,entity );
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuilder out = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            out.append(line);
        }
        JSONObject jsonObject = new JSONObject(out.toString());
        JSONObject buckets = jsonObject.getJSONObject("aggregations").getJSONObject("range").getJSONObject("buckets");
        Set<String> keys = buckets.keySet();
        List<JSONArray> arrays = new LinkedList<>();
        for (String key : keys) {
            arrays.add(buckets.getJSONObject(key).getJSONObject("categories").getJSONArray("buckets"));
        }
        List<String> keywords = new LinkedList<>();
        for (JSONArray array:arrays) {
            for (Object anArray : array) {
                keywords.add(((JSONObject) anArray).getString("key"));
            }
        }
        return keywords;
    }

    public synchronized void put(JSONObject document) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            try {
                builder.startObject();
                {
                    document.keySet().forEach(key -> {
                        try {
                            if (!key.equals("outLinks")) {
                                builder.field(key, document.get(key));
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                            errorLogger.error("ERROR! couldn't add " + document.get(key) + " to elastic");
                        }
                    });
                }
                builder.endObject();
                indexRequest.source(builder);
                indexRequest.id(DigestUtils.md5Hex(document.getString("pageLink")));
                bulkRequest.add(indexRequest);
                indexRequest = new IndexRequest(index, "_doc");
                if (bulkRequest.numberOfActions() >= elasticFlushSizeLimit) {
                    client.bulk(bulkRequest);
                    elasticAdded.add(bulkRequest.numberOfActions());
                    bulkRequest = new BulkRequest();
//                    jmxManager.markNewAddedToElastic();
                }
            } catch (IOException e) {
                errorLogger.error("ERROR! Couldn't add the document for " + document.get("pageLink"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void myput(List<Map<String, String>> docs) {
        IndexRequest indexRequest = new IndexRequest(index, "_doc");
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, String> document : docs) {
            indexRequest.source(document);
            indexRequest.id(DigestUtils.md5Hex(document.get("pageLink")));
            bulkRequest.add(indexRequest);
            indexRequest = new IndexRequest(index, "_doc");
        }
        try {
            client.bulk(bulkRequest);
            elasticAdded.add(bulkRequest.numberOfActions());
            docs.clear();
//                    jmxManager.markNewAddedToElastic();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            errorLogger.error("ERROR! Couldn't add the document for ", e);
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
