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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ElasticManager {

    private RestHighLevelClient client;
    private String index;
    private String test;
    private Logger logger = Logger.getLogger(ElasticManager.class);
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
    private static IntMeter elasticAdded = new IntMeter("elastic Added");
    private TransportClient transportClient;
    private RestClient restClient;
    private static int NUMBER_OF_KEYWORDS;
    private JMXManager jmxManager = JMXManager.getInstance();

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
    public Map<String, Map<String, Double>> getTermVector(String ids) throws IOException {
        Map<String, String> params = Collections.emptyMap();
        String jsonString = "{\n" +
                "\t\"ids\" : " + ids + ",\n" +
                "\t\"parameters\": {\n" +
                "\t\"fields\" : [\"content\"],\n" +
                "   \"offsets\" : true ,\n" +
                "   \"payloads\" : true,\n" +
                "   \"positions\" : true,\n" +
                "   \"term_statistics\": true,\n" +
                "   \"field_statistics\": true\n" +
                "\t}\n" +
                "   }";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response =
                restClient.performRequest("POST", "/" + index + "/_doc/_mtermvectors", params, entity);
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuilder out = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            out.append(line);
        }
        System.out.println(out.toString());
        JSONObject jsonObject = new JSONObject(out.toString());

//        JSONObject jsonArray = jsonObject.getJSONObject("term_vectors").getJSONObject("content").getJSONObject("terms");
//        for (String key : jsonArray.keySet()) {
//            System.out.println(key + "=" + jsonArray.get(key)); // to get the value
//        }
        return null;
    }

    public List<String> newsWordTrends(String date) throws IOException {
        Map<String, String> params = Collections.emptyMap();
        String jsonString = "{\"size\":0,\n" +
                "    \t\"aggs\":{\n" +
                "    \t\t\"range\":{\n" +
                "    \t\"date_range\": {\n" +
                "                \"field\": \"date\",\n" +
                "                \"format\": \"EEE, dd MMM yyyy\",\n" +
                "                \"ranges\": [\n" +
                "                    { \"to\": \""+date+"\" },\n" +
                "                    { \"from\": \""+date+"\" }\n" +
                "                ],\n" +
                "                \"keyed\": true\n" +
                "            }\n" +
                "    \t\n" +
                "    \t\t\t,\n" +
                "    \t\"aggs\":{\n" +
                "        \""+index+"\" : {\n" +
                "            \"terms\" : { \"field\" : \"content\" \n" +
                "            \t,\"size\":5\n" +
                "            }\n" +
                "        }\n" +
                "    \t}\t\n" +
                "    \t\t}\n" +
                "    }\n" +
                "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response =
                restClient.performRequest("POST", "/" + index + "/_search?size=0", params, entity);
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuilder out = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            out.append(line);
        }
        System.out.println(out.toString());
        JSONObject jsonObject = new JSONObject(out.toString());
        JSONArray buckets = jsonObject.getJSONObject("aggregations").getJSONObject(index).getJSONArray("buckets");
        List<String> keywords = new LinkedList<>();
        for (Object bucket : buckets) {
            keywords.add(((JSONObject) bucket).getString("key"));
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
                            logger.error("ERROR! couldn't add " + document.get(key) + " to elastic");
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
                logger.error("ERROR! Couldn't add the document for " + document.get("pageLink"));
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
            jmxManager.markNewAddedToElastic(bulkRequest.numberOfActions());
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("ERROR! Couldn't add the document for ", e);
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
