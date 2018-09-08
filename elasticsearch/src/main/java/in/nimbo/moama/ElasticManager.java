package in.nimbo.moama;

import in.nimbo.moama.configmanager.ConfigManager;
import in.nimbo.moama.metrics.IntMeter;
import in.nimbo.moama.util.ElasticPropertyType;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ElasticManager {

    private final List<String> servers;
    private int bulkCountNumberLimit;
    private int bulkSizeMB;
    private long bulkTimeInterval;
    private int bulkConcurrentRequest;
    private int bulkRetriesToPut;
    private RestHighLevelClient client;
    private String index;
    private Logger LOGGER = Logger.getLogger(ElasticManager.class);
    private int clientPort;
    private int vectorPort;
    private String clusterName;
    private TransportClient transportClient;
    private RestClient restClient;
    private static int numberOfKeywords;

    private BulkProcessor bulkProcessor;
    private String textColumn;
    private String linkColumn;

    public ElasticManager() {
        reconfigure();
        Settings settings = Settings.builder().put("cluster.name", clusterName)
                .put("client.transport.sniff", true).build();
        servers = ConfigManager.getInstance().getProperties(ElasticPropertyType.SERVERS, true).entrySet()
                .stream().map(server -> (String) server.getValue()).collect(Collectors.toList());
        HttpHost[] hosts = servers.stream().map(server -> new HttpHost(server, clientPort, "http"))
                .toArray(HttpHost[]::new);

        TransportAddress[] transportAddresses = servers.stream().map(server -> {
            try {
                return new TransportAddress(InetAddress.getByName(server), vectorPort);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            return null;
        }).filter(Objects::nonNull).toArray(TransportAddress[]::new);

        RestClientBuilder restClientBuilder = RestClient.builder(hosts);
        restClient = restClientBuilder.build();
        client = new RestHighLevelClient(restClientBuilder);

        transportClient = new PreBuiltTransportClient(settings).addTransportAddresses(transportAddresses);
        bulkProcessor = buildBulkProcessor();
    }

    private BulkProcessor buildBulkProcessor() {
        return BulkProcessor.builder(transportClient, new BulkProcessor.Listener() {
            private IntMeter bulkAfterSuccess = new IntMeter("elastic bulkAfterSuccess");
            private IntMeter bulkAdded = new IntMeter("elastic bulkAdded");
            private IntMeter bulkAfterFailure = new IntMeter("elastic bulkAfterFailure");

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                bulkAdded.increment();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                bulkAfterSuccess.increment();
                if (response.hasFailures()) {
                    LOGGER.error("We have failures");
                    for (BulkItemResponse bulkItemResponse : response.getItems()) {
                        if (bulkItemResponse.isFailed()) {
                            LOGGER.error(bulkItemResponse.getId() + " failed with message: " + bulkItemResponse.getFailureMessage());
                        }
                    }
                }

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                bulkAfterFailure.increment();
                failure.printStackTrace();
                LOGGER.error("An exception occurred while indexing", failure);

            }
        })
                .setBulkActions(bulkCountNumberLimit)
                .setBulkSize(new ByteSizeValue(bulkSizeMB, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(bulkTimeInterval))
                .setConcurrentRequests(bulkConcurrentRequest)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), bulkRetriesToPut))
                .build();
    }

    private void reconfigure() {
        bulkCountNumberLimit = ConfigManager.getInstance().getIntProperty(ElasticPropertyType.BULK_ACTION_NUMBER_LIMIT);
        bulkSizeMB = ConfigManager.getInstance().getIntProperty(ElasticPropertyType.BULK_SIZE_MB_LIMIT);
        bulkTimeInterval = ConfigManager.getInstance().getLongProperty(ElasticPropertyType.BULK_TIME_INTERVALS_MS);
        bulkConcurrentRequest = ConfigManager.getInstance().getIntProperty(ElasticPropertyType.BULK_CONCURRENT_REQUEST_NUMBER);
        bulkRetriesToPut = ConfigManager.getInstance().getIntProperty(ElasticPropertyType.BULK_RETRIES_PUT);
        clusterName = ConfigManager.getInstance().getProperty(ElasticPropertyType.CLUSTER_NAME);
        numberOfKeywords = ConfigManager.getInstance().getIntProperty(ElasticPropertyType.ELASTIC_NUMBER_OF_KEYWORDS);
        index = ConfigManager.getInstance().getProperty(ElasticPropertyType.ELASTIC_PAGES_TABLE);
        textColumn = ConfigManager.getInstance().getProperty(ElasticPropertyType.TEXT_COLUMN);
        linkColumn = ConfigManager.getInstance().getProperty(ElasticPropertyType.LINK_COLUMN);
        clientPort = ConfigManager.getInstance().getIntProperty(ElasticPropertyType.CLIENT_PORT);
        vectorPort = ConfigManager.getInstance().getIntProperty(ElasticPropertyType.VECTOR_PORT);
    }

    //TODO set url instead of ids
    public Map<String, Map<String, Double>> getTermVector(ArrayList<String> ids ) throws IOException {
        Map<String, Map<String, Double>> result = new HashMap<>();
        Map<String, String> params = Collections.emptyMap();
        JSONArray idsArray = new JSONArray(ids.stream().map(DigestUtils::md5Hex).toArray());
        String jsonString = "{\n" +
                "\t\"ids\" : " + idsArray.toString() + ",\n" +
                "\t\"parameters\": {\n" +
                "\t\"fields\" : [\"content\"],\n" +
                "   \"offsets\" : false ,\n" +
                "   \"payloads\" : false,\n" +
                "   \"positions\" : false,\n" +
                "   \"term_statistics\": true,\n" +
                "   \"field_statistics\": false,\n" +
                "\"filter\" : {\n" +
                "        \"max_num_terms\" : 5,\n" +
                "                \"min_term_freq\" : 3,\n" +
                "                \"min_doc_freq\" : 5\n" +
                "    }" +
                "\t}\n" +
                "   }";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response =
                restClient.performRequest("POST", "/" + index + "/_doc/_mtermvectors", params, entity);
        JSONArray docs = new JSONObject(EntityUtils.toString(response.getEntity())).getJSONArray("docs");
        for (Object doc : docs) {
            Map<String, Double> keys = new HashMap<>();
            try {
                JSONObject terms = ((JSONObject) doc).getJSONObject("term_vectors").getJSONObject("content").getJSONObject("terms");
                terms.keySet().forEach(key -> keys.put(key, terms.getJSONObject(key).getDouble("score")));
                result.put(((JSONObject) doc).getString("_id"), keys);
            }catch (JSONException e){
                LOGGER.error("doc does not have term vector",e);
            }
        }
        return result;
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
                "                    { \"to\": \"" + date + "\" },\n" +
                "                    { \"from\": \"" + date + "\" }\n" +
                "                ],\n" +
                "                \"keyed\": true\n" +
                "            }\n" +
                "    \t\n" +
                "    \t\t\t,\n" +
                "    \t\"aggs\":{\n" +
                "        \"" + index + "\" : {\n" +
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


    public void put(List<Map<String, String>> docs) {
        ConfigManager.getInstance().getProperty(ElasticPropertyType.BULK_ACTION_NUMBER_LIMIT);

        docs.stream().map(document -> transportClient.prepareIndex(index, "_doc",
                DigestUtils.md5Hex(document.get("pageLink"))).setSource(document).request())
                .forEach(bulkProcessor::add);
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
