package in.nimbo.moama.twitter;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.EntitySupport;
import twitter4j.Status;
import twitter4j.URLEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TwitterHandler {
    private static Properties properties = new Properties();

    static {
        try {
            properties.load(TwitterHandler.class.getResourceAsStream("/twitter.PROPERTIES"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private JavaStreamingContext jssc;

    public TwitterHandler(String name, String master) {
        SparkConf conf = new SparkConf().setAppName(name).setMaster(master);
        jssc = new JavaStreamingContext(conf, new Duration(3000));
    }

    public void run() {
//        System.setProperties(PROPERTIES);
        System.setProperty("twitter4j.oauth.consumerKey", properties.getProperty("twitter4j.oauth.consumerKey"));
        System.setProperty("twitter4j.oauth.consumerSecret", properties.getProperty("twitter4j.oauth.consumerSecret"));
        System.setProperty("twitter4j.oauth.accessToken", properties.getProperty("twitter4j.oauth.accessToken"));
        System.setProperty("twitter4j.oauth.accessTokenSecret", properties.getProperty("twitter4j.oauth.accessTokenSecret"));
        System.setProperty("twitter4j.http.proxyHost", "127.0.0.1");
        System.setProperty("twitter4j.http.proxyPort", "41709");
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);
        twitterStream.filter(status -> status.getLang().equals("en")).map(EntitySupport::getURLEntities)
                .flatMap(urls -> {
                    List<String> result = new ArrayList<>();
                    for (URLEntity url : urls) {
                        result.add(url.getURL());
                    }
                    return result;
                }).print();
        jssc.start();
        jssc.awaitTermination();
    }


}
