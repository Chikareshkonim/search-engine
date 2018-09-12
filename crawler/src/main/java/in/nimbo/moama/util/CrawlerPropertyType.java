package in.nimbo.moama.util;

import in.nimbo.moama.configmanager.PropertyType;

public enum CrawlerPropertyType implements PropertyType {
    CRAWLER_POLITE_TIME("crawler.polite.time"),
    CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA("crawler.internal.link.add.to.kafka"),
    CRAWLER_NUMBER_OF_THREADS("crawler.number.of.threads"),
    CRAWLER_START_NEW_THREAD_DELAY_MS("crawler.start.new.thread.delay.ms"),
    CRAWLER_MIN_OF_EACH_THREAD_QUEUE("crawler.min.of.each.thread.queue"),
    CRAWLER_THREAD_PRIORITY("crawler.thread.priority"),
    CRAWLER_DOMAIN_CHECKER_HASH_PRIME("crawler.domain.checker.hash.prime"),
    CRAWLER_DUPLICATE_HASH_PRIME("crawler.duplicate.hash.prime"),
    CRAWLER_SHUFFLE_SIZE("crawler.shuffle.size"),
    HBASE_TABLE("hbase.table"),
    HBASE_FAMILY_OUTLINKS("hbase.family.outlinks"),
    HBASE_FAMILY_SCORE("hbase.family.score"),
    CRAWLER_HELPER_TOPIC_NAME("crawler.helper.topic.name"),
    CRAWLER_LINK_TOPIC_NAME("crawler.links.topic.name"),
    DUPLICATE_HANDLER_MAX_CAPACITY("duplicate.handler.max.capacity"),
    DUPLICATE_HANDLER_INITIAL_CAPACITY("duplicate.handler.initial.capacity"),
    CRAWLER_CRAWLED_TOPIC_NAME("crawler.crawled.topic.name"),
    LISTENER_PORT("crawler.listener.port"),
    CRAWLER_FETCHER_THREAD("crawler.fetcher.threads"),
    CRAWLER_CONSUMER_THREADS("crawler.consumer.threads"),
    CRAWLER_CONSUMER_PRIORITY("crawler.consumer.priority"),
    CRAWLER_FETCHER_PRIORITY("crawler.fetcher.priority"),
    CRAWLER_FETCHER_BATCH_SIZE("crawler.fetcher.batch.size");

    private String type;

    CrawlerPropertyType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }
}
