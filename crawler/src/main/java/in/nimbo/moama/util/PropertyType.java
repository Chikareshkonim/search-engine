package in.nimbo.moama.util;

public enum PropertyType implements in.nimbo.moama.configmanager.PropertyType {
    CRAWLER_POLITE_TIME("crawler.polite..time"), CRAWLER_INTERNAL_LINK_ADD_TO_KAFKA("crawler.internal.link.add.to.kafka"),
    CRAWLER_NUMBER_OF_THREADS("crawler.number.of.threads"), CRAWLER_START_NEW_THREAD_DELAY_MS("crawler.start.new.thread.delay.ms"),
    CRAWLER_MIN_OF_EACH_THREAD_QUEUE("crawler.min.of.each.thread.queue"), CRAWLER_THREAD_PRIORITY("crawler.thread.priority"),
    CRAWLER_DOMAIN_CHECKER_HASH_PRIME("crawler.domain.checker.hash.prime"), CRAWLER_DUPLICATE_HASH_PRIME("crawler.duplicate.hash.prime"),
    CRAWLER_SHUFFLE_SIZE("crawler.shuffle.size");

    public void setType(String type) {

    }

    private String type;

    PropertyType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }
}
