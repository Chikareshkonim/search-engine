package in.nimbo.moama.newsutil;

import in.nimbo.moama.configmanager.PropertyType;

public enum NewsPropertyType implements PropertyType {
    NUMBER_OF_FETCHER_THREADS("news.threads.number.fetcher"), FETCHER_THREAD_PRIORITY("news.threads.priority.fetcher"),
    NEWS_QUEUE_CAPACITY("news.queue.capacity"), NEWS_DATE_FORMAT("news.date.format"),
    CACHE_INITIAL_CAPACITY("news.cache.capacity.initial"), CACHE_MAX_CAPACITY("news.cache.capacity.max"),
    NEWS_WEBSITE_TABLE("hbase.table.websites"), NEWS_PAGES_TABLE("hbase.table.pages"),
    HBASE_TEMPLATE_FAMILY("hbase.family.template"), HBASE_RSS_FAMILY("hbase.family.rss"),
    HBASE_TWITTER_FAMILY("hbase.family.twitter"), HBASE_VISITED_FAMILY("hbase.family.visited");

    private String type;

    NewsPropertyType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }
}
