package newsutil;

import in.nimbo.moama.configmanager.PropertyType;

public enum NewsPropertyType implements PropertyType {
    NUMBER_OF_FETCHER_THREADS("news.threads.number.fetcher"), FETCHER_THREAD_PRIORITY("news.threads.priority.fetcher"),
    NEWS_QUEUE_CAPACITY("news.queue.capacity"), CACHE_INITIAL_CAPACITY("news.cache.capacity.initial"),
    CACHE_MAX_CAPACITY("news.cache.capacity.max");


    private String type;

    NewsPropertyType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }
}
