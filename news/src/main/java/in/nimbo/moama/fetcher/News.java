package in.nimbo.moama.fetcher;

public class News {
    private RSSLink rssLink;
    private String text;

    public News(RSSLink rssLink, String text) {
        this.rssLink=rssLink;
        this.text=text;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public RSSLink getRssLink() {
        return rssLink;
    }

    public void setRssLink(RSSLink rssLink) {
        this.rssLink = rssLink;
    }
}
