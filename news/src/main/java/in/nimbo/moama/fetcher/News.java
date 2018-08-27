package in.nimbo.moama.fetcher;

public class News {
    private NewsInfo newsInfo;
    private String text;

    public News(NewsInfo newsInfo, String text) {
        this.newsInfo = newsInfo;
        this.text=text;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public NewsInfo getNewsInfo() {
        return newsInfo;
    }

    public void setNewsInfo(NewsInfo newsInfo) {
        this.newsInfo = newsInfo;
    }

    @Override
    public String toString() {
        return newsInfo.toString() + "\n" + "Content: " + text;
    }
}
