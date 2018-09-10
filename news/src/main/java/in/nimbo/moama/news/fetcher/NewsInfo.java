package in.nimbo.moama.news.fetcher;

public class NewsInfo {
    private String title;
    private String date;
    private String url;
    private String domain;

    public NewsInfo(String title, String date, String url, String domain) {
        this.title = title;
        this.date = date;
        this.url = url;
        this.domain = domain;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getTitle() {
        return title;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public String toString() {
        return "Title: " + title + "\n" +
                "Date: " + date + "\n" +
                "URL: " + url + "\n" +
                "Domain: " + domain;
    }
}
