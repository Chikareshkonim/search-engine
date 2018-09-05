package in.nimbo.moama.fetcher;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class News {
    private NewsInfo newsInfo;
    private String content;

    public News(NewsInfo newsInfo, String content) {

        this.newsInfo = newsInfo;
        this.content=content;
    }

    public NewsInfo getNewsInfo() {
        return newsInfo;
    }

    public void setNewsInfo(NewsInfo newsInfo) {
        this.newsInfo = newsInfo;
    }

    @Override
    public String toString() {
        return newsInfo.toString() + "\n" + "Content: " + content;
    }

    public  JSONObject documentToJson() {
        JSONObject jsonDocument = new JSONObject();
        jsonDocument.put("pageLink", newsInfo.getUrl());
        jsonDocument.put("content", content);
        jsonDocument.put("title", newsInfo.getTitle());
        jsonDocument.put("date", newsInfo.getDate());
        return jsonDocument;
    }

    public Map<String, String> getDocument() {
        Map<String, String> result = new HashMap<>();
        result.put("pageLink", newsInfo.getUrl());
        result.put("content", content);
        result.put("title", newsInfo.getTitle());
        result.put("date", newsInfo.getDate());
        return result;
    }
}
