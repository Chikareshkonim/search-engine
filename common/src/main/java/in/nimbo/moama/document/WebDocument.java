package in.nimbo.moama.document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WebDocument {
    private String textDoc;
    private ArrayList<Link> links;
    private String title;
    private String pageLink;

    public ArrayList<Link> getLinks() {
        return links;
    }

    public void setLinks(List<Link> links) {
        this.links = (ArrayList<Link>) links;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTextDoc() {
        return textDoc;
    }

    public void setTextDoc(String textDoc) {
        this.textDoc = textDoc;
    }

    public String getPageLink() {
        return pageLink;
    }

    public void setPageLink(String pageLink) {
        this.pageLink = pageLink;
    }

    public Map<String,String> elasticMap(){
        Map<String,String> map=new LinkedHashMap<>();
        map.put("content",this.textDoc);
        map.put("pageLink", pageLink);
        return map;
    }

}
