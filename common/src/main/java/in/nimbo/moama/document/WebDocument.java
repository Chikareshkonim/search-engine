package in.nimbo.moama.document;

import in.nimbo.moama.document.Link;
import org.json.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

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

    public String documentToJson(){
        JSONObject jsonDocument = new JSONObject();
        jsonDocument.put("content",this.textDoc);
        jsonDocument.put("pageLink", pageLink);
        JSONArray outLinks = new JSONArray();
        JSONObject outLink;
        for(Link link : this.getLinks()){
            outLink = new JSONObject();
            outLink.put("LinkUrl", link.getUrl());
            outLink.put("LinkAnchor", link.getAnchorLink());
            outLinks.put(outLink);
        }
        jsonDocument.put("outLinks", outLinks);
        return String.valueOf(jsonDocument);
    }

}
