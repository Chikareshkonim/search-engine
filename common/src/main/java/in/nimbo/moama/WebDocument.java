package in.nimbo.moama;

import java.util.ArrayList;
import java.util.List;

public class WebDocument {
    private String textDoc;
    private ArrayList<Link> links;
    private String title;
    private String pagelink;

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

    public String getPagelink() {
        return pagelink;
    }

    public void setPagelink(String pagelink) {
        this.pagelink = pagelink;
    }


}
