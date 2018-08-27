package in.nimbo.moama.document;

import in.nimbo.moama.document.Link;

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


}
