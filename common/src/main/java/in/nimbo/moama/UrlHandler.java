package in.nimbo.moama;

import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;

public class UrlHandler {

    public static String normalizeLink(String link, String mainUrl) {
        if (link.startsWith("/")) {
            link = mainUrl + link;
        }
        if (link.startsWith("www")) {
            link = "http://" + link;
        }
        return link;
    }

    public static Link[]

    getLinks(Elements links, String mainUrl) {
        return links.stream().filter(element -> !element.attr("href").contains("#"))
                .map(element -> new Link(element, mainUrl))
                .filter(e -> !e.getDomain().equals("ERROR"))
                .toArray(Link[]::new);
    }

    public static void splitter(List<Link> links, ArrayList<String> internalLinks, ArrayList<String> externalLinks, String mainDomain) {
        for (Link link : links) {
            if (link.getDomain().equals(mainDomain)) {
                internalLinks.add(link.getUrl());
            } else {
                externalLinks.add(link.getUrl());
            }
        }
    }
}

