package in.nimbo.moama;

import in.nimbo.moama.document.Link;
import org.jsoup.select.Elements;

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

    public static void splitter(List<Link> links, List<Link> internalLinks, List<Link> externalLinks, String mainDomain) {
        links.forEach(link -> {
            if (link.getDomain().equals(mainDomain)) internalLinks.add(link);
            else externalLinks.add(link);
        });
    }
}

