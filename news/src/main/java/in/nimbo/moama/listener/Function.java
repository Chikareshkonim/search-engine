package in.nimbo.moama.listener;

import in.nimbo.moama.CLI;
import in.nimbo.moama.RSSs;
import in.nimbo.moama.template.SiteTemplates;
import in.nimbo.moama.template.Template;
import in.nimbo.moama.template.TemplateFinder;

import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.Scanner;

public class Function {

    @CLI(help = "give you last news of each site")
    public static void lastNews(PrintStream out, Scanner scanner) {
        // TODO: 8/16/18
    }

    @CLI(help = "give you trend words in last Hour(for news)")
    public static void newsTrendWordInLastHour(PrintStream out, Scanner scanner) {
        // TODO: 8/16/18
    }

    @CLI(help = "give you trend words in last Day(for news)")
    public static void newsTrendWordInLastDay(PrintStream out, Scanner scanner) {
        // TODO: 8/16/18
    }


    @CLI(help = "give you trend words in last Week(for news)")
    public static void newsTrendWordInLastWeek(PrintStream out, Scanner scanner) {
        // TODO: 8/16/18
    }

    @CLI(help = "add a rss to your feeder")
    public static void addRss(PrintStream out, Scanner scanner) {
        out.print("please enter a rss address");
        String rss = scanner.next();
        out.print("please enter domain of this site(it will be used for finding and if its template )");
        String rssDomain = scanner.next();
        if (!SiteTemplates.getInstance().getSiteTemplates().containsKey(rssDomain)) {
            label:
            while (true) {
                Template template = null;
                out.println("you want manual/automatic adding domain template");
                switch (scanner.next()) {
                    case "manual":
                        out.println("please enter attribute model then enter its value then you must enter its date format\n" +
                                "(you should have information from it)and at least the tag of news in rss (news tag) then \n" +
                                "we will create this template and add it for you must know if you enter invalid we will not\n" +
                                "check it so keep calm and be safe");
                        template = new Template(scanner.nextLine(), scanner.nextLine(), scanner.nextLine(), rssDomain, scanner.nextLine());
                        break;
                    case "automatic":
                        out.print("please enter tag of news link in this rss");
                        String newsTag = scanner.next();
                        try {
                            template = TemplateFinder.findTemplate(rss, rssDomain, newsTag);
                        } catch (IOException e) {
                            e.printStackTrace(out);
                            out.println("\ntry again later");
                        }
                        break;
                    default:
                        out.println("incorrect input");
                        continue label;
                }
                out.println("is this correct?y/n");
                out.println(template);
                if (scanner.next().equals("y")) {
                    SiteTemplates.getInstance().getSiteTemplates().put(rssDomain,template);
                    RSSs.getInstance().rssToDomainMap.put(rss,rssDomain);
                }
                break ;
            }
        } else {
            RSSs.getInstance().rssToDomainMap.put(rss, rssDomain);
            out.println("added with this ");
            out.println(SiteTemplates.getInstance().getSiteTemplates().get(rssDomain));
        }
    }

    @CLI(help = "show you all template list")
    public static void showTemplates(PrintStream out, Scanner scanner) {
        SiteTemplates.getInstance().getSiteTemplates().forEach((key, value) -> out.println(key + "   " + value));
    }

    @CLI(help = "show all rss")
    public static void showRss(PrintStream out, Scanner scanner) {
        RSSs.getInstance().rssToDomainMap.forEach((rss,domain)->out.println(rss+"     its domain:  "+domain));
    }

    @CLI(help = "you can delete a template ")
    public static void deleteTemplate(PrintStream out, Scanner scanner) {
        out.println("please enter the domain of site");
        String domain=scanner.nextLine();
        SiteTemplates.getInstance().getSiteTemplates().remove(domain);
        LinkedList<String> linkedList=new LinkedList<>();
        RSSs.getInstance().rssToDomainMap.forEach((rss,rssDomain)->{
            if(rssDomain.equals(domain)){
                linkedList.add(rss);
            }
        });
        linkedList.forEach(e->RSSs.getInstance().rssToDomainMap.remove(e));
    }
    @CLI(help = "you can delete a rss from rss reader ")
    public static void deleteRss(PrintStream out, Scanner scanner) {
        out.println("please enter rss url");
        RSSs.getInstance().rssToDomainMap.remove(scanner.nextLine());
    }
    @CLI(help = "this method will save templates and RSSs in json file")
    public  static  void  saveAll(PrintStream out,Scanner scanner){
        SiteTemplates.getInstance().saveTemplate();
        RSSs.getInstance().saveRSSs();
    }

    @CLI(help = "this method will load templates and RSSs from json file ")
    public static void loadAll(PrintStream out, Scanner scanner) {
        SiteTemplates.getInstance().loadTemplates();
        RSSs.getInstance().loadRSSs();
    }

    @CLI(help = "save all thing will be needed and exit")
    public static void exit(PrintStream out, Scanner scanner) {
        saveAll(out, scanner);
        System.exit(0);
    }
}
