package pheme.storm;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TweetParser {
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("E MMM d k:m:s Z y");
    private String id;
    private String username;
    private boolean retweet;
    private List<String> urls;
    private String tweet;
    private Date date;
    private String via;

    public TweetParser(String text) {
        Document document = Jsoup.parse(text);
        id = document.select("a[rel=bookmark]").attr("href").replaceFirst(".*/status/", "");
        username = parse(document, "a.username");
        this.tweet = document.select("span.entry-content").text();
        retweet = parse(document, "span.entry-content").startsWith("RT");
        via = document.select("span.meta span a").text();

        urls = new ArrayList<String>();
        Elements urlElements = document.select("a.web");
        for (Element urlElement : urlElements) {
            urls.add(urlElement.select("[href]").text());
        }

        try {
            String dateText = document.select("span.timestamp").attr("data").replace("{time:'", "").replace("'}", "");
            date = DATE_FORMAT.parse(dateText);
        } catch (Exception e) {
            System.err.println("Encountered error while parsing date: " + e.getMessage());
//            throw new RuntimeException(e);
        }
    }

    private String parse(Document document, String cssQuery) {
        Element element = document.select(cssQuery).first();
        return element == null ? "N/A" : element.text();
    }

    public String username() {
        return username;
    }

    public boolean isRetweet() {
        return retweet;
    }

    public List<String> urls() {
        return urls;
    }

    public String tweet() {
        return tweet;
    }

    public Date time() {
        return date;
    }

    public String via() {
        return via;
    }

    public String id() {
        return id;
    }

    @Override
    public String toString() {
        return "TweetParser{" +
                "id='" + id + '\'' +
                ", username='" + username + '\'' +
                ", retweet=" + retweet +
                ", urls=" + urls +
                ", tweet='" + tweet + '\'' +
                ", date=" + date +
                ", via='" + via + '\'' +
                '}';
    }
}
