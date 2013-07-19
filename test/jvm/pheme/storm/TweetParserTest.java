package pheme.storm;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public class TweetParserTest {
    @Test
    public void testParse() throws Exception {
        TweetParser tweetParser = new TweetParser("<span class=\"status-content\">                  <span class=\"entry-content\">RT @" +
                "<a class=\"tweet-url username\" href=\"/strike2_de\" rel=\"nofollow\">strike2_de</a>: Aus dem Schneider &amp; " +
                "Wulf Blog: Zukunftssichere Hardware - IBM BladeServer HS22V: <a href=\"http://t.co/ENvRmLA\" class=\"tweet-url web\" " +
                "rel=\"nofollow\" target=\"_blank\">http://t.co/ENvRmLA</a></span>          </span>    <span class=\"meta entry-meta\" data=\"{}\">  " +
                "<a class=\"entry-date\" rel=\"bookmark\" href=\"http://twitter.com/dankesupporter/status/60632731264102400\">    " +
                "<span class=\"published timestamp\" data=\"{time:&apos;Wed Apr 20 09:15:44 +0000 2011&apos;}\">2:15 AM Apr 20th</span></a>  " +
                "<span>via <a href=\"http://www.hootsuite.co¢m\" rel=\"nofollow\">HootSuite</a></span>      </span>        " +
                "<ul class=\"meta-data clearfix\" />  ");

        assertEquals("strike2_de", tweetParser.username());
        assertEquals(true, tweetParser.isRetweet());

        assertEquals(1, tweetParser.urls().size());
        assertEquals("http://t.co/ENvRmLA", tweetParser.urls().get(0));
        Date date = new DateTime(2011, 4, 20, 9, 15, 44, DateTimeZone.forOffsetMillis(0)).toDate();
        assertEquals(date, tweetParser.time());
        assertEquals("HootSuite", tweetParser.via());
        assertEquals("60632731264102400", tweetParser.id());
        assertEquals("RT @strike2_de: Aus dem Schneider & Wulf Blog: Zukunftssichere Hardware - IBM BladeServer HS22V: http://t.co/ENvRmLA", tweetParser.tweet());
    }
}
