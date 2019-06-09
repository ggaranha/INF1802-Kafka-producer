import java.time.LocalDate;

public class Tweet {

    private String username;

    private String tweetText;

    private LocalDate tweetDate;

    public Tweet(String un, String tt, LocalDate td)
    {
        username = un;
        tweetText = tt;
        tweetDate = td;
    }

    public String getTweetText() {
        return tweetText;
    }

    public LocalDate getTweetDate() {
        return tweetDate;
    }

    public String getUsername() {
        return username;
    }

}
