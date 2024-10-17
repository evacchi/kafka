package kafka.server.transform;

public class Header {
    String key; String value;

    public Header(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
