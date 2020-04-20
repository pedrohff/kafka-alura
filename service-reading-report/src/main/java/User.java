public class User {
    private final String uuid;

    public User(String uuid) {
        this.uuid = uuid;
    }

    public String getReportPath() {
        return "target/others/" + uuid + ".txt";
    }

    public String getUuid() {
        return uuid;
    }
}
