package kafka.example.stream;

import kafka.example.stream.application.SaveDruidApplication;

public class Master {

    public static void main(String... a) {
        startAllApplication();
    }

    private static void startAllApplication() {
        SaveDruidApplication.buildApplication();
    }
}
