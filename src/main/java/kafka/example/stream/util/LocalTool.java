package kafka.example.stream.util;

import kafka.example.stream.model.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LocalTool {
    public static void main(String... a) {
        new LocalTool().pushDataToKafka(10, "testjson");
        KafkaHelper.destroyConnection();
    }

    private LocalTool() {
    }

    private Random random = new Random(System.currentTimeMillis());

    /**
     * test create data and push it to kafka topic
     *
     * @param count
     * @param topic
     */
    private void pushDataToKafka(int count, String topic) {
        List<Data> dataList = getDataList(count);
        for (Data data : dataList) {
            KafkaHelper.pushMessageToTopic(topic, data.toString());
//            KafkaHelper.pushMessageToTopic(topic, data.toString());
        }
    }

    private List<Data> getDataList(int count) {
        List<Data> re = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            re.add(generalDataJson());
        }
        return re;
    }

    private Data generalDataJson() {
        Data data = new Data();
        data.setTime(System.currentTimeMillis() / 1000);
        data.setId(getRandom(1, 20));
        data.setName(getRandom(4));
        data.setAmount(getRandom(5, 50));
        data.setType(getRandom(1, 4));
        return data;
    }

    private int getRandom(int bottom, int upper) {
        int r = random.nextInt();
        r = r > 0 ? r : 0 - r;
        r = r % (upper - bottom) + bottom;
        return r;
    }

    private String getRandom(int len) {
        len = len > 32 ? 32 : len;
        String r = GeneralHelper.MD5(String.valueOf(random.nextDouble()));
        return r.substring(0, len);
    }
}
