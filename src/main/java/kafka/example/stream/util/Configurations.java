package kafka.example.stream.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;


/**
 * Created by MaShangkun on 18-9-5.
 */
public final class Configurations {
    private static Configurations instance = null;
    private static String location = "conf/stream.conf";

    private Config config = null;

    private void initConfig() {
        if (null == config) {
            File configFile = new File(location);
            config = ConfigFactory.parseFile(configFile);
        }
    }

    private Configurations() {
        initConfig();
        //@todo read config from stream.conf
//        brokerList = config.getString("broker.list");
    }

    public static Configurations getInstance() {
        if (null == instance) {
            instance = new Configurations();
        }

        return instance;
    }

    public String brokerList = "192.168.1.227:9092";

    public String dataTopic = "testjson";

    public String cacheHost = "127.0.0.1";
    public Integer cachePort = 6379;

    /**
     * Druid
     */
    public String druidDataSourceName = "druid_test_01";
    public Integer druidWindowPeriod = 5;
    public String druidTranquilityZkConnect = "192.168.1.21";
    public Integer druidTranquilityMaxBatchSize = 5000;
    public Integer druidTranquilityMaxPendingBatch = 5;
    public Integer druidTranquilityLingerMs = 1000;
    public String druidIndexServerUrl = "http://192.168.1.227:8090/druid/indexer/v1/task";
    public String druidQueryUrl = "http://192.168.1.227:8082/druid/v2/?pretty";
}
