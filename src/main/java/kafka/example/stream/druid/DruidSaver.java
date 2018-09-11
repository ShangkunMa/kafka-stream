package kafka.example.stream.druid;

import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import kafka.example.stream.model.Data;
import kafka.example.stream.util.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class DruidSaver implements ProcessorSupplier<String, Data> {

    @Override
    public Processor<String, Data> get() {
        return new Processor<String, Data>() {

            private Jedis cache = null;
            private Tranquilizer<Map<String, Object>> tranquilizer = null;

            @Override
            @SuppressWarnings("unchecked")
            public void init(ProcessorContext context) {
                LogHelper.info("DruidSaver.init ...");
                tranquilizer = DruidHelper.getTranquilizer(new DruidBeamFactory(), Configurations.getInstance().druidDataSourceName);
                cache = RedisHelper.getInstance().getJedis();
            }

            @Override
            public void process(String key, Data value) {
                Map<String, Object> druidMap = DruidDataTransformer.DataToDruidMap(value);
                if (null == tranquilizer) {
                    pushDataToCache(value);
                    LogHelper.info("Tranquilizer init Failed.");
                    return;
                }
                Future future = tranquilizer.send(druidMap);
                future.addEventListener(new FutureEventListener() {
                    @Override
                    public void onFailure(Throwable cause) {
                        //@todo lpush value to redis list
                        pushDataToCache(value);
                        LogHelper.info("Save Failed: " + value.toString());
                    }

                    @Override
                    public void onSuccess(Object o) {
                        LogHelper.info("Save Success: " + value.toString());
                    }
                });
            }

            @Override
            public void close() {
                LogHelper.info("DruidSaver.close ...");
            }

            /**
             * another program to read cache resave druid
             * @param data
             */
            private void pushDataToCache(Data data) {
                data.addSaveCount();
                try {
                    cache.lpush(GlobalConstants.CACHE_KEY_LIST_DRUID_RE_SAVE, data.toString());
                } catch (Exception e) {
                    LogHelper.info("DruidSaver.pushDataToCache got error: " + e.getMessage());
                    try {
                        RedisHelper.getInstance().returnJedis(cache);
                        cache = null;
                        cache = RedisHelper.getInstance().getJedis();
                        cache.lpush(GlobalConstants.CACHE_KEY_LIST_DRUID_RE_SAVE, data.toString());
                    } catch (Exception e2) {
                        LogHelper.info("DruidSaver.pushDataToCache retry got error: " + e2.getMessage());
                    }
                }
            }
        };
    }
}
