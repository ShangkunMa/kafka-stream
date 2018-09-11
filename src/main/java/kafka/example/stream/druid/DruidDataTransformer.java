package kafka.example.stream.druid;

import kafka.example.stream.model.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by MaShangkun on 18-9-5.
 */
public class DruidDataTransformer {

    public static final String DRUID_FIELD_TIMESTAMP = "timestamp";
    public static final String DRUID_FIELD_ID = "id";
    public static final String DRUID_FIELD_TYPE = "type";
    public static final String DRUID_FIELD_NAME = "name";

    public static final String DRUID_FIELD_COUNT = "count";
    public static final String DRUID_FIELD_AMOUNT = "amount";

    public static final String[] druidSchemaFields = {
            DRUID_FIELD_TIMESTAMP,
            DRUID_FIELD_ID,
            DRUID_FIELD_TYPE,
            DRUID_FIELD_NAME,

            DRUID_FIELD_COUNT,
            DRUID_FIELD_AMOUNT
    };

    public static Map<String, Object> DataToDruidMap(Data data) {
        Map<String, Object> map = new HashMap<>();
        int i = 0;
        map.put(druidSchemaFields[i++], data.getTime());
        map.put(druidSchemaFields[i++], "" + data.getId());
        map.put(druidSchemaFields[i++], "" + data.getType());
        map.put(druidSchemaFields[i++], data.getName());

        map.put(druidSchemaFields[i++], "1");
        map.put(druidSchemaFields[i++], "" + data.getAmount());
        return map;
    }
}