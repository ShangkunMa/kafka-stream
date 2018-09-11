package kafka.example.stream.druid;

import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.*;
import com.metamx.tranquility.typeclass.Timestamper;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import kafka.example.stream.util.Configurations;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by MaShangkun on 18-9-5.
 */
public class DruidBeamFactory implements Serializable {

    /**
     * @return
     */
    public Beam<Map<String, Object>> makeBeam(String dataSource) {
        final String indexService = "druid/overlord";
        final String discoveryPath = "/druid/discovery";

        final List<String> dimensions = ImmutableList.of(
                DruidDataTransformer.DRUID_FIELD_ID,
                DruidDataTransformer.DRUID_FIELD_TYPE,
                DruidDataTransformer.DRUID_FIELD_NAME
        );
        final List<AggregatorFactory> aggregators = ImmutableList.of(
                new LongSumAggregatorFactory(DruidDataTransformer.DRUID_FIELD_COUNT, DruidDataTransformer.DRUID_FIELD_COUNT),
                new LongSumAggregatorFactory(DruidDataTransformer.DRUID_FIELD_AMOUNT, DruidDataTransformer.DRUID_FIELD_AMOUNT)
        );

        final Timestamper<Map<String, Object>> timestamper = (Timestamper<Map<String, Object>>) theMap ->
                new DateTime(((Long) theMap.get(DruidDataTransformer.DRUID_FIELD_TIMESTAMP)) * 1000L, DateTimeZone.UTC);

        final CuratorFramework curator = CuratorFrameworkFactory
                .builder()
                .connectString(Configurations.getInstance().druidTranquilityZkConnect)
                .retryPolicy(new ExponentialBackoffRetry(1000, 20, 30000))
                .build();
        curator.start();

        final TimestampSpec timestampSpec = new TimestampSpec(DruidDataTransformer.DRUID_FIELD_TIMESTAMP, "posix", null);
        final Beam<Map<String, Object>> beam = DruidBeams
                .builder(timestamper)
                .curator(curator)
                .discoveryPath(discoveryPath)
                .location(DruidLocation.create(indexService, dataSource))
                .timestampSpec(timestampSpec)
                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators, QueryGranularities.HOUR))
                .tuning(
                        ClusteredBeamTuning
                                .builder()
                                .segmentGranularity(Granularity.HOUR)
                                .windowPeriod(new Period("PT10M"))
                                .partitions(1)
                                .replicants(1)
                                .build())
                .druidBeamConfig(
                        DruidBeamConfig
                                .builder()
                                .indexRetryPeriod(new Period("PT1M"))
                                .randomizeTaskId(true)
                                .build())
                .buildBeam();

        //return beam
        return beam;
    }
}