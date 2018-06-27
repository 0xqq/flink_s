package pers.chenqian.flink.practices.tsAndWm;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class MyJTimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Double[]> {

    long maxOutOfOrderness = 3500L; // 3.5 seconds

    long currentMaxTimestamp = 0L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Double[] element, long previousElementTimestamp) {
        long timestamp = element[element.length - 1].longValue();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}
