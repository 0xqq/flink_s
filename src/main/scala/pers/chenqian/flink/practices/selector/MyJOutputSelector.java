package pers.chenqian.flink.practices.selector;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import pers.chenqian.flink.practices.constants.Idx;

import java.util.ArrayList;
import java.util.List;

public class MyJOutputSelector implements OutputSelector<Double[]> {


    @Override
    public Iterable<String> select(Double[] value) {
        List<String> list = new ArrayList<>();
        list.add(value[Idx.STAY_MS()].toString());

        return list;
    }
}
