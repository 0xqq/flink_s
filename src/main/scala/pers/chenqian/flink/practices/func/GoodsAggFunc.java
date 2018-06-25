package pers.chenqian.flink.practices.func;

import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.common.accumulators.DoubleMaximum;
import org.apache.flink.api.common.functions.AggregateFunction;
import pers.chenqian.flink.practices.constants.Idx;
import pers.chenqian.flink.practices.constants.Key;

public class GoodsAggFunc implements AggregateFunction<Double[], DoubleMaximum, Double> {


    @Override
    public DoubleMaximum createAccumulator() {
        return new DoubleMaximum();
    }

    @Override
    public DoubleMaximum add(Double[] value, DoubleMaximum accumulator) {
        accumulator.add(value[Idx.USER_ID()]);
        return accumulator;
    }

    @Override
    public Double getResult(DoubleMaximum accumulator) {
        return accumulator.getLocalValue();
    }

    @Override
    public DoubleMaximum merge(DoubleMaximum a, DoubleMaximum b) {
        a.add(b.getLocalValue());
        return a;
    }

}
