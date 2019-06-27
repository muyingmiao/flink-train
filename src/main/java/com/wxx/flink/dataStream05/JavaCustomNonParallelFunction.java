package com.wxx.flink.dataStream05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class JavaCustomNonParallelFunction implements SourceFunction<Long> {

    boolean isRunning = true;
    long counter = 1L;

    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning){
            sourceContext.collect(counter);
            counter +=1;
            Thread.sleep(1000);
        }
    }

    public void cancel() {
        isRunning = false;
    }
}
