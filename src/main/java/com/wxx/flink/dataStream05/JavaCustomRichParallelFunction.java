package com.wxx.flink.dataStream05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class JavaCustomRichParallelFunction extends RichParallelSourceFunction<Long> {

    boolean isRunning = true;
    long counter = 1L;

    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning){
            sourceContext.collect(counter);
            counter +=1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    public void cancel() {
        isRunning = false;
    }
}
