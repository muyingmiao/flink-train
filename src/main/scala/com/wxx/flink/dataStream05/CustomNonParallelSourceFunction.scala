package com.wxx.flink.dataStream05

import org.apache.flink.streaming.api.functions.source.SourceFunction

class CustomNonParallelSourceFunction extends SourceFunction[Long]{
  var counter = 1;

  var isRunning = true;
  override def cancel() = {isRunning = false}

  override def run(sourceContext: SourceFunction.SourceContext[Long]) = {
    while (isRunning){
      sourceContext.collect(counter)
      counter+=1
      Thread.sleep(1000)
    }
  }
}
