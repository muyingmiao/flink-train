package com.wxx.flink.course05

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class CustomRichParallelSourceFunction extends RichParallelSourceFunction[Long]{

  var isRunning = true

  var counter = 1L
  override def cancel() = {
    isRunning = false
  }

  override def run(sourceContext: SourceFunction.SourceContext[Long]) = {
    while(isRunning){
      sourceContext.collect(counter)
      counter +=1
      Thread.sleep(1000)
    }
  }
}
