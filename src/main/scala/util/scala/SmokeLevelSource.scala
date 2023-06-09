package util.scala

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

/**
 * Flink SourceFunction to generate random SmokeLevel events.
 */
class SmokeLevelSource extends SourceFunction[SmokeLevel] {

    // flag indicating whether source is still running
    private var running = true

    /**
     * Continuously emit one smoke level event per second.
     */
    override def run(srcCtx: SourceContext[SmokeLevel]): Unit = {

        // initialize random number generator
        val rand = new Random()

        // emit data until being canceled
        while (running) {

            if (rand.nextGaussian() > 0.8) srcCtx.collect(SmokeLevel.HIGH)
            else srcCtx.collect(SmokeLevel.LOW)

            // wait for 1 second
            Thread.sleep(1000)
        }
    }

    /**
     * Cancel the emission of smoke level events.
     */
    override def cancel(): Unit = this.running = false
}