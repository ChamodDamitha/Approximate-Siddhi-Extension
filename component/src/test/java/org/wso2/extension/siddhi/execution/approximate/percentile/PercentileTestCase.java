package org.wso2.extension.siddhi.execution.approximate.percentile;


import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class PercentileTestCase {
    static final Logger LOG = Logger.getLogger(PercentileTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testApproximatePercentile() throws InterruptedException {
        final int noOfEvents = 10;
        final double accuracy = 0.3;
        final double percentilePosition = 0.5;

        LOG.info("Approximate Percentile Test Case");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number double);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(5)#approximate:percentile(number, " + percentilePosition + ", " + accuracy + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            double percentile;

            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
                for (Event event : events) {
                    percentile = (double) event.getData(1);
//                    if (count/2.0 >= (percentile - percentile * accuracy)
//                            && count/2.0 <= (percentile + percentile * accuracy)) {
//                        Assert.assertEquals(true, true);
//                    } else {
////                        System.out.println("percentile : " + percentile + ", count/2.0 : " + (count/2.0));
//                        Assert.assertEquals(true, false);
//                    }
                    System.out.println("percentile : " + percentile);
                    count++;
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (double j = 1; j <= noOfEvents; j++) {
            inputHandler.send(new Object[]{j});
        }

        Thread.sleep(100);
        Assert.assertEquals(noOfEvents, count);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}

