package org.wso2.extension.siddhi.execution.approximate.count;


import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class CountTestCase {
    static final Logger LOG = Logger.getLogger(CountTestCase.class);
    private final int totalEventsSent = 2000;
    private final int noOfUniqueEvents = 100;

    private volatile int totalEventsArrived;
    private volatile int validEvents;
    private volatile boolean eventArrived;
    private long exactCount;
    private long lowerBound;
    private long upperBound;

    @BeforeMethod
    public void init() {
        totalEventsArrived = 0;
        validEvents = 0;
        eventArrived = false;
    }



    @Test
    public void testApproximateCount_1() throws InterruptedException {
        final int windowLength = 1000;

        final double confidence = 0.75;
        final double relativeError = 0.005;

        LOG.info("Approximate Cardinality Test Case - for Siddhi length window - " +
                "a specified relative error(" + relativeError + ") and a confidence(" + confidence + ")");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:count(number, "
                + relativeError + ", " + confidence + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
                for (Event event : events) {
                    totalEventsArrived++;

                    if (totalEventsArrived < windowLength) {
                        exactCount = (totalEventsArrived / noOfUniqueEvents) + 1;
                    } else {
                        exactCount = windowLength / noOfUniqueEvents;
                    }

                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);

                    if (exactCount >= lowerBound && exactCount <= upperBound) {
                        validEvents++;
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();


        for (int j = 0; j < totalEventsSent; j++) {
            inputHandler.send(new Object[]{j % noOfUniqueEvents});
            Thread.sleep(1);
        }

        Thread.sleep(100);
        Assert.assertEquals(totalEventsSent, totalEventsArrived);
        Assert.assertTrue(eventArrived);

        System.out.println("Retained Confidence : "
                + ((double) validEvents / totalEventsArrived));//TODO : testing

//      confidence test
        if ((double) validEvents / totalEventsArrived >= confidence) {
            Assert.assertEquals(true, true);
        } else {
            Assert.assertEquals(true, false);
        }

        siddhiAppRuntime.shutdown();
    }
}

