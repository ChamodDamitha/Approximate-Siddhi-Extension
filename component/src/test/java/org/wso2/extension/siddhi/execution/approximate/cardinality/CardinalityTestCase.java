package org.wso2.extension.siddhi.execution.approximate.cardinality;


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

public class CardinalityTestCase {
    static final Logger LOG = Logger.getLogger(CardinalityTestCase.class);
    private volatile int totalCount;
    private volatile int validCount;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        totalCount = 0;
        validCount = 0;
        eventArrived = false;
    }

    @Test
    public void testApproximateCardinality() throws InterruptedException {
        final int noOfEvents = 10000;
        final double relativeError = 0.005;

        LOG.info("Approximate Cardinality Test Case");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#approximate:cardinality(number, " + relativeError + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long cardinality;

            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
                for (Event event : events) {
                    totalCount++;
                    cardinality = (long) event.getData(1);
                    if (totalCount >= Math.floor(cardinality - cardinality * relativeError)
                            && totalCount <= Math.ceil(cardinality + cardinality * relativeError)) {
                        validCount++;
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();


        for (int j = 0; j < noOfEvents; j++) {
            inputHandler.send(new Object[]{j});
            Thread.sleep(1);
        }

        Thread.sleep(100);
        Assert.assertEquals(noOfEvents, totalCount);
        Assert.assertTrue(eventArrived);

//        System.out.println("(double) validCount / totalCount : " + ((double) validCount / totalCount));//TODO : testing

//      confidence check
        if ((double) validCount / totalCount >= 0.95) {
            Assert.assertEquals(true, true);
        } else {
            Assert.assertEquals(true, false);
        }

        siddhiAppRuntime.shutdown();
    }
}

