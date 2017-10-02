package org.wso2.extension.siddhi.execution.approximate.count;


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

public class CountTestCase {
    static final Logger LOG = Logger.getLogger(CountTestCase.class);
    private volatile int arrivedEvents;
    private volatile boolean eventArrived;
    private long count;

    @Before
    public void init() {
        arrivedEvents = 0;
        eventArrived = false;
    }

    @Test
    public void testApproximateCount() throws InterruptedException {
        final int noOfEvents = 1000;
        final double relativeError = 0.00001;
        final double confidence = 0.99;

        LOG.info("Approximate Cardinality Test Case");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(100)#approximate:count(number, "
                + relativeError + ", " + confidence + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
//                for (Event event : events) {
//                    arrivedEvents++;
//                    count = (long) event.getData(1);
//                    if (count <= 1 + relativeError * arrivedEvents) {
//                        Assert.assertEquals(true, true);
//                    } else {
//                        Assert.assertEquals(true, false);
//                    }
//                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

//        for (double j = 0; j < 50; j++) {
//            inputHandler.send(new Object[]{j % 10});
//        }

        for (int j = 0; j < noOfEvents; j++) {
            inputHandler.send(new Object[]{j % 100});
            Thread.sleep(1);
        }

        Thread.sleep(100);
//        Assert.assertEquals(noOfEvents, arrivedEvents);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}

