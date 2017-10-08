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
    private volatile int totalEvents;
    private volatile int validEvents;
    private volatile int eventsInsideWindow;
    private volatile boolean eventArrived;
    private long count;

    @Before
    public void init() {
        totalEvents = 0;
        validEvents = 0;
        eventsInsideWindow = 0;
        eventArrived = false;
    }

    @Test
    public void testApproximateCount() throws InterruptedException {
        final int noOfEvents = 10000;
        final double relativeError = 0.001;
        final double confidence = 0.99;

        LOG.info("Approximate Cardinality Test Case");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(1000)#approximate:count(number, "
                + relativeError + ", " + confidence + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    totalEvents++;
                    if (totalEvents < 1000) {
                        eventsInsideWindow = totalEvents;
                    } else {
                        eventsInsideWindow = 1000;
                    }
                    count = (long) event.getData(1);
                    if (count <= 1 + relativeError * eventsInsideWindow) {
                        validEvents++;
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
        Assert.assertEquals(noOfEvents, totalEvents);
        Assert.assertTrue(eventArrived);

        System.out.println("(double) validEvents / totalEvents : " + ((double) validEvents / totalEvents));

//      confidence test
        if ((double) validEvents / totalEvents >= confidence) {
            Assert.assertEquals(true, true);
        } else {
            Assert.assertEquals(true, false);
        }

        siddhiAppRuntime.shutdown();
    }
}

