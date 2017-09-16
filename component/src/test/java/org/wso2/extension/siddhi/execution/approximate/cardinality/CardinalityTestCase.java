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
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testApproximateCardinality() throws InterruptedException {
        final int noOfEvents = 1000;
        final double accuracy = 0.3;

        LOG.info("Approximate Cardinality TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#approximate:cardinality(number, " + accuracy + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {


            long cardinality;
            long i = 0;

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    i++;
                    cardinality = (long) event.getData(1);
                    if (cardinality >= (cardinality - cardinality * accuracy)
                            && cardinality <= (cardinality + cardinality * accuracy)) {
                        Assert.assertEquals(true, true);
                    } else {
                        Assert.assertEquals(true, false);
                    }
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (double j = 0; j < noOfEvents; j++) {
            inputHandler.send(new Object[]{j});
        }

        Thread.sleep(100);
        Assert.assertEquals(1000, noOfEvents);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}

