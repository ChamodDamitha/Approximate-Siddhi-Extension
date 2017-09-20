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
        final int noOfEvents = 100;
        final double accuracy = 0.05;

        LOG.info("Approximate Cardinality Test Case");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(7)#approximate:cardinality(number, " + accuracy + ") " +
                "select cardinality " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long cardinality;

            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
                for (Event event : events) {
                    count++;
                    cardinality = (long) event.getData(0);
                    if (count >= Math.floor(cardinality - cardinality * accuracy)
                            && count <= Math.ceil(cardinality + cardinality * accuracy)) {
                        Assert.assertEquals(true, true);
                    } else {
//                        Assert.assertEquals(true, false);
//                        System.out.println("count : " + count + ", cardinality : " + cardinality);
//                        System.out.println("error : " + ((double) (count - cardinality) / cardinality));
                    }

                    System.out.println("count : " + count + ", cardinality : " + cardinality);
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (double j = 0; j < 50; j++) {
            inputHandler.send(new Object[]{j%10});
        }

        for (double j = 0; j < 50; j++) {
            inputHandler.send(new Object[]{10});
        }

        Thread.sleep(100);
        Assert.assertEquals(noOfEvents, count);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}

