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
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testApproximateCount() throws InterruptedException {
        final int noOfEvents = 1000;
        final double relativeError = 0.001;
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
                for (Event event : events) {
                    count++;
//                    if (count < 8) {
//                        realCardinality = count;
//                    } else if (count < 52) {
//                        realCardinality = 7;
//                    } else if (count < 58) {
//                        realCardinality = 52 + 6 - count;
//                    } else {
//                        realCardinality = 1;
//                    }
//                    cardinality = (long) event.getData(0);
//                    if (realCardinality >= Math.floor(cardinality - cardinality * accuracy)
//                            && realCardinality <= Math.ceil(cardinality + cardinality * accuracy)) {
//                        Assert.assertEquals(true, true);
//                    } else {
////                        System.out.println("realCardinality : " + realCardinality + ", cardinality : " + cardinality);
//                        Assert.assertEquals(true, false);
////                        System.out.println("error : " + ((double) (count - cardinality) / cardinality));
//                    }
//
//                    System.out.println("realCardinality : " + realCardinality + ", cardinality : " + cardinality);
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

//        for (double j = 0; j < 50; j++) {
//            inputHandler.send(new Object[]{j % 10});
//        }

        for (int j = 0; j < noOfEvents; j++) {
            inputHandler.send(new Object[]{j%100});
            Thread.sleep(1);
        }

        Thread.sleep(100);
        Assert.assertEquals(noOfEvents, count);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}

