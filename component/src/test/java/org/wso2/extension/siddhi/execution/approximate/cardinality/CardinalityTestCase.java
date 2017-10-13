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
    private final int noOfEvents = 1000;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        totalCount = 0;
        validCount = 0;
        eventArrived = false;
    }

    @Test
    public void testApproximateCardinality_1() throws InterruptedException {
        final double relativeError = 0.01;
        final double confidence = 0.95;

        LOG.info("Approximate Cardinality Test Case - default relative error(" + relativeError + ") " +
                "and confidence(" + confidence + ")");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#approximate:distinctCount(number) " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long lowerBound;
            long upperBound;

            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
                for (Event event : events) {
                    totalCount++;
                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);
                    if (totalCount >= lowerBound && totalCount <= upperBound) {
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

        System.out.println("Real Confidence : " + ((double) validCount / totalCount));//TODO : testing

//      confidence check
        if ((double) validCount / totalCount >= confidence) {
            Assert.assertEquals(true, true);
        } else {
            Assert.assertEquals(true, false);
        }

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testApproximateCardinality_2() throws InterruptedException {
        final double relativeError = 0.05;
        final double confidence = 0.95;

        LOG.info("Approximate Cardinality Test Case - specified relative error(" + relativeError + ")" +
                " and default confidence(" + confidence + ")");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#approximate:cardinality(number, " + relativeError + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long lowerBound;
            long upperBound;

            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
                for (Event event : events) {
                    totalCount++;
                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);
                    if (totalCount >= lowerBound && totalCount <= upperBound) {
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

        System.out.println("Real Confidence : " + ((double) validCount / totalCount));//TODO : testing

//      confidence check
        if ((double) validCount / totalCount >= confidence) {
            Assert.assertEquals(true, true);
        } else {
            Assert.assertEquals(true, false);
        }

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testApproximateCardinality_3() throws InterruptedException {
        final double relativeError = 0.05;
        final double confidence = 0.65;

        LOG.info("Approximate Cardinality Test Case - specified relative error(" + relativeError + ")" +
                " and specified confidence(" + confidence + ")");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#approximate:cardinality(number, " + relativeError + ", " + confidence + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long cardinality;
            long lowerBound;
            long upperBound;

            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
                for (Event event : events) {
                    totalCount++;
                    cardinality = (long) event.getData(1);
                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);
                    if (totalCount >= lowerBound && totalCount <= upperBound) {
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

        System.out.println("Real Confidence : " + ((double) validCount / totalCount));//TODO : testing

//      confidence check
        if ((double) validCount / totalCount >= confidence) {
            Assert.assertEquals(true, true);
        } else {
            Assert.assertEquals(true, false);
        }

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testApproximateCardinality_4() throws InterruptedException {

        final int windowLength = 500;
        final double relativeError = 0.01;
        final double confidence = 0.95;

        LOG.info("Approximate Cardinality Test Case - for Siddhi length window - " +
                "default relative error(" + relativeError + ") and confidence(" + confidence + ")");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (number int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.length(" + windowLength + ")#approximate:cardinality(number) " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long cardinality;
            long exactCardinality;
            long lowerBound;
            long upperBound;

            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
                for (Event event : events) {
                    totalCount++;
                    if (totalCount < windowLength) {
                        exactCardinality = totalCount;
                    } else {
                        exactCardinality = windowLength;
                    }

                    cardinality = (long) event.getData(1);
                    lowerBound = (long) event.getData(2);
                    upperBound = (long) event.getData(3);
                    if (exactCardinality >= lowerBound && exactCardinality <= upperBound) {
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

        System.out.println("Real Confidence : " + ((double) validCount / totalCount));//TODO : testing

//      confidence check
        if ((double) validCount / totalCount >= 0.95) {
            Assert.assertEquals(true, true);
        } else {
            Assert.assertEquals(true, false);
        }

        siddhiAppRuntime.shutdown();
    }
}

