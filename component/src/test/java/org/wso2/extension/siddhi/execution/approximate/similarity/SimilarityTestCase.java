package org.wso2.extension.siddhi.execution.approximate.similarity;


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

public class SimilarityTestCase {
    static final Logger LOG = Logger.getLogger(SimilarityTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testApproximateSimilarity() throws InterruptedException {
        final int noOfEvents = 1000;
        final double accuracy = 0.3;

        LOG.info("Approximate Similarity Test Case");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (attribute_1 int, attribute_2 int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#approximate:similarity(attribute_1, attribute_2, " + accuracy + ") " +
                "select * " +
                "insert into outputStream;");

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            long cardinality;

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count++;
                }
                eventArrived = true;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        for (double j = 0; j < noOfEvents; j++) {
            inputHandler.send(new Object[]{j + 1, j});
        }

        Thread.sleep(100);
        Assert.assertEquals(noOfEvents, count);
        Assert.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}

