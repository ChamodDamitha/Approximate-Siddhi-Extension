/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.approximate.count;

import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * performs Count-Min Sketch algorithm to get the approximate count(frequency) of events.
 */
@Extension(
        name = "count",
        namespace = "approximate",
        description = "Performs Count-min-sketch algorithm on a streaming data set based on a specific " +
                "relative error and  a confidence value to calculate the approximate count(frequency) of events." +
                "The default relative error is set as 1%(0.01) and the default confidence is taken as 99%(0.99).",
        parameters = {
                @Parameter(
                        name = "value",
                        description = "The value used to find the count",
                        type = {DataType.INT, DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.STRING,
                                DataType.BOOL, DataType.TIME, DataType.OBJECT}
                ),
                @Parameter(
                        name = "relative.error",
                        description = "This is the relative error for which the count is obtained. " +
                                "The values must be in the range of (0, 1).",
                        type = {DataType.DOUBLE},
                        optional = true,
                        defaultValue = "0.01"
                ),
                @Parameter(
                        name = "confidence",
                        description = "This is the confidence for which the relative error is true. " +
                                "The values must be in the range of (0, 1).",
                        type = {DataType.DOUBLE},
                        optional = true,
                        defaultValue = "0.99"
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "approximate.count",
                        description = "Represents the approximate count of the event after the event arrived",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "lowerBound",
                        description = "Represents the lower bound of the count after the event arrived",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "upperBound",
                        description = "Represents the upper bound of the count after the event arrived",
                        type = {DataType.LONG}
                )
        },
        examples = { //TODO : examples with windows only and warning , two types of windows
                @Example(
                        syntax = "define stream InputStream (some_attribute int);" +
                                "from InputStream#approximate:count(some_attribute)\n" +
                                "select count\n" +
                                "insert into OutputStream;",
                        description = "count of events based on some_attribute is " +
                                "calculated for a default relative error of 0.01 and a default confidence of 0.99"
                ),
                @Example(
                        syntax = "define stream InputStream (some_attribute int);" +
                                "from InputStream#approximate:count(some_attribute, 0.05)\n" +
                                "select count\n" +
                                "insert into OutputStream;",
                        description = "count of events based on some_attribute is " +
                                "calculated for an relative error of 0.05 and a default confidence of 0.99"
                ),
                @Example(
                        syntax = "define stream InputStream (some_attribute int);" +
                                "from InputStream#approximate:count(some_attribute, 0.05, 0.9)\n" +
                                "select count\n" +
                                "insert into OutputStream;",
                        description = "count of events based on some_attribute is " +
                                "calculated for an relative error of 0.05 and a confidence of 0.9"
                ),
                @Example(
                        syntax = "define stream InputStream (some_attribute int);" +
                                "from InputStream#window.length(1000)#approximate:count(some_attribute, 0.05, 0.9)\n" +
                                "select count\n" +
                                "insert into OutputStream;",
                        description = "count of events in a length window based on some_attribute is " +
                                "calculated for an relative error of 0.05 and a confidence of 0.9"
                ),
        }
)
public class CountExtension extends StreamProcessor {
    private static final Logger logger = Logger.getLogger(CountExtension.class.getName());

    private CountMinSketch countMinSketch;

    private long[] approximateCount = new long[3];

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

//      default values for relative error and confidence
        double relativeError = 0.01;
        double confidence = 0.99;

//       validate number of attributes
        if (!(attributeExpressionExecutors.length >= 1 && attributeExpressionExecutors.length <= 3)) {
            throw new SiddhiAppCreationException("1 - 3 attributes are expected but " +
                    attributeExpressionExecutors.length + " attributes are found inside the count function");
        }

        //expressionExecutors[1] --> relativeError
        if (attributeExpressionExecutors.length > 1) {

            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("relative error has to be a constant but found " +
                        this.attributeExpressionExecutors[1].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
                relativeError = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new SiddhiAppCreationException("relative error should be of type Double but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

            if ((relativeError <= 0) || (relativeError >= 1)) {
                throw new SiddhiAppCreationException("relative error must be in the range of (0, 1)");
            }
        }

        //expressionExecutors[2] --> confidence
        if (attributeExpressionExecutors.length > 2) {

            if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("confidence has to be a constant but found " +
                        this.attributeExpressionExecutors[2].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.DOUBLE) {
                confidence = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new SiddhiAppCreationException("confidence should be of type Double but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }

            if ((confidence <= 0) || (confidence >= 1)) {
                throw new SiddhiAppCreationException("relative error must be in the range of (0, 1)");
            }
        }

        countMinSketch = new CountMinSketch(relativeError, confidence);

        List<Attribute> attributeList = new ArrayList<>(3);
        attributeList.add(new Attribute("count", Attribute.Type.LONG));
        attributeList.add(new Attribute("lowerBound", Attribute.Type.LONG));
        attributeList.add(new Attribute("upperBound", Attribute.Type.LONG));
        return attributeList;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                Object newData = attributeExpressionExecutors[0].execute(streamEvent); //TODO : check null
                if (streamEvent.getType().equals(StreamEvent.Type.CURRENT)) {
                    approximateCount = countMinSketch.insert(newData); //TODO : object for E
                } else if (streamEvent.getType().equals(StreamEvent.Type.EXPIRED)) {
                    approximateCount = countMinSketch.remove(newData);
                } //TODO : rest event -> clear everything

//              outputData = {count, lower bound, upper bound}
                Object[] outputData = {approximateCount[0], approximateCount[1], approximateCount[2]}; // TODO : two methods to count and bounds

                if (outputData == null) {
                    streamEventChunk.remove(); //TODO : remove if check ,
                } else {
                    logger.debug("Populating output");//tODO : remove
                    complexEventPopulater.populateComplexEvent(streamEvent, outputData);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Map<String, Object> currentState() {
        synchronized (this) {
            Map<String, Object> map = new HashMap();
            map.put("countMinSketch", countMinSketch);
            logger.debug("storing countMinSketch");
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
            countMinSketch = (CountMinSketch) map.get("countMinSketch");
        }
    }
}
