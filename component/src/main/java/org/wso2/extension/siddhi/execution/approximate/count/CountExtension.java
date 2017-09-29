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
import org.wso2.extension.siddhi.execution.approximate.cardinality.HyperLogLog;
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
 * performs HyperLogLog algorithm to get approximate cardinality
 */
@Extension(
        name = "count",
        namespace = "approximate",
        description = "Performs Count-min-sketch algorithm on a streaming data set based on a specific " +
                "relative error and cofidence. ",
        parameters = {
                @Parameter(
                        name = "value",
                        description = "The value used to find count",
                        type = {DataType.INT, DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.STRING,
                                DataType.BOOL, DataType.TIME, DataType.OBJECT}
                ),
                @Parameter(
                        name = "relative.error",
                        description = "this is the relative error for which the count is obtained",
                        type = {DataType.DOUBLE}
                ),
                @Parameter(
                        name = "confidence",
                        description = "this is the confidence for which the relative error is true",
                        type = {DataType.DOUBLE}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "count",
                        description = "Represents the count of the event after the event arrived",
                        type = {DataType.LONG}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (some_attribute int);" +
                                "from InputStream#approximate:count(some_attribute, 0.01, 0.9)\n" +
                                "select count\n" +
                                "insert into OutputStream;",
                        description = "count of events based on some_attribute is " +
                                "calculated for an relative error of 0.01 and confidence of 0.9"
                ),
        }
)
public class CountExtension extends StreamProcessor {
    private static final Logger logger = Logger.getLogger(CountExtension.class.getName());
    private double relativeError = 0.01;
    private double confidence = 0.9;
    private CountMinSketch countMinSketch;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
//       validate number of attributes
        if (attributeExpressionExecutors.length != 3) {
            throw new SiddhiAppCreationException("3 attributes are expected but " +
                    attributeExpressionExecutors.length + " attributes are found inside the count function");
        }

        //expressionExecutors[1] --> relativeError
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

        //expressionExecutors[2] --> confidence
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
            throw new SiddhiAppCreationException("confidence must be in the range of (0, 1)");
        }

        countMinSketch = new CountMinSketch(relativeError, confidence);

        List<Attribute> attributeList = new ArrayList<>(1);
        attributeList.add(new Attribute("count", Attribute.Type.LONG));
        return attributeList;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                Object newData = attributeExpressionExecutors[0].execute(streamEvent);
                if (streamEvent.getType().equals(StreamEvent.Type.CURRENT)) {
                    countMinSketch.insert(newData);

                } else if (streamEvent.getType().equals(StreamEvent.Type.EXPIRED)) {
                    countMinSketch.remove(newData);
                }
                Object[] outputData = {countMinSketch.approximateCount(newData)};

                if (outputData == null) {
                    streamEventChunk.remove();
                } else {
//                    logger.debug("Populating output");
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
            map.put("relativeError", relativeError);
            map.put("confidence", confidence);
            logger.debug("storing countMinSketch");
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
            relativeError = (Double) map.get("relativeError");
            confidence = (Double) map.get("confidence");
            countMinSketch = (CountMinSketch) map.get("countMinSketch");
        }
    }
}
