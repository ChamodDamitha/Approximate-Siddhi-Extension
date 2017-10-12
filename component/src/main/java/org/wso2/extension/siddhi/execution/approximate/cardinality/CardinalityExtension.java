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

package org.wso2.extension.siddhi.execution.approximate.cardinality;

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
 * performs HyperLogLog algorithm to get approximate cardinality
 */
@Extension(
        name = "cardinality",
        namespace = "approximate",
        description = "Performs HyperLogLog algorithm on a streaming data set based on a specific relative error" +
                " and a confidence value to calculate the unique count of the events(cardinality).",
        parameters = {
                @Parameter(
                        name = "value",
                        description = "The value used to find cardinality",
                        type = {DataType.INT, DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.STRING,
                                DataType.BOOL, DataType.TIME, DataType.OBJECT}
                ),
                @Parameter(
                        name = "relative.error",
                        description = "this is the relative error for which the cardinality is obtained",
                        type = {DataType.DOUBLE},
                        optional = true,
                        defaultValue = "0.01"
                ),
                @Parameter(
                        name = "confidence",
                        description = "this is the confidence for which the relative error is true",
                        type = {DataType.DOUBLE},
                        optional = true,
                        defaultValue = "0.95"
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "cardinality",
                        description = "Represents the cardinality after the event arrived",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "lowerBound",
                        description = "Represents the lower bound of the cardinality after the event arrived",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "upperBound",
                        description = "Represents the upper bound of the cardinality after the event arrived",
                        type = {DataType.LONG}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (some_attribute int);" +
                                "from InputStream#approximate:cardinality(some_attribute)\n" +
                                "select cardinality\n" +
                                "insert into OutputStream;",
                        description = "cardinality of events in a stream based on some_attribute is " +
                                "calculated for a default relative error of 0.01 and  a default confidence of 0.95"
                ),
                @Example(
                        syntax = "define stream InputStream (some_attribute int);" +
                                "from InputStream#approximate:cardinality(some_attribute, 0.05)\n" +
                                "select cardinality\n" +
                                "insert into OutputStream;",
                        description = "cardinality of events in a stream based on some_attribute is " +
                                "calculated for a relative error of 0.05 and a default confidence of 0.95"
                ),
                @Example(
                        syntax = "define stream InputStream (some_attribute int);" +
                                "from InputStream#approximate:cardinality(some_attribute, 0.05, 0.65)\n" +
                                "select cardinality\n" +
                                "insert into OutputStream;",
                        description = "cardinality of events in a stream based on some_attribute is " +
                                "calculated for a relative error of 0.05 and a confidence of 0.65"
                ),
                @Example(
                        syntax = "define stream InputStream (some_attribute int);" +
                                "from InputStream#window.length(1000)" +
                                "#approximate:cardinality(some_attribute, 0.05, 0.65)\n" +
                                "select cardinality\n" +
                                "insert into OutputStream;",
                        description = "cardinality of events in a length window based on some_attribute is " +
                                "calculated for a relative error of 0.05 and a confidence of 0.65"
                ),
        }
)
public class CardinalityExtension extends StreamProcessor {
    private static final Logger logger = Logger.getLogger(CardinalityExtension.class.getName());
    private HyperLogLog hyperLogLog;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {

//      default values for relative error and confidence
        double relativeError = 0.01;
        double confidence = 0.95;

//       validate number of attributes
        if (!(attributeExpressionExecutors.length >= 1 && attributeExpressionExecutors.length <= 3)) {
            throw new SiddhiAppCreationException("1 - 3 attributes are expected but " +
                    attributeExpressionExecutors.length + " attributes are found inside the cardinality function");
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

            if (confidence != 0.65 && confidence != 0.95 && confidence != 0.99) {
                throw new SiddhiAppCreationException("confidence must be a value from 0.65, 0.95 and 0.99");
            }
        }

        hyperLogLog = new HyperLogLog(relativeError, confidence);

        List<Attribute> attributeList = new ArrayList<>(3);
        attributeList.add(new Attribute("cardinality", Attribute.Type.LONG));
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
                Object newData = attributeExpressionExecutors[0].execute(streamEvent);
                if (streamEvent.getType().equals(StreamEvent.Type.CURRENT)) {
                    hyperLogLog.addItem(newData);

                } else if (streamEvent.getType().equals(StreamEvent.Type.EXPIRED)) {
                    hyperLogLog.removeItem(newData);
                }

                Object[] outputData = {hyperLogLog.getCardinality(), hyperLogLog.getConfidenceInterval()[0],
                        hyperLogLog.getConfidenceInterval()[1]};

                if (outputData == null) {
                    streamEventChunk.remove();
                } else {
                    logger.debug("Populating output");
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
            map.put("hyperLogLog", hyperLogLog);
            logger.debug("storing hyperLogLog");
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
            hyperLogLog = (HyperLogLog) map.get("hyperLogLog");
        }
    }
}
