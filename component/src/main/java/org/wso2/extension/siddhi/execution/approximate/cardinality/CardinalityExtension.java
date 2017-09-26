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
import org.wso2.siddhi.core.event.ComplexEvent;
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
        description = "Performs HyperLogLog algorithm on a streaming data set based on a specific accuracy. ",
        parameters = {
                @Parameter(
                        name = "value",
                        description = "The value used to find cardinality",
                        type = {DataType.INT, DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.STRING,
                                DataType.BOOL, DataType.TIME, DataType.OBJECT}
                ),
                @Parameter(
                        name = "accuracy",
                        description = "this is the accuracy for which the cardinality is obtained",
                        type = {DataType.DOUBLE}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "cardinality",
                        description = "Represents the cardinality after the event arrived",
                        type = {DataType.LONG}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (some_attribute int);" +
                                "from InputStream#approximate:cardinality(some_attribute, 0.01)\n" +
                                "select cardinality\n" +
                                "insert into OutputStream;",
                        description = "cardinality of events based on some_attribute is " +
                                "calculated for an accuracy of 0.01"
                ),
        }
)
public class CardinalityExtension extends StreamProcessor {
    private static final Logger logger = Logger.getLogger(CardinalityExtension.class.getName());
    private double accuracy = 0.01;
    private HyperLogLog hyperLogLog;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
//       validate number of attributes
        if (attributeExpressionExecutors.length != 2) {
            throw new SiddhiAppCreationException("2 attributes are expected but " +
                    attributeExpressionExecutors.length + " attributes are found inside the cardinality function");
        }

        //expressionExecutors[1] --> accurracy
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppCreationException("accuracy has to be a constant but found " +
                    this.attributeExpressionExecutors[1].getClass().getCanonicalName());
        }

        if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
            accuracy = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
        } else {
            throw new SiddhiAppCreationException("accuracy should be of type Double but found " +
                    attributeExpressionExecutors[1].getReturnType());
        }

        if ((accuracy <= 0) || (accuracy >= 1)) {
            throw new SiddhiAppCreationException("accuracy must be in the range of (0, 1)");
        }

        hyperLogLog = new HyperLogLog(accuracy);

        List<Attribute> attributeList = new ArrayList<>(1);
        attributeList.add(new Attribute("cardinality", Attribute.Type.LONG));
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
                Object[] outputData = {hyperLogLog.getCardinality()};
//                Object[] outputData = {3000};

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
            map.put("hyperLogLog", hyperLogLog);
            map.put("accuracy", accuracy);
            logger.debug("storing hyperLogLog");
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
            accuracy = (Double) map.get("accuracy");
            hyperLogLog = (HyperLogLog) map.get("hyperLogLog");
        }
    }
}
