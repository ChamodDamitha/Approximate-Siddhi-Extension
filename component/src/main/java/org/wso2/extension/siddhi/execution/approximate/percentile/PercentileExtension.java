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

package org.wso2.extension.siddhi.execution.approximate.percentile;

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
 * performs TDigest algorithm to get approximate percentiles
 */
@Extension(
        name = "percentile",
        namespace = "approximate",
        description = "Performs TDigest algorithm on a streaming data set based on a specific accuracy. ",
        parameters = {
                @Parameter(
                        name = "value",
                        description = "The value to calculate percentiles",
                        type = {DataType.INT, DataType.DOUBLE, DataType.FLOAT, DataType.LONG}
                ),
                @Parameter(
                        name = "percentileposition",
                        description = "The position of the percentile to be calculated",
                        type = {DataType.DOUBLE}
                ),
                @Parameter(
                        name = "accuracy",
                        description = "this is the accuracy for which the similarity is obtained",
                        type = {DataType.DOUBLE}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "percentile",
                        description = "Represents the percentile value calculated",
                        type = {DataType.DOUBLE}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (some_attribute int);" +
                                "from InputStream#approximate:similarity(some_attribute, 0.5, 0.01)\n" +
                                "select percentile\n" +
                                "insert into OutputStream;",
                        description = "calculate 50th percentile of values of some_attribute for an accuracy of 0.01"
                ),
        }
)
public class PercentileExtension extends StreamProcessor {
    private static final Logger logger = Logger.getLogger(PercentileExtension.class.getName());
    private double accuracy = 0.01;
    private double percentilePosition;
    PercentileCalculator percentileCalculator;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
//      validate number of attributes
        if (attributeExpressionExecutors.length != 3) {
            throw new SiddhiAppCreationException("3 attributes are expected but " +
                    attributeExpressionExecutors.length + " attributes are found inside the percentile function");
        }

//      expressionExecutors[1] --> accurracy
        if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppCreationException("accuracy has to be a constant but found " +
                    this.attributeExpressionExecutors[2].getClass().getCanonicalName());
        }

        if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.DOUBLE) {
            accuracy = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
        } else {
            throw new SiddhiAppCreationException("accuracy must be of type Double but found " +
                    attributeExpressionExecutors[2].getReturnType());
        }

        if ((accuracy <= 0) || (accuracy >= 1)) {
            throw new SiddhiAppCreationException("accuracy must be in the range of (0, 1) but found " + accuracy);
        }

//      validate second attribute
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppCreationException("percentile position has to be a constant but found " +
                    this.attributeExpressionExecutors[1].getClass().getCanonicalName());
        }

        if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
            percentilePosition = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
        } else {
            throw new SiddhiAppCreationException("percentile position must be of type Double but found " +
                    attributeExpressionExecutors[1].getReturnType());
        }

        if ((percentilePosition <= 0) || (percentilePosition >= 1)) {
            throw new SiddhiAppCreationException("percentile position must be in the range of (0, 1)" +
                    " but found " + accuracy);
        }

//      validate first attribute
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.INT &&
                attributeExpressionExecutors[0].getReturnType() != Attribute.Type.LONG &&
                attributeExpressionExecutors[0].getReturnType() != Attribute.Type.DOUBLE &&
                attributeExpressionExecutors[0].getReturnType() != Attribute.Type.FLOAT) {
            throw new SiddhiAppCreationException("first attribute inside similarity function" +
                    " must be of type INT or LONG or DOUBLE or FLOAT but found " +
                    this.attributeExpressionExecutors[0].getReturnType());
        }

        percentileCalculator = new PercentileApproximator();
        percentileCalculator.initialize(percentilePosition, accuracy);

        List<Attribute> attributeList = new ArrayList<>(1);
        attributeList.add(new Attribute("percentile", Attribute.Type.DOUBLE));
        return attributeList;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                double value;
                if (attributeExpressionExecutors[0].execute(streamEvent).getClass().equals(Integer.class)) {
                    value = ((Integer) attributeExpressionExecutors[0].execute(streamEvent)) / 1.0;
                } else if (attributeExpressionExecutors[0].execute(streamEvent).getClass().equals(Long.class)) {
                    value = ((Long) attributeExpressionExecutors[0].execute(streamEvent)) / 1.0;
                } else if (attributeExpressionExecutors[0].execute(streamEvent).getClass().equals(Float.class)) {
                    value = ((Float) attributeExpressionExecutors[0].execute(streamEvent)) / 1.0;
                } else {
                    value = (Double) attributeExpressionExecutors[0].execute(streamEvent);
                }
                if(streamEvent.getType().equals(StreamEvent.Type.CURRENT)) {
                    percentileCalculator.add(value);
                }
                else if(streamEvent.getType().equals(StreamEvent.Type.EXPIRED)) {
                    percentileCalculator.remove(value);
                }
                Object[] outputData = {percentileCalculator.getPercentile(percentilePosition)};

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
            map.put("percentileCalculator", percentileCalculator);
            map.put("accuracy", accuracy);
            map.put("percentilePosition", percentilePosition);
            logger.debug("storing Percentile Calculator");
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
            accuracy = (Double) map.get("accuracy");
            percentilePosition = (Double) map.get("percentilePosition");
            percentileCalculator = (PercentileCalculator) map.get("percentileCalculator");
        }
    }
}
