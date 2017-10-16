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

package org.wso2.extension.siddhi.execution.approximate.ditinctCountEver;

import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.execution.approximate.distinctCount.HyperLogLog;
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

//TODO : two extensions distinctCount, distinctCountEver

/**
 * Performs HyperLogLog algorithm to get the approximate distinctCount of events in a stream.
 */
@Extension(
        name = "distinctCountEver",
        namespace = "approximate",
        description = "Performs HyperLogLog algorithm on a streaming data set based on a specific relative error" +
                " and a confidence value to calculate the number of distinct events.", //TODO : reduce info - done
        parameters = { //TODO : define distinctCount - done
                @Parameter(
                        name = "value",
                        description = "The value used to find distinctCount",
                        type = {DataType.INT, DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.STRING,
                                DataType.BOOL, DataType.TIME, DataType.OBJECT}
                ),
                @Parameter(
                        name = "relative.error",
                        description = "This is the relative error for which the distinct count is obtained. " +
                                "The values must be in the range of (0, 1).",
                        type = {DataType.DOUBLE},
                        optional = true,
                        defaultValue = "0.01"
                ),
                @Parameter(
                        name = "confidence",
                        description = "This is the confidence for which the relative error is true. " +
                                "The value must be one out of 0.65, 0.95, 0.99.",
                        type = {DataType.DOUBLE},
                        optional = true,
                        defaultValue = "0.95"
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "distinctCountEver",
                        description = "Represents the distinct count considering the last event ",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "distinctCountEverLowerBound",
                        description = "Represents the lower bound of the distinct count considering the last event",
                        type = {DataType.LONG}
                ),
                @ReturnAttribute(
                        name = "distinctCountEverUpperBound",
                        description = "Represents the upper bound of the distinct count considering the last event",
                        type = {DataType.LONG}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (someAttribute int);\n" +
                                //TODO : \n after every line, camel case - done
                                "from InputStream#approximate:distinctCountEver(someAttribute)\n" +
                                "select distinctCountEver, distinctCountEverLowerBound, distinctCountEverUpperBound\n" +
                                "insert into OutputStream;\n",
                        description = "Distinct count of events in a stream based on someAttribute is " +
                                "calculated for a default relative error of 0.01 and a default confidence of 0.95. " +
                                "Here the distinct count is the number of different values received for " +
                                "someAttribute. The answers are 95% guaranteed to have a +-1% error."
                ), //TODO : distinctCount -> approximateCardinality, every output - done
                @Example(
                        syntax = "define stream InputStream (some_attribute string);\n" +
                                "from InputStream#approximate:distinctCountEver(some_attribute, 0.05)\n" +
                                "select distinctCountEver, distinctCountEverLowerBound, distinctCountEverUpperBound\n" +
                                "insert into OutputStream;\n",
                        description = "Distinct count of events in a stream based on someAttribute is " +
                                "calculated for a relative error of 0.05 and a default confidence of 0.95. " +
                                "Here the distinct count is the number of different values received for " +
                                "someAttribute. The answers are 95% guaranteed to have a +-5% error."
                ),
                @Example(
                        syntax = "define stream InputStream (someAttribute double);\n" +
                                "from InputStream#approximate:distinctCountEver(someAttribute, 0.05, 0.65)\n" +
                                "select distinctCountEver, distinctCountEverLowerBound, distinctCountEverUpperBound\n" +
                                "insert into OutputStream;\n",
                        description = "distinctCount of events in a stream based on someAttribute is " +
                                "calculated for a relative error of 0.05 and a confidence of 0.65 ." +
                                "Here the distinct count is the number of different values received for " +
                                "someAttribute. The answers are 65% guaranteed to have a +-5% error."

                        //TODO : remove example - done
                )
        }
)
public class DistinctCountEverExtension extends StreamProcessor {
    private static final Logger logger = Logger.getLogger(DistinctCountEverExtension.class.getName());
    private HyperLogLog<Object> hyperLogLog;

    private ExpressionExecutor valueExecutor;

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
                    attributeExpressionExecutors.length + " attributes are found inside the distinctCountEver function");
        }

        //expressionExecutors[1] --> relativeError
        if (attributeExpressionExecutors.length > 1) {

            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("The 2nd parameter inside distinctCountEver function " +
                        "- 'relative.error' has to be a constant but found " +
                        this.attributeExpressionExecutors[1].getClass().getCanonicalName()); //TODO : 2nd param 'relative.error' - done
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.DOUBLE) {
                relativeError = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new SiddhiAppCreationException("The 2nd parameter inside distinctCountEver function - " +
                        "'relative.error' should be of type Double but found " +
                        attributeExpressionExecutors[1].getReturnType());
            }

            if ((relativeError <= 0) || (relativeError >= 1)) {
                throw new SiddhiAppCreationException("The 2nd parameter inside distinctCountEver function" +
                        " - 'relative.error' must be in the range of (0, 1) but found " + relativeError); //TODO : print passed value - done
            }
        }

        //expressionExecutors[2] --> confidence
        if (attributeExpressionExecutors.length > 2) {
            if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppCreationException("The 2nd parameter inside distinctCountEver function - " +
                        "'confidence' has to be a constant but found " +
                        this.attributeExpressionExecutors[2].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.DOUBLE) {
                confidence = (Double) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new SiddhiAppCreationException("The 2nd parameter inside distinctCountEver function - " +
                        "'confidence' should be of type Double but found " +
                        attributeExpressionExecutors[2].getReturnType());
            }

            if (Math.abs(confidence - 0.65) > 0.0000001 && Math.abs(confidence - 0.95) > 0.0000001
                    && Math.abs(confidence - 0.99) > 0.0000001) {
                throw new SiddhiAppCreationException("he 2nd parameter inside distinctCountEver function - " +
                        "'confidence' must be a value from 0.65, 0.95 and 0.99 but found " + confidence);
            }
        }

        valueExecutor = attributeExpressionExecutors[0];
        hyperLogLog = new HyperLogLog<>(relativeError, confidence, false);

        List<Attribute> attributeList = new ArrayList<>(3);
        attributeList.add(new Attribute("distinctCountEver", Attribute.Type.LONG)); //TODO : change names - done
        attributeList.add(new Attribute("distinctCountEverLowerBound", Attribute.Type.LONG));
        attributeList.add(new Attribute("distinctCountEverUpperBound", Attribute.Type.LONG));
        return attributeList;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                Object newData = valueExecutor.execute(streamEvent); //TODO : assign attributeExpressionExecutors[0] to a var - done
                if (newData == null) {
                    streamEventChunk.remove();
                } else {
                    if (streamEvent.getType().equals(StreamEvent.Type.CURRENT)) {
                        hyperLogLog.addItem(newData);
                    } else if (streamEvent.getType().equals(StreamEvent.Type.EXPIRED)) {
                        hyperLogLog.removeItem(newData);
                    } else if (streamEvent.getType().equals(StreamEvent.Type.RESET)) {
                        hyperLogLog.clear();
                    }

//                  outputData = {distinctCount, lower bound, upper bound}
                    Object[] outputData = {hyperLogLog.getCardinality(), hyperLogLog.getConfidenceInterval()[0],
                            hyperLogLog.getConfidenceInterval()[1]};

                    //TODO : remove debugs and logs
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
