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
 * performs HyperLogLog algorithm to get approximate distinctCount
 */
@Extension(
        name = "count",
        namespace = "exact",
        description = "Performs Count-min-sketch algorithm on a streaming data set based on a specific " +
                "relative error and cofidence. ",
        parameters = {
                @Parameter(
                        name = "value",
                        description = "The value used to find count",
                        type = {DataType.INT, DataType.DOUBLE, DataType.FLOAT, DataType.LONG, DataType.STRING,
                                DataType.BOOL, DataType.TIME, DataType.OBJECT}
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
                                "from InputStream#exact:count(some_attribute)\n" +
                                "select count\n" +
                                "insert into OutputStream;",
                        description = "count of events based on some_attribute is " +
                                "calculated exactly"
                ),
        }
)
public class ExactCountExtension extends StreamProcessor {
    private static final Logger logger = Logger.getLogger(ExactCountExtension.class.getName());
    private HashMap<String, Long> countMap;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
//       validate number of attributes
        if (attributeExpressionExecutors.length != 1) {
            throw new SiddhiAppCreationException("1 attribute is expected but " +
                    attributeExpressionExecutors.length + " attributes are found inside the count function");
        }

        countMap = new HashMap<String, Long>();

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
                Long count = countMap.get(newData.toString());
                if (streamEvent.getType().equals(StreamEvent.Type.CURRENT)) {
                    if (count == null) {
                        countMap.put(newData.toString(),1L);
                    } else {
                        countMap.put(newData.toString(), count + 1);
                    }
                } else if (streamEvent.getType().equals(StreamEvent.Type.EXPIRED)) {
                    if (count != null) {
                        countMap.put(newData.toString(), count - 1);
                    }
                }
                Object[] outputData = {countMap.get(newData.toString())};

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
            return map;
        }
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        synchronized (this) {
        }
    }
}
