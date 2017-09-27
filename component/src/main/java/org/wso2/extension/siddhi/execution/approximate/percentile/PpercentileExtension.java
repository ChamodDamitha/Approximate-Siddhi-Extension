package org.wso2.extension.siddhi.execution.approximate.percentile;


import org.apache.log4j.Logger;
//import org.wso2.extension.siddhi.execution.math.util.ValueParser;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * AttributeAggregator which implements the following function.
 * <code>percentile(value, p)</code>
 * Returns an estimate for the pth percentile of the stored values.
 * Accept Type(s): value: FLOAT,INT,LONG,DOUBLE / p: DOUBLE
 * Return Type: DOUBLE
 */
@Extension(
        name = "percentile",
        namespace = "approximate",
        description = "Returns the pth percentile value of the arg values.",
        parameters = {
                @Parameter(
                        name = "arg",
                        description = "The values of which the percentile should be found",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE}),
                @Parameter(
                        name = "p",
                        description = "Estimate of which percentile to be found (pth percentile) " +
                                "where p is any number greater than 0 or less than or equal to 100",
                        type = {DataType.DOUBLE}),
                @Parameter(
                        name = "err",
                        description = "Estimate of which percentile to be found (pth percentile) " +
                                "where p is any number greater than 0 or less than or equal to 100",
                        type = {DataType.DOUBLE})
        },
        returnAttributes = @ReturnAttribute(
                description = "Estimate of the 'p'th percentile value of the 'arg' values",
                type = {DataType.DOUBLE}),
        examples = @Example(
                description = "approximate:percentile(temperature, 0.97)",
                syntax = "define stream InValueStream (sensorId int, temperature double); \n" +
                        "from InValueStream \n" +
                        "select approximate:percentile(temperature, 0.97) as percentile \n" +
                        "insert into OutMediationStream;")
)
public class PpercentileExtension extends AttributeAggregator {

    private static final Logger logger = Logger.getLogger(PercentileExtension.class.getName());
    private double accuracy = 0.01;
    private double percentilePosition;
    PercentileCalculator percentileCalculator;
    ArrayList<Double> values = new ArrayList<>();

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ConfigReader configReader,
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

            if ((percentilePosition < 0) || (percentilePosition > 1)) {
                throw new SiddhiAppCreationException("percentile position must be in the range of (0, 1)" +
                        " but found " + percentilePosition);
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

        }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.DOUBLE;
    }

    @Override
    public Object processAdd(Object data) {
        // will not occur
        return new IllegalStateException("Percentile cannot process a single argument, but found " + data.toString());
    }

    @Override
    public Object processAdd(Object[] data) {
        double value;
        if (data[0].getClass().equals(Integer.class)) {
            value = ((Integer) data[0]) / 1.0;
        } else if (data[0].getClass().equals(Long.class)) {
            value = ((Long) data[0]) / 1.0;
        } else if (data[0].getClass().equals(Float.class)) {
            value = ((Float) data[0]) / 1.0;
        } else {
            value = (Double) data[0];
        }
        values.add(value);

        percentileCalculator = new PercentileApproximator();
        percentileCalculator.initialize(percentilePosition, accuracy);
        percentileCalculator.add(values);

        return percentileCalculator.getPercentile(percentilePosition);
    }

    @Override
    public Object processRemove(Object data) {
        // will not occur
        return new IllegalStateException("Percentile cannot process a single argument, but found " + data.toString());

    }

    @Override
    public Object processRemove(Object[] data) {
//        double value = valueParser.parseValue(data[0]);
//        sortedArrayListRemove(valuesList, value);
//        return getPercentileValue(valuesList, percentileValue);
        double value;
        if (data[0].getClass().equals(Integer.class)) {
            value = ((Integer) data[0]) / 1.0;
        } else if (data[0].getClass().equals(Long.class)) {
            value = ((Long) data[0]) / 1.0;
        } else if (data[0].getClass().equals(Float.class)) {
            value = ((Float) data[0]) / 1.0;
        } else {
            value = (Double) data[0];
        }
        values.remove(value);
        percentileCalculator = new PercentileApproximator();
        percentileCalculator.initialize(percentilePosition, accuracy);
        percentileCalculator.add(values);

        return percentileCalculator.getPercentile(percentilePosition);
    }

    @Override
    public Object reset() {
        return 0.0;
    }

    @Override
    public void start() {
        // Nothing to start
    }

    @Override
    public void stop() {
        // nothing to stop
    }

    @Override
    public Map<String, Object> currentState() {
        return Collections.singletonMap("percentileCalculator", percentileCalculator);
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        percentileCalculator = (PercentileCalculator) map.get(percentileCalculator);
    }


}
