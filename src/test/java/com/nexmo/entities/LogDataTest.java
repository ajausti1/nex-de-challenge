package com.nexmo.entities;


import com.nexmo.mappers.LogDataExtractorUtil;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.batch.item.validator.ValidationException;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZonedDateTime;

/**
 * Created by Andrew Austin on 9/22/17.
 */
public class LogDataTest {

    LogDataExtractorUtil logDataExtractorUtil = new LogDataExtractorUtil();
    ZonedDateTime parsedDateTime = ZonedDateTime.parse("08/04/2017 13:47:23 (023)", LogDataExtractorUtil.FORMATTER);
    Timestamp dateTime = Timestamp.from(parsedDateTime.toInstant());

    @Test
    public void thatCostCalculationWorks_complete() {
        LogData logData = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        Assert.assertEquals(BigDecimal.valueOf(0.12), logData.getCost());
    }

    @Test
    public void thatCostCalculationWorks_changingRouteCost() {
        LogData logData = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        Assert.assertEquals(BigDecimal.valueOf(0.12), logData.getCost());

        logData.setRouteCost(BigDecimal.valueOf(1.0));
        Assert.assertEquals(BigDecimal.valueOf(1.1), logData.getCost());
    }

    @Test
    public void thatCostCalculationWorks_changingTransitCost() {
        LogData logData = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        Assert.assertEquals(BigDecimal.valueOf(0.12), logData.getCost());

        logData.setTransitCost(BigDecimal.valueOf(2.0));
        Assert.assertEquals(BigDecimal.valueOf(2.02), logData.getCost());
    }

    @Test(expected = ValidationException.class)
    public void thatCostCalculationWorks_noTransaction() {
        LogData logData = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), null);

        Assert.assertEquals(BigDecimal.valueOf(0.1), logData.getCost());
    }

    @Test(expected = ValidationException.class)
    public void thatCostCalculationWorks_noRoute() {
        LogData logData = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), null, BigDecimal.valueOf(0.02));

        Assert.assertEquals(BigDecimal.valueOf(0.02), logData.getCost());
    }
}
