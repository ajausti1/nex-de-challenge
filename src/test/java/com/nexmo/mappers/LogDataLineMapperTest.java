package com.nexmo.mappers;

import com.nexmo.entities.LogData;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.validator.ValidationException;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Andrew Austin on 9/22/17.
 */
public class LogDataLineMapperTest {

    LogDataLineMapper logDataLineMapper = new LogDataLineMapper();

    @Test
    public void thatMapperWorks() {
        Map<String, String> extractedMap = new HashMap<>();
        LogData logData = new LogData();

        String accountId = "20fe9c40a0d2b1eb070723e6fa169d9c";
        String gatewayId = "a78dec0c5254e20a2dae44c76bd0ec18";
        String country = "JO";
        String status = "submitted";
        String messagePrice = "0.0624015";
        String transitCost = "0.02";
        String routeCost = "0.12";
        //        BigDecimal messagePrice = BigDecimal.valueOf(0.0624015);
        //        BigDecimal transitCost = BigDecimal.valueOf(0.02);
        //        BigDecimal routeCost = BigDecimal.valueOf(0.12);

        extractedMap.put("account-id", accountId);
        extractedMap.put("gateway-id", gatewayId);
        extractedMap.put("country", country);
        extractedMap.put("status", status);
        extractedMap.put("price.message-price", messagePrice);
        extractedMap.put("cost.transit-cost", transitCost);
        extractedMap.put("cost.route-cost", routeCost);

        logData = logDataLineMapper.mapLogDataValues(logData, extractedMap);

        Assert.assertEquals(accountId, logData.getAccountId());
        Assert.assertEquals(gatewayId, logData.getGatewayId());
        Assert.assertEquals(country, logData.getCountry());
        Assert.assertEquals(status, logData.getStatus());

        Assert.assertEquals(BigDecimal.valueOf(Double.valueOf(messagePrice)), logData.getPrice());
        Assert.assertEquals(BigDecimal.valueOf(Double.valueOf(transitCost)), logData.getTransitCost());
        Assert.assertEquals(BigDecimal.valueOf(Double.valueOf(routeCost)), logData.getRouteCost());

    }

    @Test
    public void thatMissingCostIsOK() {
        Map<String, String> extractedMap = new HashMap<>();
        LogData logData = new LogData();

        String accountId = "20fe9c40a0d2b1eb070723e6fa169d9c";
        String gatewayId = "a78dec0c5254e20a2dae44c76bd0ec18";
        String country = "JO";
        String status = "submitted";
        String messagePrice = "0.0624015";
        String transitCost = "0.02";
        String routeCost = "0.12";
        //        BigDecimal messagePrice = BigDecimal.valueOf(0.0624015);
        //        BigDecimal transitCost = BigDecimal.valueOf(0.02);
        //        BigDecimal routeCost = BigDecimal.valueOf(0.12);

        extractedMap.put("account-id", accountId);
        extractedMap.put("gateway-id", gatewayId);
        extractedMap.put("country", country);
        extractedMap.put("status", status);
        extractedMap.put("price.message-price", messagePrice);
        extractedMap.put("cost.transit-cost", transitCost);

        logData = logDataLineMapper.mapLogDataValues(logData, extractedMap);

        Assert.assertEquals(accountId, logData.getAccountId());
        Assert.assertEquals(gatewayId, logData.getGatewayId());
        Assert.assertEquals(country, logData.getCountry());
        Assert.assertEquals(status, logData.getStatus());

        Assert.assertEquals(BigDecimal.valueOf(Double.valueOf(messagePrice)), logData.getPrice());
        Assert.assertEquals(BigDecimal.valueOf(Double.valueOf(transitCost)), logData.getTransitCost());
    }

    @Test(expected = ParseException.class)
    public void thatNullCostThrows() {
        Map<String, String> extractedMap = new HashMap<>();
        LogData logData = new LogData();

        String accountId = "20fe9c40a0d2b1eb070723e6fa169d9c";
        String gatewayId = "a78dec0c5254e20a2dae44c76bd0ec18";
        String country = "JO";
        String status = "submitted";
        String messagePrice = "0.0624015";
        String transitCost = "0.02";
        String routeCost = "0.12";
        //        BigDecimal messagePrice = BigDecimal.valueOf(0.0624015);
        //        BigDecimal transitCost = BigDecimal.valueOf(0.02);
        //        BigDecimal routeCost = BigDecimal.valueOf(0.12);

        extractedMap.put("account-id", accountId);
        extractedMap.put("gateway-id", gatewayId);
        extractedMap.put("country", country);
        extractedMap.put("status", status);
        extractedMap.put("price.message-price", messagePrice);
        extractedMap.put("cost.transit-cost", null);

        logData = logDataLineMapper.mapLogDataValues(logData, extractedMap);
    }

    @Test(expected = ParseException.class)
    public void thatBadCostThrows() {
        Map<String, String> extractedMap = new HashMap<>();
        LogData logData = new LogData();

        String accountId = "20fe9c40a0d2b1eb070723e6fa169d9c";
        String gatewayId = "a78dec0c5254e20a2dae44c76bd0ec18";
        String country = "JO";
        String status = "submitted";
        String messagePrice = "0.0624015";
        String transitCost = "0.02";
        String routeCost = "0.12";
        //        BigDecimal messagePrice = BigDecimal.valueOf(0.0624015);
        //        BigDecimal transitCost = BigDecimal.valueOf(0.02);
        //        BigDecimal routeCost = BigDecimal.valueOf(0.12);

        extractedMap.put("account-id", accountId);
        extractedMap.put("gateway-id", gatewayId);
        extractedMap.put("country", country);
        extractedMap.put("status", status);
        extractedMap.put("price.message-price", messagePrice);
        extractedMap.put("cost.transit-cost", "");

        logData = logDataLineMapper.mapLogDataValues(logData, extractedMap);
    }

    LogDataExtractorUtil logDataExtractorUtil = new LogDataExtractorUtil();
    ZonedDateTime parsedDateTime = ZonedDateTime.parse("08/04/2017 13:47:23 (023)", LogDataExtractorUtil.FORMATTER);
    Timestamp dateTime = Timestamp.from(parsedDateTime.toInstant());

    @Test
    public void thatMessageIdValidatorWorks() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateMessageId(logDataValidatorTestObj);
    }

    @Test(expected = ValidationException.class)
    public void thatMessageIdValidatorThrowsOnEmpty() {
        LogData logDataValidatorTestObj = new LogData("", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateMessageId(logDataValidatorTestObj);

    }

    @Test(expected = ValidationException.class)
    public void thatMessageIdValidatorThrowsOnNull() {
        LogData logDataValidatorTestObj = new LogData(null, dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateMessageId(logDataValidatorTestObj);

    }

    @Test(expected = ValidationException.class)
    public void thatMessageIdValidatorThrowsOnBadVal() {
        LogData logDataValidatorTestObj = new LogData("something bad", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateMessageId(logDataValidatorTestObj);
    }

    @Test
    public void thatAccountIdValidatorWorks() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateAccountId(logDataValidatorTestObj);
    }

    @Test(expected = ValidationException.class)
    public void thatAccountIdValidatorThrowsOnEmpty() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateAccountId(logDataValidatorTestObj);

    }

    @Test(expected = ValidationException.class)
    public void thatAccountIdValidatorThrowsOnNull() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, null, "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateAccountId(logDataValidatorTestObj);

    }

    @Test(expected = ValidationException.class)
    public void thatAccountIdValidatorThrowsOnBadVal() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "something bad", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateAccountId(logDataValidatorTestObj);
    }

    @Test
    public void thatGatewayIdValidatorWorks() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateGatewayId(logDataValidatorTestObj);
    }

    @Test(expected = ValidationException.class)
    public void thatGatewayIdValidatorThrowsOnEmpty() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateGatewayId(logDataValidatorTestObj);

    }

    @Test(expected = ValidationException.class)
    public void thatGatewayIdValidatorThrowsOnNull() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", null, "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateGatewayId(logDataValidatorTestObj);

    }

    @Test(expected = ValidationException.class)
    public void thatGatewayIdValidatorThrowsOnBadVal() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "something bad", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateGatewayId(logDataValidatorTestObj);
    }


    @Test
    public void thatCountryValidatorWorks() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateCountry(logDataValidatorTestObj);
    }

    @Test(expected = ValidationException.class)
    public void thatCountryValidatorThrowsOnEmpty() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateCountry(logDataValidatorTestObj);

    }

    @Test(expected = ValidationException.class)
    public void thatCountryValidatorThrowsOnNull() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", null, "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateCountry(logDataValidatorTestObj);

    }

    @Test(expected = ValidationException.class)
    public void thatCountryValidatorThrowsOnBadVal() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "something bad", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateCountry(logDataValidatorTestObj);
    }


    @Test
    public void thatStatusValidatorWorks() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateStatus(logDataValidatorTestObj);
    }

    @Test(expected = ValidationException.class)
    public void thatStatusValidatorThrowsOnEmpty() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateStatus(logDataValidatorTestObj);

    }

    @Test(expected = ValidationException.class)
    public void thatStatusValidatorThrowsOnNull() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "", null, BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateStatus(logDataValidatorTestObj);
    }

    @Test
    public void thatCostValidatorWorks() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validateCost(logDataValidatorTestObj);
    }

    @Test(expected = ValidationException.class)
    public void thatCostValidatorThrowsOnNull() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), null);

        // No error is success
        logDataLineMapper.validateCost(logDataValidatorTestObj);
    }

    @Test
    public void thatPriceValidatorWorks() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "UK", "submitted", BigDecimal.valueOf(0.429877), BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validatePrice(logDataValidatorTestObj);
    }

    @Test(expected = ValidationException.class)
    public void thatPriceValidatorThrowsOnNull() {
        LogData logDataValidatorTestObj = new LogData("b1c76ea92a0ccb8f44c2230846a50fa4", dateTime, "20fe9c40a0d2b1eb070723e6fa169d9c", "a78dec0c5254e20a2dae44c76bd0ec18", "", "submitted", null, BigDecimal.valueOf(0.1), BigDecimal.valueOf(0.02));

        // No error is success
        logDataLineMapper.validatePrice(logDataValidatorTestObj);
    }
}
