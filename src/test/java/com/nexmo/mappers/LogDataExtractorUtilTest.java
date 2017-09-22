package com.nexmo.mappers;


import org.junit.Assert;
import org.junit.Test;
import org.springframework.batch.item.ParseException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Andrew Austin on 9/22/17.
 */
public class LogDataExtractorUtilTest {

    LogDataExtractorUtil logDataExtractorUtil = new LogDataExtractorUtil();
    String validLogLine = "b1c76ea92a0ccb8f44c2230846a50fa4 :: 08/04/2017 13:47:23 (023),account-id=20fe9c40a0d2b1eb070723e6fa169d9c,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=submitted,price={message-price=0.0624015},cost={transit-cost=0.02, route-cost=0.12}";

    @Test
    public void thatExtractCSVDataFromLineWorks() {
        Map<String, String> extractedMap = logDataExtractorUtil.extractCSVDataFromLine(validLogLine, 0);

        Assert.assertEquals(extractedMap.get("account-id"), "20fe9c40a0d2b1eb070723e6fa169d9c");
        Assert.assertEquals(extractedMap.get("gateway-id"), "a78dec0c5254e20a2dae44c76bd0ec18");
        Assert.assertEquals(extractedMap.get("country"), "JO");
        Assert.assertEquals(extractedMap.get("status"), "submitted");
        Assert.assertEquals(extractedMap.get("price.message-price"), "0.0624015");
        Assert.assertEquals(extractedMap.get("cost.transit-cost"), "0.02");
        Assert.assertEquals(extractedMap.get("cost.route-cost"), "0.12");
    }

    @Test
    public void thatMessageIdParseWorks() {
        String expectedMessageId = "b1c76ea92a0ccb8f44c2230846a50fa4";
        String extractedMessageId = logDataExtractorUtil.extractMessageId(validLogLine, 0);

        Assert.assertEquals(expectedMessageId, extractedMessageId);
    }

    @Test(expected = ParseException.class)
    public void thatMalformedMessageIdParseThrows() {
        // messageId too short
        String badLogLine = "b1c76ea92a0ccb8f44c2230846a :: 08/04/2017 13:47:23 (023),account-id=20fe9c40a0d2b1eb070723e6fa169d9c,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=submitted,price={message-price=0.0624015},cost={transit-cost=0.02, route-cost=0.12}";

        logDataExtractorUtil.extractMessageId(badLogLine, 0);
    }

    @Test(expected = ParseException.class)
    public void thatMissingMessageIdParseThrows() {
        // No messageId
        String badLogLine = " :: 08/04/2017 13:47:23 (023),account-id=20fe9c40a0d2b1eb070723e6fa169d9c,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=submitted,price={message-price=0.0624015},cost={transit-cost=0.02, route-cost=0.12}";

        logDataExtractorUtil.extractMessageId(badLogLine, 0);
    }

    @Test(expected = ParseException.class)
    public void thatUnexpectedSeparatorThrows() {
        // No messageId
        String badLogLine = "b1c76ea92a0ccb8f44c2230846a50fa4 ;; 08/04/2017 13:47:23 (023),account-id=20fe9c40a0d2b1eb070723e6fa169d9c,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=submitted,price={message-price=0.0624015},cost={transit-cost=0.02, route-cost=0.12}";

        logDataExtractorUtil.extractMessageId(badLogLine, 0);
    }


    @Test
    public void confirmDateTimeFormatter() {
        String dateTimeString = "08/04/2017 13:47:23 (023)";
        DateTimeFormatter formatter = LogDataExtractorUtil.FORMATTER;
        ZonedDateTime parsedDate = ZonedDateTime.parse(dateTimeString, formatter);

        Assert.assertEquals(parsedDate.getYear(), 2017);
        Assert.assertEquals(parsedDate.getDayOfMonth(), 4);
        Assert.assertEquals(parsedDate.getHour(), 13);
        Assert.assertEquals(parsedDate.getSecond(), 23);
    }

    @Test
    public void thatDateParseWorks() {
        String dateTimeString = "08/04/2017 13:47:23 (023)";
        DateTimeFormatter formatter = LogDataExtractorUtil.FORMATTER;
        ZonedDateTime parsedDate = ZonedDateTime.parse(dateTimeString, formatter);

        ZonedDateTime extractedDateTime = logDataExtractorUtil.extractDateTime(validLogLine, 0);

        Assert.assertEquals(parsedDate, extractedDateTime);
    }

    @Test(expected = ParseException.class)
    public void thatMalformedDateParseThrows() {
        // messageId too short
        String badLogLine = "b1c76ea92a0ccb8f44c2230846a :: 08/04/217 13:47:23 (023),account-id=20fe9c40a0d2b1eb070723e6fa169d9c,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=submitted,price={message-price=0.0624015},cost={transit-cost=0.02, route-cost=0.12}";

        logDataExtractorUtil.extractMessageId(badLogLine, 0);
    }

    @Test(expected = ParseException.class)
    public void thatMissingDateParseThrows() {
        // No messageId
        String badLogLine = " :: ,account-id=20fe9c40a0d2b1eb070723e6fa169d9c,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=submitted,price={message-price=0.0624015},cost={transit-cost=0.02, route-cost=0.12}";

        logDataExtractorUtil.extractMessageId(badLogLine, 0);
    }

    @Test
    public void thatCSVIsParsedCorrectly() {

        Map<String, String> extractedMap = new HashMap<>();
        String csvString = "account-id=20fe9c40a0d2b1eb070723e6fa169d9c,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=submitted,price={message-price=0.0624015},cost={transit-cost=0.02, route-cost=0.12}";
        extractedMap = logDataExtractorUtil.parseCSVString(csvString, 0, extractedMap, null);

        Assert.assertEquals(extractedMap.get("account-id"), "20fe9c40a0d2b1eb070723e6fa169d9c");
        Assert.assertEquals(extractedMap.get("gateway-id"), "a78dec0c5254e20a2dae44c76bd0ec18");
        Assert.assertEquals(extractedMap.get("country"), "JO");
        Assert.assertEquals(extractedMap.get("status"), "submitted");
        Assert.assertEquals(extractedMap.get("price.message-price"), "0.0624015");
        Assert.assertEquals(extractedMap.get("cost.transit-cost"), "0.02");
        Assert.assertEquals(extractedMap.get("cost.route-cost"), "0.12");
    }

    @Test
    public void thatCSVMissingValueIsParsedCorrectly() {

        Map<String, String> extractedMap = new HashMap<>();
        String csvString = "account-id=,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=,price={message-price=0.0624015},cost={transit-cost=0.02, route-cost=0.12}";
        extractedMap = logDataExtractorUtil.parseCSVString(csvString, 0, extractedMap, null);

        Assert.assertEquals(extractedMap.get("account-id"), "");
        Assert.assertEquals(extractedMap.get("gateway-id"), "a78dec0c5254e20a2dae44c76bd0ec18");
        Assert.assertEquals(extractedMap.get("country"), "JO");
        Assert.assertEquals(extractedMap.get("status"), "");
        Assert.assertEquals(extractedMap.get("price.message-price"), "0.0624015");
        Assert.assertEquals(extractedMap.get("cost.transit-cost"), "0.02");
        Assert.assertEquals(extractedMap.get("cost.route-cost"), "0.12");
    }

    @Test
    public void thatCSVEmptySubstringIsParsedCorrectly() {

        Map<String, String> extractedMap = new HashMap<>();
        String csvString = "account-id=20fe9c40a0d2b1eb070723e6fa169d9c,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=submitted,price={message-price=0.0624015},cost={}";
        extractedMap = logDataExtractorUtil.parseCSVString(csvString, 0, extractedMap, null);

        Assert.assertEquals(extractedMap.get("account-id"), "20fe9c40a0d2b1eb070723e6fa169d9c");
        Assert.assertEquals(extractedMap.get("gateway-id"), "a78dec0c5254e20a2dae44c76bd0ec18");
        Assert.assertEquals(extractedMap.get("country"), "JO");
        Assert.assertEquals(extractedMap.get("status"), "submitted");
        Assert.assertEquals(extractedMap.get("price.message-price"), "0.0624015");

        // Confirm that the cost structure wasn't added to the map
        Assert.assertEquals(5, extractedMap.size());
    }
}
