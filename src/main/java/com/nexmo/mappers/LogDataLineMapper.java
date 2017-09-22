package com.nexmo.mappers;

import com.nexmo.entities.LogData;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Assumptions:
 * <p/>
 * We make the assumption that field order is fixed across all logs and make no attempt to dynamically identify field
 * positioning.  If this assumption is false, dynamic field identification will be required.
 */
@Component
public class LogDataLineMapper implements LineMapper<LogData> {

    @Autowired
    LogDataExtractorUtil logDataExtractorUtil;

    public LogData mapLine(String line, int lineNumber) throws ParseException, ValidationException {

        LogData logData = new LogData();

        // Step 1 - Extract the messageID - This performs some validation that messageId len is correct and that
        // the expected messageId / Date delim is correct
        logData.setMessageId(logDataExtractorUtil.extractMessageId(line, lineNumber));

        // Step 2 - Extract the date object - This performs validation that the start of the line is correctly formatted
        logData.setTimestamp(Timestamp.from(logDataExtractorUtil.extractDateTime(line, lineNumber).toInstant()));

        // Step 3 - Decompose the rest of the line (CSV and sub-CSV objects) into a hashmap
        Map<String, String> logDataAsMap = logDataExtractorUtil.extractCSVDataFromLine(line, lineNumber);

        // Step 4 - Map values from the hashmap to the object
        logData = mapLogDataValues(logData, logDataAsMap);

        // Pass the object through our validators; any failure will cause this method to fail
        validateMessageId(logData);
        validateTimestamp(logData);
        validateAccountId(logData);
        validateGatewayId(logData);
        validateCountry(logData);
        validateStatus(logData);
        validatePrice(logData);
        validateCost(logData);

        return logData;
    }

    /**
     * Parses the logDataAsMap object and populates the (raw) logData object.  No final object validation is performed
     * here, only mappings.
     *
     * @param logData
     * @param logDataAsMap
     * @return
     */
    public LogData mapLogDataValues(LogData logData, Map<String, String> logDataAsMap) throws ParseException {

        logData.setAccountId(logDataAsMap.get("account-id"));
        logData.setGatewayId(logDataAsMap.get("gateway-id"));
        logData.setCountry(logDataAsMap.get("country"));
        logData.setStatus(logDataAsMap.get("status"));

        // We insert values as BigDecimals, so we have to do null-checks - if the value doesn't exist; don't complain,
        // just don't set it.  We do poor-mans checking in lieu of the more robust regexp here:
        // http://docs.oracle.com/javase/8/docs/api/java/lang/Double.html#valueOf-java.lang.String-
        String numericFieldName = "";
        try {
            if (logDataAsMap.containsKey("price.message-price")) {
                numericFieldName = "price.message-price";
                logData.setPrice(BigDecimal.valueOf(Double.valueOf(logDataAsMap.get("price.message-price"))));
            }

            if (logDataAsMap.containsKey("cost.transit-cost")) {
                numericFieldName = "cost.transit-cost";
                logData.setTransitCost(BigDecimal.valueOf(Double.valueOf(logDataAsMap.get("cost.transit-cost"))));
            }

            if (logDataAsMap.containsKey("cost.route-cost")) {
                numericFieldName = "cost.route-cost";
                logData.setRouteCost(BigDecimal.valueOf(Double.valueOf(logDataAsMap.get("cost.route-cost"))));
            }
        } catch (NumberFormatException | NullPointerException nfe) {
            // While non-existent cost / price values are thrown during the validation phase, an invalidly formatted
            // error is a parse exception, so we throw here
            throw new ParseException("Invalid format while parsing value for field " + numericFieldName, nfe);
        }

        return logData;
    }


    public void validateMessageId(LogData logData) throws ValidationException {
        if (StringUtils.isEmpty(logData.getMessageId())) {
            throw new ValidationException("messageId must be set");
        } else if (logDataExtractorUtil.MESSAGE_ID_LEN != logData.getMessageId().length()) {
            throw new ValidationException("messageId '" + logData.getMessageId() + "' has improper size; expected " + logDataExtractorUtil.MESSAGE_ID_LEN);
        }
    }

    public static int ACCOUNT_ID_LEN = "7fd1846ebb16c328008b702c77c46b1c".length();

    public void validateAccountId(LogData logData) throws ValidationException {
        if (StringUtils.isEmpty(logData.getAccountId())) {
            throw new ValidationException("accountId must be set");
        } else if (ACCOUNT_ID_LEN != logData.getAccountId().length()) {
            throw new ValidationException("accountId '" + logData.getAccountId() + "'  has improper size; expected " + ACCOUNT_ID_LEN);
        }
    }

    public void validateTimestamp(LogData logData) throws ValidationException {
        if (logData.getTimestamp() == null) {
            throw new ValidationException("timestamp must be set be null");
        }
    }

    public static int GATEWAY_ID_LEN = "f89ac9a1257a10942ee8a938432eaa6f".length();

    public void validateGatewayId(LogData logData) throws ValidationException {
        if (StringUtils.isEmpty(logData.getGatewayId())) {
            throw new ValidationException("gatewayId must be set");
        } else if (GATEWAY_ID_LEN != logData.getGatewayId().length()) {
            throw new ValidationException("GatewayId '" + logData.getGatewayId() + "' has improper size; expected " + GATEWAY_ID_LEN);
        }
    }

    public static int COUNTRY_ID_LEN = "UK".length();

    public void validateCountry(LogData logData) throws ValidationException {
        if (StringUtils.isEmpty(logData.getCountry())) {
            throw new ValidationException("country must be set");
        } else if (COUNTRY_ID_LEN != logData.getCountry().length()) {
            throw new ValidationException("Country '" + logData.getCountry() + "' has improper size; expected " + COUNTRY_ID_LEN);
        }
    }

    public void validateStatus(LogData logData) throws ValidationException {
        if (StringUtils.isEmpty(logData.getStatus())) {
            throw new ValidationException("status must be set");
        }
    }

    public void validatePrice(LogData logData) throws ValidationException {
        if (logData.getPrice() == null) {
            throw new ValidationException("price cannot be null");
        }
    }

    public void validateCost(LogData logData) throws ValidationException {
        if (logData.getCost() == null) {
            throw new ValidationException("cost cannot be null");
        }
    }
}