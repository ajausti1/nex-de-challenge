package com.nexmo.mappers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.file.mapping.JsonLineMapper;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;


/**
 * Provides parsing utility methods
 *
 * Created by Andrew Austin on 9/22/17.
 */
@Component
public class LogDataExtractorUtil {

    private static final Logger logger = LoggerFactory.getLogger(LogDataExtractorUtil.class);

    // Leverage Jackson JsonLineMapper to parse JSON sub components
    private JsonLineMapper jsonLineMapper;

    // TODO: Making an assumption that all messageIds have a fixed len / will not change to take advantage of
    // fast substring parsing - if this assumption is invalid we need to refactor to leverage another mechanism
    // of messageId len detection (e.g. regexp).  We use an example messageId to get the count.
    public static final int MESSAGE_ID_LEN = "9d311564aec493f23347e848439584e0".length();   // 32

    // TODO: Making an assumption that messageId comes first in all records across all files
    private static final int MESSAGE_ID_START_IDX = 0;
    private static final int MESSAGE_ID_STOP_IDX = MESSAGE_ID_START_IDX + MESSAGE_ID_LEN;

    private static final String MESSAGE_ID_DATE_SEP = " :: ";

    // The position where the dateString starts
    // TODO: We make the assumption that the date immediately follows the messageId + MESSAGE_ID_DATE_SEP
    private static final int DATE_STR_START_IDX = " :: ".length() + MESSAGE_ID_LEN;

    // TODO: Making an assumption that all dates have a fixed len / will not change - see MESSAGE_ID_LEN TDOO.
    private static final int DATE_STR_LEN = "08/04/2017 13:47:23 (149)".length();

    private static final String DATE_TIME_FORMATTER_PATTERN = "MM/dd/yyyy HH:mm:ss (SSS)";

    // DateTime zone value - this is the value for the location in which the logs were generated
    // TODO: We are assuming all logs are being generated in London at this point, we can abstract this out if this turns
    // out to be untrue.
    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(DATE_TIME_FORMATTER_PATTERN).withZone(ZoneId.of("Europe/London"));

    // Convenience calculation for date parsing
    private static final int DATE_STR_STOP_IDX = DATE_STR_LEN + DATE_STR_START_IDX;

    private static final String KV_SEP = "=";
    private static final String TOKEN_SEP = ",";

    /**
     * Returns the messageId from the line.  Expects the messageId to be the first variable and to be immediately followed
     * by the MESSAGE_ID_DATE_SEP.
     *
     * @param line       The full line to be parsed.
     * @param lineNumber The line number of the line to be parsed (used for logging).
     * @return The extracted messageId.
     * @throws ParseException Throws when messageId is not followed by the MESSAGE_ID_DATE_SEP value (which indicates a
     *                        malformed / improperly-lengthed messageId.
     */
    public String extractMessageId(String line, int lineNumber) throws ParseException {
        // Confirm the separator value is at the correct line position - see assumptions above
        if (line.substring(MESSAGE_ID_STOP_IDX, DATE_STR_START_IDX).equals(MESSAGE_ID_DATE_SEP)) {
            return line.substring(MESSAGE_ID_START_IDX, MESSAGE_ID_STOP_IDX);
        } else {
            // The substring doesn't match the expected separator - the messageId is malformed / not expected
            throw new ParseException("ParseException at line " + lineNumber + " messageId does not match expected length / is not first field in record");
        }
    }

    /**
     * Extracts the ZonedDateTime from the line.  Expects the date/time string to follow the messageId and
     * MESSAGE_ID_DATE_SEP
     *
     * @param line       The full line to be parsed.
     * @param lineNumber The line number of the line to be parsed (used for logging).
     * @return The parsed ZonedDateTime object.
     * @throws ParseException Thrown when the date/time value cannot be parsed to a ZonedDateTime object.
     */
    public ZonedDateTime extractDateTime(String line, int lineNumber) throws ParseException {
        // A missing timestamp value will cause us to throw a ParseException
        try {
            // TODO: We expect extractMessageId to be called first / to have the initial line format validated first
            String extractedTimestamp = line.substring(DATE_STR_START_IDX, DATE_STR_STOP_IDX);

            return ZonedDateTime.parse(extractedTimestamp, FORMATTER);
        } catch (Exception e) {
            throw new ParseException("ParseException at line " + lineNumber + " when attempting to extract timestamp");
        }
    }

    /**
     * Extracts the remaining (i.e. non-messageId non-date) fields from the log line.
     *
     * @param line       The full line to be parsed.
     * @param lineNumber The line number of the line to be parsed (used for logging).
     * @return A hashmap where the key is the CSV field key and the value is the CSV field value using '=' as the delim.
     * Nested-CSV values are decomposed to an exploded form (e.g. key={key_1=value} becomes key.key_1=value)
     * @throws ParseException Thrown when a parsing was not successful.
     */
    public Map<String, String> extractCSVDataFromLine(String line, int lineNumber) throws ParseException {
        // messageId and date are non-standard CSV; remove them, then parse the remaining CSV line

        HashMap<String, String> extractedMap = new HashMap<>();
        String csvStringComponent = line.substring(DATE_STR_STOP_IDX + 1);

        return parseCSVString(csvStringComponent, lineNumber, extractedMap, null);
    }

    /**
     * Parses the CSV component of our log message and parses the nested CSV objects.
     * <p/>
     * Currently only operates one nested CSV line deep and does not allow for the '{' character except for nested object
     * demaracation.
     *
     * @param line         The full line to be parsed.
     * @param lineNumber   The line number of the line to be parsed (used for logging).
     * @param extractedMap The pre-created extractedMap object that will be appended to
     * @param objectPrefix Optional; value to be prepended to the key to be added to the extractedMap (for nested CSV strings)
     * @return A hashmap where the key is the CSV field key and the value is the CSV field value using '=' as the delim.
     * Nested-CSV values are decomposed to an exploded form (e.g. key={key_1=value} becomes key.key_1=value)
     * @throws ParseException Thrown when a parsing was not successful.
     */
    public Map<String, String> parseCSVString(String line, int lineNumber, Map<String, String> extractedMap, String objectPrefix) throws ParseException {
        try {

            String key;
            String val;

            int tokenIdx = 0;
            int strEndIdx = line.length();
            int kvSepIdx;

            // Starting at IDX, find the first equals, save the value before the equal as 'key'
            while (tokenIdx < strEndIdx) {
                // Find the IDX of the equals
                kvSepIdx = line.indexOf(KV_SEP, tokenIdx);

                key = line.substring(tokenIdx, kvSepIdx);

                // If the char after the equals is '{', this is a nested object
                // TODO: We assume single-level nested CSV objects that don't contain the char '{' in their value; more work
                // TODO: would need to be done in order to accommodate multi-level nested CSV objects.
                if (line.charAt(kvSepIdx + 1) == '{') {
                    // Nested CSV; find the closure point
                    int nestedCSVClosureIdx = line.indexOf("}", kvSepIdx);

                    // Extract the subCSV string - everything from the '{' to the '}'
                    String subCSVObjectStr = line.substring(kvSepIdx + 2, nestedCSVClosureIdx);

                    // Extract the contents and have them added to our map
                    parseCSVString(subCSVObjectStr, lineNumber, extractedMap, key);

                    // Set the tokenIdx value to be 2 past the closure IDX as the pattern is '},'
                    tokenIdx = nestedCSVClosureIdx + 2;
                } else {

                    // If 'objectPrefix' if it exists, prefix the key to be used
                    if (objectPrefix != null) {
                        // NOTE: Main-line CSV strings don't have spaces between delimiters, but nested CSV strings do
                        // We trim the key only for in-line objects for performance
                        key = objectPrefix + "." + key.trim();
                    }

                    // The first char of the value is not '{', so this is a true value - extract everything until the next ','
                    int nextTokenSepIdx = line.indexOf(TOKEN_SEP, kvSepIdx);

                    // If nextTokenSepIdx is -1, this is the last value in the line
                    if (nextTokenSepIdx > -1) {
                        val = line.substring(kvSepIdx + 1, nextTokenSepIdx);

                        // Set the tokenIdx = nextTokenSepIdx + 1 so next loop starts at the first char of the next key
                        tokenIdx = nextTokenSepIdx + 1;
                    } else {
                        val = line.substring(kvSepIdx + 1);

                        // Set the tokenIdx = strEndIdx so we terminate on next loop
                        tokenIdx = strEndIdx;
                    }

                    extractedMap.put(key, val);
                }
            }

            return extractedMap;
        } catch (Exception e) {
            throw new ParseException("ParseException at line " + lineNumber + " when attempting to parse CSV", e);
        }
    }
}
