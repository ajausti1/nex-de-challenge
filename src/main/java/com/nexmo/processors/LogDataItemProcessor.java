package com.nexmo.processors;

import com.nexmo.entities.LogData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

/**
 * Triggered after line has been mapped to the LogData line and provides a hook for transformation and validation
 */
public class LogDataItemProcessor implements ItemProcessor<LogData, LogData> {

    private static final Logger log = LoggerFactory.getLogger(LogDataItemProcessor.class);

    public static int SUCCESS_COUNT = 0;

    @Override
    public LogData process(final LogData logData) throws Exception {

        if (++SUCCESS_COUNT % 25000 == 0) {
            log.info("Processed {} lines . . .", SUCCESS_COUNT);
        }

        return logData;
    }
}
