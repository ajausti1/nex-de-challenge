package com.nexmo.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;

@Component
public class FileVerificationSkipper implements SkipPolicy {

    private static int SKIP_COUNT = 10000;
    private static final Logger logger = LoggerFactory.getLogger(FileVerificationSkipper.class);
    public static int REJECTED_COUNT = 0;

    @Override
    public boolean shouldSkip(Throwable exception, int skipCount) throws SkipLimitExceededException {

        if (exception instanceof FileNotFoundException) {
            return false;
        } else if (exception instanceof FlatFileParseException && skipCount <= SKIP_COUNT) {
            REJECTED_COUNT++;

            FlatFileParseException ffpe = (FlatFileParseException) exception;
            StringBuilder errorMessage = new StringBuilder();

            errorMessage.append("[ " + ffpe.getLineNumber() + " ]\t");

            if (exception.getCause() != null) {
                errorMessage.append(ffpe.getCause().getMessage());
            }

            errorMessage.append("\t\t|| " + ffpe.getInput());

            logger.error("{}", errorMessage);

            return true;
        } else {
            logger.error("Exceeded skip count of {} - fatal failure!", SKIP_COUNT);
            return false;
        }
    }
}
