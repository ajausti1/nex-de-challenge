package com.nexmo.mappers;

import com.nexmo.entities.LogData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests to confirm everything is wired correctly and errors are propagating correctly
 * <p/>
 * Created by Andrew Austin on 9/22/17.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@Import({LogDataLineMapper.class, LogDataExtractorUtil.class})
public class LogDataLineMapperIT {

    @Autowired
    LogDataLineMapper logDataLineMapper;

    @Test
    public void thatMapLineWorks() {
        String validLogLine = "b1c76ea92a0ccb8f44c2230846a50fa4 :: 08/04/2017 13:47:23 (023),account-id=20fe9c40a0d2b1eb070723e6fa169d9c,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=submitted,price={message-price=0.0624015},cost={transit-cost=0.02, route-cost=0.12}";
        LogData logData = logDataLineMapper.mapLine(validLogLine, 0);
    }

    @Test(expected = ParseException.class)
    public void thatMapLineThrowsParseException() {
        String validLogLine = "b1c76ea92a0ccb8f44c2230846a50fa4 :: 08/04G/2017 13:47:23 (023),account-id=20fe9c40a0d2b1eb070723e6fa169d9c,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=submitted,price={message-price=0.0624015},cost={transit-cost=0.02, route-cost=0.12}";
        LogData logData = logDataLineMapper.mapLine(validLogLine, 0);
    }

    @Test(expected = ValidationException.class)
    public void thatMapLineThrowsValidationException() {
        String validLogLine = "b1c76ea92a0ccb8f44c2230846a50fa4 :: 08/04/2017 13:47:23 (023),account-id=,gateway-id=a78dec0c5254e20a2dae44c76bd0ec18,country=JO,status=submitted,price={message-price=0.0624015},cost={transit-cost=0.02, route-cost=0.12}";
        LogData logData = logDataLineMapper.mapLine(validLogLine, 0);
    }
}
