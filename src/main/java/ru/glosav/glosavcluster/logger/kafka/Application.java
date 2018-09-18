package ru.glosav.glosavcluster.logger.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.glosav.glosavcluster.utils.time.DateTimeUtils;

import java.util.TimeZone;

/**
 * @author Grigory Panin
 */
@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone(DateTimeUtils.UTC));
        SpringApplication.run(Application.class, args);
    }
}