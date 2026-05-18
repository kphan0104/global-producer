package com.global.producer.model;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.util.StringUtils;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class Schedule {

    private String cron;
    private String duration;
    private String timezone;

    public boolean isValid() {
        return hasCron() ^ hasDuration();
    }

    public boolean hasCron() {
        return StringUtils.hasText(cron);
    }

    public boolean hasDuration() {
        return StringUtils.hasText(duration);
    }

    public boolean hasTimezone() {
        return StringUtils.hasText(timezone);
    }

    public Duration resolveDuration() {
        return DurationStyle.detectAndParse(duration, ChronoUnit.MILLIS);
    }
}
