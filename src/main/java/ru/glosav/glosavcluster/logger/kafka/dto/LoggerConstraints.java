package ru.glosav.glosavcluster.logger.kafka.dto;

import ru.glosav.glosavcluster.utils.time.DateTimeUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Ограничения логирования
 *
 * @author Grigory Panin
 */
public class LoggerConstraints {

    /**
     * Набор ID операторов
     */
    private List<Integer> operators;

    /**
     * Набор devops
     */
    private List<Long> devops;

    /**
     * Начальная дата
     */
    private Long startDate;

    /**
     * Конечная дата
     */
    private Long endDate;

    /**
     * Отображение protobuf
     */
    private boolean protobuf;

    /**
     * Период расчета статистики
     */
    private Integer statPeriod;

    public List<Integer> getOperators() {
        return operators;
    }

    public void setOperators(List<Integer> operators) {
        this.operators = operators;
    }

    public List<Long> getDevops() {
        return devops;
    }

    public void setDevops(List<Long> devops) {
        this.devops = devops;
    }

    public Long getStartDate() {
        return startDate;
    }

    public void setStartDate(Long startDate) {
        this.startDate = startDate;
    }

    public Long getEndDate() {
        return endDate;
    }

    public void setEndDate(Long endDate) {
        this.endDate = endDate;
    }

    public boolean isProtobuf() {
        return protobuf;
    }

    public void setProtobuf(boolean protobuf) {
        this.protobuf = protobuf;
    }

    public Integer getStatPeriod() {
        return statPeriod;
    }

    public void setStatPeriod(Integer statPeriod) {
        this.statPeriod = statPeriod;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        if (devops != null) {
            builder.append("\n\tdevops:\t\t\t" + Arrays.toString(operators.toArray()));
        } else {
            builder.append("\n\toperators:\t\t");
            if (operators != null) {
                builder.append(Arrays.toString(operators.toArray()));
            } else {
                builder.append("all");
            }
        }

        if (startDate != null) {
            builder.append("\n\tstartDate:\t\t" + DateTimeUtils.formatToISO8601(DateTimeUtils.getUTCByUnixTime(startDate)));
        }

        if (endDate != null) {
            builder.append("\n\tendDate:\t\t" + DateTimeUtils.formatToISO8601(DateTimeUtils.getUTCByUnixTime(endDate)));
        }

        builder.append("\n\tprotobuf:\t\t" + protobuf);

        if (statPeriod != null) {
            builder.append("\n\tstatPeriod:\t\t" + statPeriod);
        }

        return builder.toString();
    }
}