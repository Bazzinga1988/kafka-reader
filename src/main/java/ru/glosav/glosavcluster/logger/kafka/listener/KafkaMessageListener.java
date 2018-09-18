package ru.glosav.glosavcluster.logger.kafka.listener;

import com.google.protobuf.GeneratedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.glosav.glosavcluster.device.Unit;
import ru.glosav.glosavcluster.logger.kafka.dto.LoggerConstraints;
import ru.glosav.glosavcluster.utils.time.DateTimeUtils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Логирующий слушатель Kafka
 *
 * @author Grigory Panin
 */
public abstract class KafkaMessageListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageListener.class);

    /**
     * Ограничения логирования
     */
    private LoggerConstraints constraints;

    /**
     * Счетчик сообщений
     */
    private long counter;

    /**
     * Счетчики сообщений по операторам
     */
    private Map<Integer, Long> operatorCounters = new TreeMap<>();

    public KafkaMessageListener(LoggerConstraints constraints) {
        this.constraints = constraints;

        logger.info(
            "\n\n**************************************** Start logging *****************************************\n" +
            constraints.toString() +
            "\n\n************************************************************************************************");
    }

    protected abstract String getMessageName();

    protected final Logger getLogger() {
        return logger;
    }

    protected LoggerConstraints getConstraints() {
        return constraints;
    }

    protected long getCounter() {
        return counter;
    }

    protected final void log(Unit unit, long utc, GeneratedMessage proto) {
        String logUtc = DateTimeUtils.formatToISO8601(DateTimeUtils.getUTCByUnixTime(utc));
        String logString = logUtc + " [" + getMessageName() + "]: " + unit;
        getLogger().info(constraints.isProtobuf() ? logString + "\n" + proto : logString);

        if (constraints.getStatPeriod() != null) {
            ++counter;

            Long operatorCounter = operatorCounters.get(unit.getOperatorId());
            if (operatorCounter == null) {
                operatorCounter = 0L;
            }
            operatorCounters.put(unit.getOperatorId(), ++operatorCounter);

            if (counter % constraints.getStatPeriod() == 0) {
                logStatistics();
            }
        }
    }

    private void logStatistics() {
        List<String> operatorCounterStatistics = operatorCounters.keySet()
            .stream()
            .map(operatorId -> "сообщений оператора " + operatorId + ":\t\t\t" + operatorCounters.get(operatorId))
            .collect(Collectors.toList());

        String statistics =
            "\n\n----------------------------------------- Статистика -------------------------------------------\n" +
            "\n\tПрочитано сообщений:\t\t\t\t" + counter +
            "\n\tиз них:\n\t" +
            String.join("\n\t", operatorCounterStatistics) +
             getStatistics() +
             "\n\n------------------------------------------------------------------------------------------------\n";

        getLogger().info(statistics);
    }

    protected abstract String getStatistics();

    protected final boolean isSkip(Unit unit, long utc) {
        if (constraints.getDevops() != null) {
            if (!constraints.getDevops().contains(unit.getDevOp())) {
                return true;
            }
        } else if (constraints.getOperators() != null && !constraints.getOperators().contains(unit.getOperatorId())) {
            return true;
        }

        if (constraints.getStartDate() != null && constraints.getStartDate() > utc) {
            return true;
        }

        if (constraints.getEndDate() != null && constraints.getEndDate() < utc) {
            return true;
        }

        return false;
    }
}