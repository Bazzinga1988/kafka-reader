package ru.glosav.glosavcluster.logger.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import ru.glosav.glosavcluster.device.Unit;
import ru.glosav.glosavcluster.logger.kafka.dto.LoggerConstraints;
import ru.glosav.glosavcluster.protobuf.generated.v1.message.SensorWrapperProto;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Логирующий слушатель Kafka для Sensordata
 *
 * @author Grigory Panin
 */
public class SensordataListener  extends KafkaMessageListener {

    private Map<Integer, Long> sensorTypeCounters = new TreeMap<>();

    public SensordataListener(LoggerConstraints constraints) {
        super(constraints);
    }

    @Override
    protected String getMessageName() {
        return "Sensordata";
    }

    @KafkaListener(topics = "${kafka.topic.message.name}")
    public void listen(ConsumerRecord<Long, SensorWrapperProto.SENSOR_WRAPPER> record) {
        try {
            Unit unit = new Unit(record.key());
            long utc = record.value().getUtc();
            if (!isSkip(unit, utc)) {
                if (getConstraints().getStatPeriod() != null) {
                    calcStatistics(record.value());
                }
                log(unit, utc, record.value());
            }
        } catch (Exception exception) {
            getLogger().error(exception.getMessage(), exception);
        }
    }

    private void calcStatistics(SensorWrapperProto.SENSOR_WRAPPER wrapper) {
        Integer sensorType = wrapper.getSensorType();
        Long sensorTypeCounter = sensorTypeCounters.get(sensorType);
        if (sensorTypeCounter == null) {
            sensorTypeCounter = 0L;
        }
        sensorTypeCounters.put(sensorType, ++sensorTypeCounter);
    }

    @Override
    protected String getStatistics() {
        List<String> sensorTypeStatistics = sensorTypeCounters.keySet()
            .stream()
            .map(sensorType -> "сообщений с типом " + sensorType + ":\t\t\t\t" + sensorTypeCounters.get(sensorType))
            .collect(Collectors.toList());

        return "\n\t" + String.join("\n\t", sensorTypeStatistics);
    }
}