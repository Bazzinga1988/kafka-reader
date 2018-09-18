package ru.glosav.glosavcluster.logger.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import ru.glosav.glosavcluster.device.Unit;
import ru.glosav.glosavcluster.logger.kafka.dto.LoggerConstraints;
import ru.glosav.glosavcluster.protobuf.generated.v1.event.EventProto;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Логирующий слушатель Kafka для Eventdata
 *
 * @author Grigory Panin
 */
public class EventdataListener extends KafkaMessageListener {

    private long pointCounter;
    private long validPointCounter;
    private Map<Integer, Long> eventTypeCounters = new TreeMap<>();

    public EventdataListener(LoggerConstraints constraints) {
        super(constraints);
    }

    @Override
    protected String getMessageName() {
        return "Eventdata";
    }

    @KafkaListener(topics = "${kafka.topic.message.name}")
    public void listen(ConsumerRecord<Long, EventProto.EVENT> record) {
        try {
            Unit unit = new Unit(record.key());
            long utc = record.value().getHeader().getUtc();
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

    private void calcStatistics(EventProto.EVENT event) {
        if (event.getHeader().hasPos()) {
            ++pointCounter;
            if (event.getHeader().getPos().getValid()) {
                ++validPointCounter;
            }
        }

        Integer eventType = event.getHeader().getEventType();
        Long eventTypeCounter = eventTypeCounters.get(eventType);
        if (eventTypeCounter == null) {
            eventTypeCounter = 0L;
        }
        eventTypeCounters.put(eventType, ++eventTypeCounter);
    }

    @Override
    protected String getStatistics() {
        List<String> eventTypeStatistics = eventTypeCounters.keySet()
            .stream()
            .map(eventType -> "событий с типом" + String.format("% 5d", eventType) +
                ":\t\t\t\t" + eventTypeCounters.get(eventType))
            .collect(Collectors.toList());

        return "\n\tСообщений с координатами:\t\t\t" + pointCounter +
            "\n\tС валидными координатами:\t\t\t" + validPointCounter +
            "\n\t" + String.join("\n\t", eventTypeStatistics);
    }
}