package ru.glosav.glosavcluster.logger.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import ru.glosav.glosavcluster.device.Unit;
import ru.glosav.glosavcluster.logger.kafka.dto.LoggerConstraints;
import ru.glosav.glosavcluster.protobuf.generated.v1.message.ShortMessageProto;

/**
 * Логирующий слушатель Kafka для ShortTeledata
 *
 * @author Grigory Panin
 */
public class ShortTeledataListener extends KafkaMessageListener {

    private long validPointCounter;

    public ShortTeledataListener(LoggerConstraints constraints) {
        super(constraints);
    }

    @Override
    protected String getMessageName() {
        return "ShortTeledata";
    }

    @KafkaListener(topics = "${kafka.topic.message.name}")
    public void listen(ConsumerRecord<Long, ShortMessageProto.SHORT_MESSAGE> record) {
        try {
            Unit unit = new Unit(record.key());
            long utc = record.value().getTm();
            if (!isSkip(unit, utc)) {
                ShortMessageProto.SHORT_MESSAGE message = record.value();
                if (getConstraints().getStatPeriod() != null && message.getValid()) {
                    ++validPointCounter;
                }
                log(unit, utc, message);
            }
        } catch (Exception exception) {
            getLogger().error(exception.getMessage(), exception);
        }
    }

    @Override
    protected String getStatistics() {
        return "\n\tС валидными координатами:\t\t\t" + validPointCounter + "\n";
    }
}