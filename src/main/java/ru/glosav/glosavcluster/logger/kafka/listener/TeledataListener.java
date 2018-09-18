package ru.glosav.glosavcluster.logger.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import ru.glosav.glosavcluster.device.Unit;
import ru.glosav.glosavcluster.logger.kafka.dto.LoggerConstraints;
import ru.glosav.glosavcluster.protobuf.generated.v1.message.MessageProto;
import ru.glosav.glosavcluster.protobuf.generated.v1.service.TSProto;
import ru.glosav.glosavcluster.utils.ProtobufUtils;

/**
 * Логирующий слушатель Kafka для Teledata
 *
 * @author Grigory Panin
 */
public class TeledataListener extends KafkaMessageListener {

    private long pointCounter;
    private long validPointCounter;
    private long digitalCounter;
    private long analogCounter;
    private long counterCounter;
    private long liquidCounter;
    private long canCounter;
    private long markerDataCounter;

    public TeledataListener(LoggerConstraints constraints) {
        super(constraints);
    }

    @Override
    protected String getMessageName() {
        return "Teledata";
    }

    @KafkaListener(topics = "${kafka.topic.message.name}")
    public void listen(ConsumerRecord<Long, MessageProto.MESSAGE> record) {
        try {
            Unit unit = new Unit(record.key());
            long utc = ProtobufUtils.getTimestampFromMessage(record.value());
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

    private void calcStatistics(MessageProto.MESSAGE message) {
        if (message.hasTs()) {
            TSProto.TELEMATICS_SERVICE ts = message.getTs();
            if (ts.hasPoint()) {
                ++pointCounter;
                if (ts.getPoint().getVld()) {
                    ++validPointCounter;
                }
            }
            if (!ts.getDigitalList().isEmpty()) {
                ++digitalCounter;
            }
            if (!ts.getAnalogList().isEmpty()) {
                ++analogCounter;
            }
            if (!ts.getCounterList().isEmpty()) {
                ++counterCounter;
            }
            if (ts.hasLiquidLevel()) {
                ++liquidCounter;
            }
            if (ts.hasCanData()) {
                ++canCounter;
            }
            if (ts.hasMarkerData()) {
                ++markerDataCounter;
            }
        }
    }

    @Override
    protected String getStatistics() {
        return "\n\tСообщений с координатами:\t\t\t" + pointCounter +
            "\n\tС валидными координатами:\t\t\t" + validPointCounter +
            "\n\tСообщений с digital sensor:\t\t\t" + digitalCounter +
            "\n\tСообщений с analog sensor:\t\t\t" + analogCounter +
            "\n\tСообщений с counter sensor:\t\t\t" + counterCounter +
            "\n\tСообщений с liquid sensor:\t\t\t" + liquidCounter +
            "\n\tСообщений с can sensor:   \t\t\t" + canCounter +
            "\n\tСообщений с marker_data:\t\t\t" + markerDataCounter;
    }
}