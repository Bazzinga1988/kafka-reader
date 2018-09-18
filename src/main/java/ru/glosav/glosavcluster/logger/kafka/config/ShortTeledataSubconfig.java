package ru.glosav.glosavcluster.logger.kafka.config;

import com.google.protobuf.GeneratedMessage;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import ru.glosav.glosavcluster.kafka.InShortMessageDeserializer;
import ru.glosav.glosavcluster.logger.kafka.dto.LoggerConstraints;
import ru.glosav.glosavcluster.logger.kafka.listener.KafkaMessageListener;
import ru.glosav.glosavcluster.logger.kafka.listener.ShortTeledataListener;
import ru.glosav.glosavcluster.protobuf.generated.v1.message.ShortMessageProto;

/**
 * Конфигурация Kafka для ShortTeledata
 *
 * @author Grigory Panin
 */
public class ShortTeledataSubconfig extends KafkaSubconfig {

    @Override
    public Class<?> getInValueDeserializerClass() {
        return InShortMessageDeserializer.class;
    }

    @Override
    public KafkaMessageListener getListener(LoggerConstraints loggerConstraints) {
        return new ShortTeledataListener(loggerConstraints);
    }

    @Override
    protected ConcurrentKafkaListenerContainerFactory<Long, ? extends GeneratedMessage> createKafkaListenerContainerFactory() {
        return new ConcurrentKafkaListenerContainerFactory<Long, ShortMessageProto.SHORT_MESSAGE>();
    }
}