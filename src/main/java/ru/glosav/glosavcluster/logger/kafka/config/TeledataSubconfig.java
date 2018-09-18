package ru.glosav.glosavcluster.logger.kafka.config;

import com.google.protobuf.GeneratedMessage;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import ru.glosav.glosavcluster.kafka.InMessageDeserializer;
import ru.glosav.glosavcluster.logger.kafka.dto.LoggerConstraints;
import ru.glosav.glosavcluster.logger.kafka.listener.KafkaMessageListener;
import ru.glosav.glosavcluster.logger.kafka.listener.TeledataListener;
import ru.glosav.glosavcluster.protobuf.generated.v1.message.MessageProto;

/**
 * Конфигурация Kafka для Teledata
 *
 * @author Grigory Panin
 */
public class TeledataSubconfig extends KafkaSubconfig {

    @Override
    public Class<?> getInValueDeserializerClass() {
        return InMessageDeserializer.class;
    }

    @Override
    public KafkaMessageListener getListener(LoggerConstraints loggerConstraints) {
        return new TeledataListener(loggerConstraints);
    }

    @Override
    protected ConcurrentKafkaListenerContainerFactory<Long, ? extends GeneratedMessage> createKafkaListenerContainerFactory() {
        return new ConcurrentKafkaListenerContainerFactory<Long, MessageProto.MESSAGE>();
    }
}