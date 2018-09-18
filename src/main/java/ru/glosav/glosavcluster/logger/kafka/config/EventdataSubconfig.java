package ru.glosav.glosavcluster.logger.kafka.config;

import com.google.protobuf.GeneratedMessage;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import ru.glosav.glosavcluster.kafka.InEventDeserializer;
import ru.glosav.glosavcluster.logger.kafka.dto.LoggerConstraints;
import ru.glosav.glosavcluster.logger.kafka.listener.EventdataListener;
import ru.glosav.glosavcluster.logger.kafka.listener.KafkaMessageListener;
import ru.glosav.glosavcluster.protobuf.generated.v1.event.EventProto;

/**
 * Конфигурация Kafka для Eventdata
 *
 * @author Grigory Panin
 */
public class EventdataSubconfig extends KafkaSubconfig {

    @Override
    public Class<?> getInValueDeserializerClass() {
        return InEventDeserializer.class;
    }

    @Override
    public KafkaMessageListener getListener(LoggerConstraints loggerConstraints) {
        return new EventdataListener(loggerConstraints);
    }

    @Override
    protected ConcurrentKafkaListenerContainerFactory<Long, ? extends GeneratedMessage> createKafkaListenerContainerFactory() {
        return new ConcurrentKafkaListenerContainerFactory<Long, EventProto.EVENT>();
    }
}