package ru.glosav.glosavcluster.logger.kafka.config;

import com.google.protobuf.GeneratedMessage;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import ru.glosav.glosavcluster.logger.kafka.dto.LoggerConstraints;
import ru.glosav.glosavcluster.logger.kafka.listener.KafkaMessageListener;

/**
 * Конфигурация Kafka для конкретного типа сообщения
 *
 * @author Grigory Panin
 */
public abstract class KafkaSubconfig {

    public abstract Class<?> getInValueDeserializerClass();

    public abstract KafkaMessageListener getListener(LoggerConstraints loggerConstraints);

    public final KafkaListenerContainerFactory<?> getKafkaListenerContainerFactory(ConsumerFactory consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Long, ? extends GeneratedMessage> factory =
            createKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        factory.getContainerProperties().setPollTimeout(30000);
        factory.setAutoStartup(true);
        return factory;
    }

    protected abstract ConcurrentKafkaListenerContainerFactory<Long, ? extends GeneratedMessage>
        createKafkaListenerContainerFactory();
}