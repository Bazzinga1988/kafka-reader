package ru.glosav.glosavcluster.logger.kafka.config;

import com.google.protobuf.GeneratedMessage;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import ru.glosav.glosavcluster.kafka.InSensorWrapperDeserializer;
import ru.glosav.glosavcluster.logger.kafka.dto.LoggerConstraints;
import ru.glosav.glosavcluster.logger.kafka.listener.KafkaMessageListener;
import ru.glosav.glosavcluster.logger.kafka.listener.SensordataListener;
import ru.glosav.glosavcluster.protobuf.generated.v1.message.SensorWrapperProto;

/**
 * Конфигурация Kafka для Sensordata
 *
 * @author Grigory Panin
 */
public class SensordataSubconfig extends KafkaSubconfig {

    @Override
    public Class<?> getInValueDeserializerClass() {
        return InSensorWrapperDeserializer.class;
    }

    @Override
    public KafkaMessageListener getListener(LoggerConstraints loggerConstraints) {
        return new SensordataListener(loggerConstraints);
    }

    @Override
    protected ConcurrentKafkaListenerContainerFactory<Long, ? extends GeneratedMessage> createKafkaListenerContainerFactory() {
        return new ConcurrentKafkaListenerContainerFactory<Long, SensorWrapperProto.SENSOR_WRAPPER>();
    }
}