package ru.glosav.glosavcluster.logger.kafka.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import ru.glosav.glosavcluster.kafka.InKeyDeserializer;
import ru.glosav.glosavcluster.logger.kafka.dto.LoggerConstraints;
import ru.glosav.glosavcluster.logger.kafka.listener.KafkaMessageListener;
import ru.glosav.glosavcluster.storm.KafkaTopics;
import ru.glosav.glosavcluster.utils.time.DateTimeUtils;
;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Конфигурация Kafka
 *
 * @author Grigory Panin
 */
@Configuration
@ConfigurationProperties(prefix = "logger")
public class KafkaConsumerConfig {

    private LoggerConstraints loggerConstraints = new LoggerConstraints();

    private KafkaSubconfig kafkaSubconfig;

    @Value("${kafka.topic.message.type}")
    private String type;

    @Value("${kafka.topic.message.client-id}")
    private String clientId;

    @Value("${kafka.topic.message.group-id}")
    private String groupId;

    @Value("${kafka.address}")
    private String bootstrapServers;

    @Value("${logger.start-date}")
    public void setStartDate(String date) {
        if (!StringUtils.isBlank(date)) {
            Long startDate = DateTimeUtils.fromUtcString(date).toEpochSecond(ZoneOffset.UTC) * 1000;
            loggerConstraints.setStartDate(startDate);
        }
    }

    @Value("${logger.end-date}")
    public void setEndDate(String date) {
        if (!StringUtils.isBlank(date)) {
            Long endDate = DateTimeUtils.fromUtcString(date).toEpochSecond(ZoneOffset.UTC) * 1000;
            loggerConstraints.setEndDate(endDate);
        }
    }

    @Value("${logger.protobuf}")
    public void setWithProtobuf(boolean withProtobuf) {
        loggerConstraints.setProtobuf(withProtobuf);
    }

    @Value("${logger.stat-period}")
    public void setStatPeriod(Integer statPeriod) {
        loggerConstraints.setStatPeriod(statPeriod);
    }

    public void setOperators(List<Integer> operators) {
        if (operators != null && !operators.isEmpty()) {
            loggerConstraints.setOperators(operators);
        }
    }

    public void setDevops(List<Long> devops) {
        if (devops != null && !devops.isEmpty()) {
            loggerConstraints.setDevops(devops);
        }
    }

    @Bean
    public Map consumerConfigs() {
        Map props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, this.clientId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "10000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "10000");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, InKeyDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getKafkaSubconfig().getInValueDeserializerClass());
        return props;
    }

    @Bean
    public ConsumerFactory consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public KafkaListenerContainerFactory<?> kafkaListenerContainerFactory() {
        return getKafkaSubconfig().getKafkaListenerContainerFactory(consumerFactory());
    }

    @Bean
    public KafkaMessageListener getListener() {
        return getKafkaSubconfig().getListener(loggerConstraints);
    }

    private KafkaSubconfig getKafkaSubconfig() {
        if (kafkaSubconfig != null) {
            return kafkaSubconfig;
        } else if (KafkaTopics.TELEDATA.getTopic().equals(type)) {
            kafkaSubconfig = new TeledataSubconfig();
        } else if (KafkaTopics.SHORT_TELEDATA.getTopic().equals(type)) {
            kafkaSubconfig = new ShortTeledataSubconfig();
        } else if (KafkaTopics.EVENTDATA.getTopic().equals(type)) {
            kafkaSubconfig = new EventdataSubconfig();
        } else if ("sensor".equals(type)) {
            kafkaSubconfig = new SensordataSubconfig();
        } else {
            throw new IllegalStateException("Возможность чтения записей типа " + type + " не поддерживается");
        }

        return kafkaSubconfig;
    }
}