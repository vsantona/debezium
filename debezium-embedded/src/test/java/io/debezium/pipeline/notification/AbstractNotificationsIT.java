/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.notification;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.JMX;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.pipeline.notification.channels.jmx.JmxNotificationChannelMXBean;

public abstract class AbstractNotificationsIT<T extends SourceConnector> extends AbstractConnectorTest {

    protected abstract Class<T> connectorClass();

    protected abstract Configuration.Builder config();

    protected abstract String connector();

    protected abstract String server();

    protected String task() {
        return null;
    }

    protected String database() {
        return null;
    }

    protected void startConnector(Function<Configuration.Builder, Configuration.Builder> custConfig) {

        final Configuration config = custConfig.apply(config()).build();

        start(connectorClass(), config);
    }

    protected abstract String snapshotStatusResult();

    @Test
    public void notificationNotSentIfNoChannelIsConfigured() {
        // Testing.Print.enable();

        startConnector(config -> config.with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification"));
        assertConnectorIsRunning();

        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();
    }

    @Test
    public void reportErrorWhenSinkChannelIsEnabledAndNoTopicConfigurationProvided() {
        // Testing.Print.enable();

        LogInterceptor logInterceptor = new LogInterceptor("io.debezium.connector");
        startConnector(config -> config
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink"));

        Assertions.assertThat(logInterceptor.containsErrorMessage(
                "Connector configuration is not valid. The 'notification.sink.topic.name' value is invalid: Notification topic name must be provided when kafka notification channel is enabled"))
                .isTrue();
    }

    protected void assertTableNotificationsSentToJmx(List<Notification> notifications, String tableName) {
        Optional<Notification> tableNotification;
        tableNotification = notifications.stream()
                .filter(v -> v.getType().equals("TABLE_SCAN_IN_PROGRESS") && v.getAdditionalData().containsValue(tableName))
                .findAny();

        assertThat(tableNotification.isPresent()).isTrue();
        assertThat(tableNotification.get().getAggregateType()).isEqualTo("Initial Snapshot");
        assertThat(tableNotification.get().getTimestamp()).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));

    }

    protected void assertTableNotificationsSentToTopic(List<SourceRecord> notifications, String tableName) {
        Optional<Struct> tableNotification;
        tableNotification = notifications.stream()
                .map(s -> ((Struct) s.value()))
                .filter(v -> v.getString("type").equals("TABLE_SCAN_IN_PROGRESS") && v.getMap("additional_data").containsValue(tableName))
                .findAny();
        assertThat(tableNotification.isPresent()).isTrue();
        assertThat(tableNotification.get().getString("aggregate_type")).isEqualTo("Initial Snapshot");
        assertThat(tableNotification.get().getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));

        tableNotification = notifications.stream()
                .map(s -> ((Struct) s.value()))
                .filter(v -> v.getString("type").equals("TABLE_SCAN_COMPLETED") && v.getMap("additional_data").containsValue(tableName))
                .findAny();
        assertThat(tableNotification.isPresent()).isTrue();
        assertThat(tableNotification.get().getString("aggregate_type")).isEqualTo("Initial Snapshot");
        assertThat(tableNotification.get().getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
    }

    protected List<Notification> readNotificationFromJmx()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException, MBeanException {

        ObjectName notificationBean = getObjectName();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        MBeanInfo mBeanInfo = server.getMBeanInfo(notificationBean);

        List<String> attributesNames = Arrays.stream(mBeanInfo.getAttributes()).map(MBeanAttributeInfo::getName).collect(Collectors.toList());
        assertThat(attributesNames).contains("Notifications");

        JmxNotificationChannelMXBean proxy = JMX.newMXBeanProxy(
                server,
                notificationBean,
                JmxNotificationChannelMXBean.class);

        return proxy.getNotifications();
    }

    protected MBeanNotificationInfo[] readJmxNotifications()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException, MBeanException {

        ObjectName notificationBean = getObjectName();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        MBeanInfo mBeanInfo = server.getMBeanInfo(notificationBean);

        return mBeanInfo.getNotifications();
    }

    private ObjectName getObjectName() throws MalformedObjectNameException {

        return new ObjectName(String.format("debezium.%s:type=management,context=notifications,server=%s", connector(), server()));
    }

    protected List<javax.management.Notification> registerJmxNotificationListener()
            throws MalformedObjectNameException, InstanceNotFoundException {

        ObjectName notificationBean = getObjectName();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        List<javax.management.Notification> receivedNotifications = new ArrayList<>();
        server.addNotificationListener(notificationBean, new ClientListener(), null, receivedNotifications);

        return receivedNotifications;
    }

    protected void resetNotifications()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, MBeanException {

        ObjectName notificationBean = getObjectName();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        server.invoke(notificationBean, "reset", new Object[]{}, new String[]{});

    }

    public static class ClientListener implements NotificationListener {

        @Override
        public void handleNotification(javax.management.Notification notification, Object handback) {

            ((List<javax.management.Notification>) handback).add(notification);
        }
    }
}
