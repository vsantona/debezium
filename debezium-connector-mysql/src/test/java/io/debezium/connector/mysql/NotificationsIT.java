/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanException;
import javax.management.MBeanNotificationInfo;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.notification.AbstractNotificationsIT;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.util.Testing;

public class NotificationsIT extends AbstractNotificationsIT<MySqlConnector> {

    protected static final String SERVER_NAME = "is_test";
    protected final UniqueDatabase DATABASE = new UniqueDatabase(SERVER_NAME, "incremental_snapshot-test").withDbHistoryPath(SCHEMA_HISTORY_PATH);
    protected static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-is.txt")
            .toAbsolutePath();

    @Before
    public void before() throws SQLException {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    public void notificationCorrectlySentOnItsTopic() throws InterruptedException {
        startConnector(config -> config
                .with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification")
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink"));

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        List<SourceRecord> notifications = new ArrayList<>();
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {

            consumeAvailableRecords(r -> {
                if (r.topic().equals("io.debezium.notification")) {
                    notifications.add(r);
                }
            });
            return notifications.size() == 18;
        });

        assertThat(notifications).hasSize(18);
        SourceRecord sourceRecord = notifications.get(0);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("aggregate_type")).isEqualTo("Initial Snapshot");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("type")).isEqualTo("STARTED");
        Assertions.assertThat(((Struct) sourceRecord.value()).getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));

        assertTableNotification(notifications, "a", sourceRecord, "TABLE_SCAN_IN_PROGRESS");
        assertTableNotification(notifications, "a", sourceRecord, "TABLE_SCAN_COMPLETED");
        assertTableNotification(notifications, "b", sourceRecord, "TABLE_SCAN_IN_PROGRESS");
        assertTableNotification(notifications, "b", sourceRecord, "TABLE_SCAN_COMPLETED");
        assertTableNotification(notifications, "c", sourceRecord, "TABLE_SCAN_IN_PROGRESS");
        assertTableNotification(notifications, "c", sourceRecord, "TABLE_SCAN_COMPLETED");
        assertTableNotification(notifications, "a4", sourceRecord, "TABLE_SCAN_IN_PROGRESS");
        assertTableNotification(notifications, "a4", sourceRecord, "TABLE_SCAN_COMPLETED");
        assertTableNotification(notifications, "a42", sourceRecord, "TABLE_SCAN_IN_PROGRESS");
        assertTableNotification(notifications, "a42", sourceRecord, "TABLE_SCAN_COMPLETED");
        assertTableNotification(notifications, "a_dt", sourceRecord, "TABLE_SCAN_IN_PROGRESS");
        assertTableNotification(notifications, "a_dt", sourceRecord, "TABLE_SCAN_COMPLETED");
        assertTableNotification(notifications, "a_date", sourceRecord, "TABLE_SCAN_IN_PROGRESS");
        assertTableNotification(notifications, "a_date", sourceRecord, "TABLE_SCAN_COMPLETED");
        assertTableNotification(notifications, "debezium_signal", sourceRecord, "TABLE_SCAN_IN_PROGRESS");
        assertTableNotification(notifications, "debezium_signal", sourceRecord, "TABLE_SCAN_COMPLETED");

        sourceRecord = notifications.get(notifications.size() - 1);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("aggregate_type")).isEqualTo("Initial Snapshot");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("type")).isEqualTo(snapshotStatusResult());
        Assertions.assertThat(((Struct) sourceRecord.value()).getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
    }

    private static void assertTableNotification(List<SourceRecord> notifications, String a, SourceRecord sourceRecord, String TABLE_SCAN_COMPLETED) {
        Optional<SourceRecord> any = notifications.stream()
                .filter(n -> (((Struct) n.value()).getMap("additional_data")).containsValue(a)).findAny();
        Assertions.assertThat(any.isPresent()).isTrue();
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("aggregate_type")).isEqualTo("Initial Snapshot");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("type")).isEqualTo(TABLE_SCAN_COMPLETED);
        Assertions.assertThat(((Struct) sourceRecord.value()).getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
    }

    @Test
    @Ignore
    public void notificationCorrectlySentOnJmx()
            throws ReflectionException, MalformedObjectNameException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException,
            MBeanException, InterruptedException {

        startConnector(config -> config
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "jmx"));

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .pollDelay(1, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> !readNotificationFromJmx().isEmpty());

        List<Notification> notifications = readNotificationFromJmx();

        assertThat(notifications).hasSize(2);
        assertThat(notifications.get(0))
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", "STARTED")
                .hasFieldOrProperty("timestamp");
        assertThat(notifications.get(1))
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", snapshotStatusResult())
                .hasFieldOrProperty("timestamp");

        resetNotifications();

        notifications = readNotificationFromJmx();
        assertThat(notifications).hasSize(0);
    }

    @Test
    @Ignore
    public void emittingDebeziumNotificationWillGenerateAJmxNotification()
            throws ReflectionException, MalformedObjectNameException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException,
            MBeanException, InterruptedException, JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        startConnector(config -> config
                .with(CommonConnectorConfig.SNAPSHOT_DELAY_MS, 2000)
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "jmx"));

        List<javax.management.Notification> jmxNotifications = registerJmxNotificationListener();

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(connector(), server(), task(), database());

        MBeanNotificationInfo[] notifications = readJmxNotifications();

        assertThat(notifications).allSatisfy(mBeanNotificationInfo -> assertThat(mBeanNotificationInfo.getName()).isEqualTo(Notification.class.getName()));

        assertThat(jmxNotifications).hasSize(2);
        assertThat(jmxNotifications.get(0)).hasFieldOrPropertyWithValue("message", "Initial Snapshot generated a notification");
        Notification notification = mapper.readValue(jmxNotifications.get(0).getUserData().toString(), Notification.class);
        assertThat(notification)
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", "STARTED")
                .hasFieldOrPropertyWithValue("additionalData", Map.of("connector_name", server()));
        assertThat(notification.getTimestamp()).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));

        assertThat(jmxNotifications.get(1)).hasFieldOrPropertyWithValue("message", "Initial Snapshot generated a notification");
        notification = mapper.readValue(jmxNotifications.get(1).getUserData().toString(), Notification.class);
        assertThat(notification)
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", "COMPLETED")
                .hasFieldOrPropertyWithValue("additionalData", Map.of("connector_name", server()));
        assertThat(notification.getTimestamp()).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
    }

    @Override
    protected Class<MySqlConnector> connectorClass() {
        return MySqlConnector.class;
    }

    @Override
    protected Configuration.Builder config() {
        return DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL.getValue());
    }

    @Override
    protected String connector() {
        return "mysql";
    }

    @Override
    protected String server() {
        return DATABASE.getServerName();
    }

    @Override
    protected String snapshotStatusResult() {
        return "COMPLETED";
    }
}
