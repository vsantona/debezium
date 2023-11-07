/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.pipeline.notification.AbstractNotificationsIT;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.util.Testing;

public class NotificationsIT extends AbstractNotificationsIT<OracleConnector> {

    private OracleConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "a");
        connection.execute("CREATE TABLE a (pk numeric(9,0) primary key, aa numeric(9,0))");
        TestHelper.streamTable(connection, "a");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() {
        stopConnector();
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
            return notifications.size() == 2;
        });

        assertThat(notifications).hasSize(2);
        SourceRecord sourceRecord = notifications.get(0);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("aggregate_type")).isEqualTo("Initial Snapshot");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("type")).isEqualTo("STARTED");
        Assertions.assertThat(((Struct) sourceRecord.value()).getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));

        assertTableNotificationsSentToTopic(notifications, "a");

        sourceRecord = notifications.get(notifications.size() - 1);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("aggregate_type")).isEqualTo("Initial Snapshot");
        Assertions.assertThat(((Struct) sourceRecord.value()).getString("type")).isEqualTo(snapshotStatusResult());
        Assertions.assertThat(((Struct) sourceRecord.value()).getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
    }

    @Test
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

        assertTableNotificationsSentToJmx(notifications, "a");

        assertThat(notifications.get(notifications.size() - 1))
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", snapshotStatusResult())
                .hasFieldOrProperty("timestamp");

        resetNotifications();

        notifications = readNotificationFromJmx();
        assertThat(notifications).hasSize(0);
    }

    @Test
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

        assertThat(jmxNotifications.get(jmxNotifications.size() - 1)).hasFieldOrPropertyWithValue("message", "Initial Snapshot generated a notification");
        notification = mapper.readValue(jmxNotifications.get(jmxNotifications.size() - 1).getUserData().toString(), Notification.class);
        assertThat(notification)
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", "COMPLETED")
                .hasFieldOrPropertyWithValue("additionalData", Map.of("connector_name", server()));
        assertThat(notification.getTimestamp()).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
    }

    @Override
    protected Class<OracleConnector> connectorClass() {
        return OracleConnector.class;
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.INITIAL);
    }

    @Override
    protected String connector() {
        return "oracle";
    }

    @Override
    protected String server() {
        return TestHelper.SERVER_NAME;
    }

    @Override
    protected String snapshotStatusResult() {
        return "COMPLETED";
    }
}
