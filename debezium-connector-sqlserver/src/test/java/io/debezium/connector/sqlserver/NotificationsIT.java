/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

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
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.pipeline.notification.AbstractNotificationsIT;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.util.Testing;

public class NotificationsIT extends AbstractNotificationsIT<SqlServerConnector> {

    @Before
    public void before() throws SQLException {

        TestHelper.createTestDatabase();
        SqlServerConnection sqlServerConnection = TestHelper.testConnection();
        sqlServerConnection.execute(
                "CREATE TABLE tablea (id int primary key, cola varchar(30))",
                "CREATE TABLE tableb (id int primary key, colb varchar(30))",
                "INSERT INTO tablea VALUES(1, 'a')");
        TestHelper.enableTableCdc(sqlServerConnection, "tablea");

        initializeConnectorTestFramework();

        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropTestDatabase();
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
        sourceRecord = notifications.get(1);
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
        assertThat(notifications.get(1))
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

        assertThat(jmxNotifications.get(1)).hasFieldOrPropertyWithValue("message", "Initial Snapshot generated a notification");
        notification = mapper.readValue(jmxNotifications.get(1).getUserData().toString(), Notification.class);
        assertThat(notification)
                .hasFieldOrPropertyWithValue("aggregateType", "Initial Snapshot")
                .hasFieldOrPropertyWithValue("type", "COMPLETED")
                .hasFieldOrPropertyWithValue("additionalData", Map.of("connector_name", server()));
        assertThat(notification.getTimestamp()).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
    }

    @Override
    protected Class<SqlServerConnector> connectorClass() {
        return SqlServerConnector.class;
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.SCHEMA_ONLY);
    }

    @Override
    protected String connector() {
        return "sql_server";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER_NAME;
    }

    @Override
    protected String task() {
        return "0";
    }

    @Override
    protected String database() {
        return TestHelper.TEST_DATABASE_1;
    }

    @Override
    protected String snapshotStatusResult() {
        return "COMPLETED";
    }

    @Test
    public void completeReadingFromACaptureInstanceNotificationEmitted() throws SQLException {
        startConnector(config -> config
                .with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification")
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink"));

        assertConnectorIsRunning();

        TestHelper.waitForStreamingStarted();

        SqlServerConnection connection = TestHelper.testConnection();
        connection.execute("INSERT INTO tablea VALUES(2, 'b')");
        connection.execute("ALTER TABLE tablea ADD colb int NULL");
        TestHelper.enableTableCdc(connection, "tablea", "tablea_c2");
        connection.execute("INSERT INTO tablea VALUES(3, 'c', 3)");
        connection.close();

        List<SourceRecord> notifications = new ArrayList<>();
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            consumeAvailableRecords(r -> {
                if (r.topic().equals("io.debezium.notification")) {
                    notifications.add(r);
                }
            });
            return notifications.size() == 3;
        });

        Assertions.assertThat(notifications).hasSize(3);
        SourceRecord sourceRecord = notifications.get(2);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        Struct value = (Struct) sourceRecord.value();
        Assertions.assertThat(value.getString("aggregate_type")).isEqualTo("Capture Instance");
        Assertions.assertThat(value.getString("type")).isEqualTo("COMPLETED");
        Assertions.assertThat(value.getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
        Map<String, String> additionalData = value.getMap("additional_data");
        Assertions.assertThat(additionalData.get("server")).isEqualTo("server1");
        Assertions.assertThat(additionalData.get("database")).isEqualTo(TestHelper.TEST_DATABASE_1);
        Assertions.assertThat(additionalData.get("capture_instance")).isEqualTo("dbo_tablea");
        Lsn startLsn = Lsn.valueOf(additionalData.get("start_lsn"));
        Lsn stopLsn = Lsn.valueOf(additionalData.get("stop_lsn"));
        Lsn commitLsn = Lsn.valueOf(additionalData.get("commit_lsn"));
        Assertions.assertThat(startLsn).isLessThan(stopLsn);
        Assertions.assertThat(stopLsn).isLessThan(commitLsn);

        connection = TestHelper.testConnection();
        connection.execute("EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'tablea', @capture_instance = 'dbo_tablea'");
        connection.execute("ALTER TABLE tablea ADD colc int NULL");
        TestHelper.enableTableCdc(connection, "tablea", "tablea_c3");
        connection.execute("INSERT INTO tablea VALUES(4, 'c', 4, 4)");
        connection.close();

        List<SourceRecord> notifications2 = new ArrayList<>();
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            consumeAvailableRecords(r -> {
                if (r.topic().equals("io.debezium.notification")) {
                    notifications2.add(r);
                }
            });
            return notifications2.size() == 1;
        });

        Assertions.assertThat(notifications2).hasSize(1);
        sourceRecord = notifications2.get(0);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        value = (Struct) sourceRecord.value();
        Assertions.assertThat(value.getString("aggregate_type")).isEqualTo("Capture Instance");
        Assertions.assertThat(value.getString("type")).isEqualTo("COMPLETED");
        Assertions.assertThat(value.getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
        additionalData = value.getMap("additional_data");
        Assertions.assertThat(additionalData.get("server")).isEqualTo("server1");
        Assertions.assertThat(additionalData.get("database")).isEqualTo(TestHelper.TEST_DATABASE_1);
        Assertions.assertThat(additionalData.get("capture_instance")).isEqualTo("tablea_c2");
        startLsn = Lsn.valueOf(additionalData.get("start_lsn"));
        stopLsn = Lsn.valueOf(additionalData.get("stop_lsn"));
        commitLsn = Lsn.valueOf(additionalData.get("commit_lsn"));
        Assertions.assertThat(startLsn).isLessThan(stopLsn);
        Assertions.assertThat(stopLsn).isLessThan(commitLsn);
    }
}
