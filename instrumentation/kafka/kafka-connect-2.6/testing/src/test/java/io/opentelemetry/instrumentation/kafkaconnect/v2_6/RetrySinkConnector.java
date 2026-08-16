/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.kafkaconnect.v2_6;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public class RetrySinkConnector extends SinkConnector {

  static final String ATTEMPTED_PATH_CONFIG = "attempted.path";
  static final String COMPLETED_PATH_CONFIG = "completed.path";

  private Map<String, String> properties;

  @Override
  public void start(Map<String, String> properties) {
    this.properties = new HashMap<>(properties);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return RetrySinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>();
    configs.add(properties);
    return configs;
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return new ConfigDef()
        .define(ATTEMPTED_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "")
        .define(COMPLETED_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "");
  }

  @Override
  public String version() {
    return "test";
  }

  public static class RetrySinkTask extends SinkTask {

    private Path attemptedPath;
    private Path completedPath;

    @Override
    public String version() {
      return "test";
    }

    @Override
    public void start(Map<String, String> properties) {
      attemptedPath = Paths.get(properties.get(ATTEMPTED_PATH_CONFIG));
      completedPath = Paths.get(properties.get(COMPLETED_PATH_CONFIG));
    }

    @Override
    public void put(Collection<SinkRecord> records) {
      if (records.isEmpty()) {
        return;
      }

      try {
        Files.createFile(attemptedPath);
        throw new RetriableException("retry once");
      } catch (FileAlreadyExistsException ignored) {
        try {
          Files.write(completedPath, new byte[0]);
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      } catch (IOException e) {
        throw new ConnectException(e);
      }
    }

    @Override
    public void stop() {}
  }
}
