package com.github.mmolimar.kafka.connect.fs;

import com.github.mmolimar.kafka.connect.fs.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class FsSourceConnector extends SourceConnector {

    private static Logger log = LoggerFactory.getLogger(FsSourceConnector.class);

    private FsSourceConnectorConfig config;

    // datav fix
    public static final AtomicInteger taskCount = new AtomicInteger(1);

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info("{} Starting connector...", this);
        try {
            config = new FsSourceConnectorConfig(properties);
        } catch (ConfigException ce) {
            throw new ConnectException("Couldn't start FsSourceConnector due to configuration error.", ce);
        } catch (Exception ce) {
            throw new ConnectException("An error has occurred when starting FsSourceConnector." + ce);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (config == null) {
            throw new ConnectException("Connector config has not been initialized.");
        }
        final List<Map<String, String>> taskConfigs = new ArrayList<>();

        List<String> fsUris = config.getFsUris();
        int groups = Math.min(fsUris.size(), maxTasks);
        ConnectorUtils.groupPartitions(fsUris, groups)
                .forEach(dirs -> {
                    Map<String, String> taskProps = new HashMap<>(config.originalsStrings());
                    taskProps.put(FsSourceConnectorConfig.FS_URIS, String.join(",", dirs));
                    taskConfigs.add(taskProps);
                });

        log.debug("{} Partitions grouped as: {}", this, taskConfigs);

        taskCount.set(taskConfigs.size());
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("{} Stopping FsSourceConnector.", this);
        // Nothing to do
    }

    @Override
    public ConfigDef config() {
        return FsSourceConnectorConfig.conf();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
