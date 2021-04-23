package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.EnvironmentIndex;
import com.itmo.java.basics.logic.Database;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private final DatabaseConfig config;
    private final EnvironmentIndex environmentIndex = new EnvironmentIndex();

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        return environmentIndex.searchForKey(name);
    }

    @Override
    public void addDatabase(Database db) {
        environmentIndex.onIndexedEntityUpdated(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return Path.of(config.getWorkingPath());
    }
}