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
    private final Map<String, Database> index = new HashMap<>();
    private final EnvironmentIndex environmentIndex = new EnvironmentIndex();

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public Optional<Database> getDatabase(String name) {
//        return Optional.of(index.get(name));
        return environmentIndex.searchForKey(name);
    }

    @Override
    public void addDatabase(Database db) {
//        index.put(db.getName(), db);
        environmentIndex.onIndexedEntityUpdated(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return Path.of(config.getWorkingPath());
    }
}