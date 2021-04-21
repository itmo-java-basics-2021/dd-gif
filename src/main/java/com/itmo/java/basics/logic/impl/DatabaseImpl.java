package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.DatabaseIndex;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final String name;
    private final Path path;
    private Map<String, Table> databaseIndex = new HashMap<>();

    private DatabaseImpl(String name, Path path) {
        this.name = name;
        this.path = path;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        var db = new DatabaseImpl(dbName, databaseRoot.resolve(dbName));

        try {
            Files.createDirectory(databaseRoot.resolve(dbName));
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when trying to create database %s", dbName), e);
        }

        return db;
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        DatabaseImpl database = new DatabaseImpl(context.getDbName(), context.getDatabasePath());
        database.databaseIndex = context.getTables();
        return database;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (databaseIndex.get(tableName) != null) {
            throw new DatabaseException(String.format("The table %s already exists", tableName));
        }

        databaseIndex.put(tableName, TableImpl.create(tableName, path, new TableIndex()));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (databaseIndex.get(tableName) == null) {
            throw new DatabaseException(String.format("There is no table %s", tableName));
        }

        if (objectKey == null) {
            throw new DatabaseException("The key mustn't be null value");
        }

        databaseIndex.get(tableName).write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (databaseIndex.get(tableName) == null || objectKey == null) {
            return Optional.empty();
        } else {
            return databaseIndex.get(tableName).read(objectKey);
        }
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (databaseIndex.get(tableName) == null) {
            throw new DatabaseException(String.format("There is no table %s", tableName));
        }

        if (objectKey == null) {
            throw new DatabaseException("The key mustn't be null value");
        }

        databaseIndex.get(tableName).delete(objectKey);
    }
}
