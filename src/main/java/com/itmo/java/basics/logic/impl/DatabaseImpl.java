package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.DatabaseIndex;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final String name;
    private final Path path;
    private final DatabaseIndex databaseIndex = new DatabaseIndex();

    private DatabaseImpl(String name, Path path) {
        this.name = name;
        this.path = path;
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        var db = new DatabaseImpl(dbName, databaseRoot.resolve(dbName));

        try {
            Files.createDirectory(databaseRoot.resolve(dbName));
        } catch (IOException e) {
            throw new DatabaseException(e);
        }

        return db;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if(databaseIndex.searchForKey(tableName).isPresent()) {
            throw new DatabaseException("such table already exists");
        }
        databaseIndex.onIndexedEntityUpdated(tableName, TableImpl.create(tableName, path, new TableIndex()));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (databaseIndex.searchForKey(tableName).isEmpty()) {
            throw new DatabaseException("no such table");
        }
        databaseIndex.searchForKey(tableName).get().write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (objectKey == null || databaseIndex.searchForKey(tableName).isEmpty()) {
            return Optional.empty();
        }
        else {
            return databaseIndex.searchForKey(tableName).get().read(objectKey);
        }
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (databaseIndex.searchForKey(tableName).isEmpty()) {
            throw new DatabaseException("no such table");
        }
        else {
            databaseIndex.searchForKey(tableName).get().delete(objectKey);
        }
    }
}
