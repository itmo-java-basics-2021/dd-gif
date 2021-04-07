package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class TableImpl implements Table {
    private final String tableName;
    private final Path path;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        this.tableName = tableName;
        this.path = pathToDatabaseRoot;
        this.tableIndex = tableIndex;
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        TableImpl table = new TableImpl(tableName, pathToDatabaseRoot.resolve(tableName), tableIndex);

        try {
            Files.createDirectory(pathToDatabaseRoot.resolve(tableName));
            table.currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), table.path);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when trying to create table %s in path %s", tableName, pathToDatabaseRoot.toString()), e);
        }
        return table;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        try {
            if (!currentSegment.write(objectKey, objectValue)) {
                currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), path);
                currentSegment.write(objectKey, objectValue);
            }
            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when trying to write pair key-value %s-%s", objectKey, new String(objectValue)), e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (tableIndex.searchForKey(objectKey).isEmpty()) {
            return Optional.empty();
        }

        try {
            return tableIndex.searchForKey(objectKey).get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when trying to read a value with key %s", objectKey), e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        var tmp = tableIndex.searchForKey(objectKey);
        if (tmp.isPresent()) {
            try {
                if (tmp.get().isReadOnly()) {
                    if (currentSegment.isReadOnly()) {
                        tableIndex.onIndexedEntityUpdated(objectKey, SegmentImpl.create(SegmentImpl.createSegmentName(tableName), path));
                    } else {
                        tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
                    }
                }
                tmp.get().delete(objectKey);
            } catch (IOException e) {
                throw new DatabaseException(String.format("IO exception when trying to delete a value with key %s", objectKey), e);
            }
        }
    }
}
