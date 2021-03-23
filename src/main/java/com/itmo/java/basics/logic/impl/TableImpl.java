package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class TableImpl implements Table {
    private final String tableName;
    private final Path path;
    private final TableIndex tableIndex;
    private Segment currentSegment = null;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) {
        this.tableName = tableName;
        this.path = pathToDatabaseRoot;
        this.tableIndex = tableIndex;
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        Table table = new TableImpl(tableName, pathToDatabaseRoot.resolve(tableName), tableIndex);

        try {
            Files.createDirectory(pathToDatabaseRoot.resolve(tableName));
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
        return table;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (currentSegment == null || currentSegment.isReadOnly()) {
            currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), path);
        }

        try {
            currentSegment.write(objectKey, objectValue);
            tableIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (tableIndex.searchForKey(objectKey).isEmpty()) return Optional.empty();

        try {
            return tableIndex.searchForKey(objectKey).get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if (tableIndex.searchForKey(objectKey).isPresent()) {
            try {
                tableIndex.searchForKey(objectKey).get().delete(objectKey);
            } catch (IOException e) {
                throw new DatabaseException(e);
            }
        }
    }
}
