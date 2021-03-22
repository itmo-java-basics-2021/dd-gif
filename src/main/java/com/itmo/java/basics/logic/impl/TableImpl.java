package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class TableImpl implements Table {
    private final String tableName;
    private final Path pathToDatabaseRoot;
    private final TableIndex tableIndex;
    private Segment currentSegment = null;

    public TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) {
        this.tableName = tableName;
        this.pathToDatabaseRoot = pathToDatabaseRoot;
        this.tableIndex = tableIndex;
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        Table table = new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
        return table;
    }

    @Override
    public String getName() {
        return tableName;
    }

    // TODO убрать (или поменять) IOException
    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException, IOException {
        // TODO разобраться че за pathToDatabaseRoot
        if (currentSegment == null || currentSegment.isReadOnly())
            currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToDatabaseRoot);
        currentSegment.write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {

        return Optional.empty();
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {

    }
}
