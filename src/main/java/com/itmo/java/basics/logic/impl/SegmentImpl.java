package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

public class SegmentImpl implements Segment {
    private final String segmentName;
    private final Path tableRootPath;
    private int size = 0;
    private boolean isReadOnly = false;
    private final SegmentIndex segmentIndex = new SegmentIndex();


    public SegmentImpl(String segmentName, Path tableRootPath) {
        this.segmentName = segmentName;
        this.tableRootPath = tableRootPath;
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        Segment segment = new SegmentImpl(segmentName, tableRootPath);

        try {
            Files.createFile(tableRootPath.resolve(Paths.get(segment.getName())));
        }
        catch (IOException e) {
            throw new DatabaseException(e);
        }

        return segment;
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (objectKey == null || objectValue == null || isReadOnly()) {
            return false;
        }
        SetDatabaseRecord stbr = new SetDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8), objectValue);

        return this.appendToFile(objectKey, stbr);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        if (objectKey == null || segmentIndex.searchForKey(objectKey).isEmpty()) {
            return Optional.empty();
        }

        try (DatabaseInputStream dbis = new DatabaseInputStream(new FileInputStream(String.valueOf(tableRootPath.resolve(Paths.get(segmentName)))))) {
            dbis.skip(Objects.requireNonNull(segmentIndex.searchForKey(objectKey).orElse(null)).getOffset());
            var result = dbis.readDbUnit();
            if (result.get().isValuePresented()) {
                return Optional.of(result.get().getValue());
            }
            else {
                return Optional.empty();
            }
        }

    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        if (objectKey == null || segmentIndex.searchForKey(objectKey).isEmpty()) {
            return false;
        }
        RemoveDatabaseRecord rdbr = new RemoveDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8));

        return this.appendToFile(objectKey, rdbr);
    }

    private boolean appendToFile(String objectKey, WritableDatabaseRecord databaseRecord) throws IOException{
        try (DatabaseOutputStream dbos = new DatabaseOutputStream(new FileOutputStream(String.valueOf(tableRootPath.resolve(Paths.get(segmentName))), true)))  {
            segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(size));
            size += dbos.write(databaseRecord);
        }

        if (size >= 100000) {
            isReadOnly = true;
        }
        return true;
    }
}
