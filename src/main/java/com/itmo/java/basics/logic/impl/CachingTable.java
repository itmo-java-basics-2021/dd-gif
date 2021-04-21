package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

public class CachingTable implements Table {
    private final DatabaseCacheImpl cache;
    private final TableImpl table;

    public CachingTable(TableImpl table) {
        this.table = table;
        cache = new DatabaseCacheImpl();
    }

    @Override
    public String getName() {
        return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        table.write(objectKey, objectValue);
        cache.set(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (cache.get(objectKey) == null) {
            return table.read(objectKey);
        } else {
            return Optional.of(cache.get(objectKey));
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        table.delete(objectKey);
        cache.delete(objectKey);
    }

    public TableImpl getTable() {
        return table;
    }
}
