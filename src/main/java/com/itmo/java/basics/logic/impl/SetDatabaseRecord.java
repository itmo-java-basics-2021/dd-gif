package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.util.Objects;

public class SetDatabaseRecord implements WritableDatabaseRecord {
    private final byte[] key;
    private final byte[] value;

    public SetDatabaseRecord(byte[] key, byte[] value) {
        this.key = key;
        this.value = Objects.requireNonNull(value, "The value is null");
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public long size() {
        return 8 + key.length  + value.length;
    }

    @Override
    public boolean isValuePresented() {
        return true;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return value.length;
    }
}
