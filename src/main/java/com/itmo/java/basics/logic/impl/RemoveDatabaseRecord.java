package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class RemoveDatabaseRecord implements WritableDatabaseRecord {
    private final byte[] key;
    private final byte[] value = null;

    public RemoveDatabaseRecord(byte[] key) {
        this.key = key;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return new byte[] {-1};
    }

    @Override
    public long size() {
        return 9 + key.length;
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return -1;
    }
}
