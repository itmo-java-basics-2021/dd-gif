package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {
    public static final int CACHE_SIZE = 5000;
    private final LinkedHashMap<String, byte[]> cache = new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
            return size() > CACHE_SIZE;
        }
    };

    @Override
    public byte[] get(String key) {
        byte[] value = cache.get(key);

        if (value == null) {
            return null;
        } else {
            this.delete(key);
            this.set(key, value);
        }
        return cache.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        cache.put(key, value);
    }

    @Override
    public void delete(String key) {
        cache.remove(key);
    }
}
