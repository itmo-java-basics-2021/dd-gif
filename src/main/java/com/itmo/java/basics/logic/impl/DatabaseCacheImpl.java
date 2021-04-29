package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {
    public static final int CACHE_SIZE = 5000;
    private final LinkedHashMap<String, byte[]> cache = new LinkedHashMap<String, byte[]>() {
        protected boolean removeEldestEntity(Map.Entry<String, byte[]> eldest) {
            return size() > CACHE_SIZE;
        }
    };

    @Override
    public byte[] get(String key) {
        if (cache.get(key) == null) {
            return null;
        } else {
            this.set(key, cache.get(key));
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
