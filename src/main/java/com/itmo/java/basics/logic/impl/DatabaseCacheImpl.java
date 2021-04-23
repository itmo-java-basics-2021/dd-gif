package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.Iterator;
import java.util.LinkedHashMap;

public class DatabaseCacheImpl implements DatabaseCache {
    public static final int CACHE_SIZE = 5000;
    private final LinkedHashMap<String, byte[]> cache = new LinkedHashMap<>();

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
        if (cache.containsKey(key)) {
            this.delete(key);
        } else if (cache.size() == CACHE_SIZE) {
            Iterator<String> iterator = cache.keySet().iterator();
            iterator.next();
            iterator.remove();
        }

        cache.put(key, value);
    }

    @Override
    public void delete(String key) {
        cache.remove(key);
    }
}
