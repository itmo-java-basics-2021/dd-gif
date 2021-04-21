package com.itmo.java.basics.index.impl;

import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.impl.CachingTable;

import java.util.Map;

public class DatabaseIndex extends MapBasedKvsIndex<String, Table>{
    public Map<String, Table> getIndex() {
        return index;
    }
}
