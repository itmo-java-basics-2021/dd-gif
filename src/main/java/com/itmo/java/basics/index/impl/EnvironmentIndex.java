package com.itmo.java.basics.index.impl;

import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.util.Map;

public class EnvironmentIndex extends MapBasedKvsIndex<String, Database>{
    public Map<String, Database> getIndex() {
        return index;
    }
}
