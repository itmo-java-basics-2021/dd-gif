package com.itmo.java.basics.index.impl;

import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.util.Map;

public class TableIndex extends MapBasedKvsIndex<String, Segment> {
    public Map<String, Segment> getIndex() {
        return index;
    }
}
