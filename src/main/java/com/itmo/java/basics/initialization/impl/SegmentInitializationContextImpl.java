package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String segmentName;
    private final Path segmentPath;
    private long currentSize;
    private final SegmentIndex index;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, int currentSize) {
        this.segmentName = segmentName;
        this.segmentPath = tablePath;
        this.currentSize = currentSize;
        index = new SegmentIndex();
    }

    @Override
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return index;
    }

    @Override
    public long getCurrentSize() {
        return currentSize;
    }

    @Override
    public void setCurrentSize(long size) {
        currentSize += size;
    }
}
