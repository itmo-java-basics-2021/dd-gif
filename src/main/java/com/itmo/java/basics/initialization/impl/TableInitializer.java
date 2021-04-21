package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.impl.CachingTable;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;

public class TableInitializer implements Initializer {
    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File table = context.currentTableContext().getTablePath().toFile();
        if (!table.exists() || !table.isDirectory() || !table.canRead()) {
            throw new DatabaseException(String.format("Something went wrong when trying to initialize table %s",
                    table.getName()));
        }

        Path path = context.currentTableContext().getTablePath();
//        File[] segments = workingDirectory.listFiles();

        var segments = new ArrayList<Path>();

        try {
            Files.walkFileTree(path, new HashSet<>(), 1, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    segments.add(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new DatabaseException("dsa");
        }

        segments.sort(Comparator.comparing(Path::getFileName));

        for (var segment : segments) {
            context = new InitializationContextImpl(context.executionEnvironment(),
                    context.currentDbContext(), context.currentTableContext(),
                    new SegmentInitializationContextImpl(segment.toFile().getName(),
                            context.currentTableContext().getTablePath(), 0));
            segmentInitializer.perform(context);
        }

        CachingTable initializedTable = TableImpl.initializeFromContext(context.currentTableContext());
        context.currentDbContext().addTable(initializedTable);
    }
}
