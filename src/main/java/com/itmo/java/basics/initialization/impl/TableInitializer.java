package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.impl.CachingTable;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

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
        Path path = context.currentTableContext().getTablePath().resolve(context.currentTableContext().getTableName());
        File workingDirectory = new File(path.toString());
        File[] segments = workingDirectory.listFiles();

        if (segments != null && segments.length != 0) {
            for (var segment : segments) {
                var newContext = new InitializationContextImpl(context.executionEnvironment(),
                        context.currentDbContext(), context.currentTableContext(),
                        new SegmentInitializationContextImpl(segment.getName(),
                                context.currentTableContext().getTablePath(), 0));
                segmentInitializer.perform(newContext);
            }
        }

        CachingTable table = TableImpl.initializeFromContext(context.currentTableContext());
        context.currentDbContext().addTable(table);
    }
}
