package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.CachingTable;
import com.itmo.java.basics.logic.impl.DatabaseImpl;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class DatabaseInitializer implements Initializer {
    private final TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        Path path = initialContext.currentDbContext().getDatabasePath();
        File workingDirectory = new File(path.toString());
        File[] tables = workingDirectory.listFiles();

        if (tables != null && tables.length != 0) {
            InitializationContextImpl newContext;
            for (var table : tables) {
                newContext = new InitializationContextImpl(initialContext.executionEnvironment(),
                        initialContext.currentDbContext(),
                        new TableInitializationContextImpl(table.getName(),
                                initialContext.currentDbContext().getDatabasePath(), null),
                        null);
                tableInitializer.perform(newContext);
            }
        }
        initialContext.executionEnvironment().addDatabase(
                DatabaseImpl.initializeFromContext(initialContext.currentDbContext()));
    }
}
