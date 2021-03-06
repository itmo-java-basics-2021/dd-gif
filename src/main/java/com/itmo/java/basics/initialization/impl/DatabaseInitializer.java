package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.file.Path;

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
     *                           или если возникла ошибка дочерних инициализаторов
     */

    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        File db = initialContext.currentDbContext().getDatabasePath().toFile();
        if (!db.exists() || !db.isDirectory() || !db.canRead()) {
            throw new DatabaseException(String.format("Something went wrong when trying to initialize db %s",
                    db.getName()));
        }

        Path path = initialContext.currentDbContext().getDatabasePath();
        File workingDirectory = new File(path.toString());
        File[] tables = workingDirectory.listFiles();

        for (var table : tables) {
            InitializationContextImpl newContext = new InitializationContextImpl(initialContext.executionEnvironment(),
                    initialContext.currentDbContext(),
                    new TableInitializationContextImpl(table.getName(),
                            initialContext.currentDbContext().getDatabasePath(), new TableIndex()),
                    initialContext.currentSegmentContext());
            tableInitializer.perform(newContext);
        }

        initialContext.executionEnvironment().addDatabase(
                DatabaseImpl.initializeFromContext(initialContext.currentDbContext()));
    }
}