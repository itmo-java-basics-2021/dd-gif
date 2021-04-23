package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DatabaseServerInitializer implements Initializer {
    private final DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path path = context.executionEnvironment().getWorkingPath();
        File workingDirectory = new File(path.toString());

        if (!path.toFile().exists()) {
            try {
                Files.createDirectory(path);
            } catch (IOException e) {
                throw new DatabaseException(String.format("IO exception when trying to create working directory" +
                        " %s", path.toString()), e);
            }
        } else {
            File[] databases = workingDirectory.listFiles();
            InitializationContextImpl newContext;

            if (databases != null && databases.length > 0) {
                for (var database : databases) {
                    newContext = new InitializationContextImpl(context.executionEnvironment(),
                            new DatabaseInitializationContextImpl(database.getName(), path),
                            context.currentTableContext(), context.currentSegmentContext());
                    databaseInitializer.perform(newContext);
                }
            }
        }
    }
}