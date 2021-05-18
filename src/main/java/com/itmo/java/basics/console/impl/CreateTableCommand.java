package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания базы таблицы
 */
public class CreateTableCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;
    private final static int REQUIRED_ARGUMENTS = 4;

    /**
     * Создает команду
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, имя таблицы
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateTableCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {

        if (commandArgs == null) {
            throw new IllegalArgumentException("Illegal Argument Exception: command arguments is null");
        }

        if (commandArgs.size() < REQUIRED_ARGUMENTS) {
            throw new IllegalArgumentException("Illegal Argument Exception: command arguments number is wrong");
        }

        for (RespObject commandArg : commandArgs) {
            if (commandArg == null) {
                throw new IllegalArgumentException("Illegal Argument Exception: one of command arguments is null");
            }
        }

        this.env = env;
        this.commandArgs = commandArgs;
    }

    /**
     * Создает таблицу в нужной бд
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная таблица была создана. Например, "Table table1 in database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {

        try {
            String dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tbName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();

            Optional<Database> db = env.getDatabase(dbName);
            if (db.isEmpty()) {
                return DatabaseCommandResult.error(
                        new DatabaseException("Database Exception: database " + dbName + " is not exist"));
            }

            db.get().createTableIfNotExists(tbName);

            return DatabaseCommandResult.success(
                    ("Table " + tbName + " was created successfully").getBytes(StandardCharsets.UTF_8));
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
    }
}
