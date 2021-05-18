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
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;
    private static final int REQUIRED_ARGUMENTS = 6;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {

        if (commandArgs == null) {
            throw new IllegalArgumentException("Illegal Argument Exception: command arguments is null");
        }

        if (commandArgs.size() != REQUIRED_ARGUMENTS) {
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
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {

        try {
            String dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tbName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            String key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
            String value = commandArgs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString();

            Optional<Database> db = env.getDatabase(dbName);
            db.get().write(tbName, key, value.getBytes(StandardCharsets.UTF_8));

            return DatabaseCommandResult.success(
                    ("Pair key-value was added successfully").getBytes(StandardCharsets.UTF_8));
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
    }
}
