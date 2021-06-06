package com.itmo.java.basics;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.impl.DatabaseInitializationContextImpl;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class DatabaseServer {

    private final ExecutionEnvironment env;

    private DatabaseServer(ExecutionEnvironment env) {

        this.env = env;
    }

    /**
     * Constructor
     *
     * @param env         env для инициализации. Далее работа происходит с заполненым объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {

        initializer.perform(new InitializationContextImpl(env, null, null, null));

        return new DatabaseServer(env);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {

        var args = message.getObjects();
        int commandPosition = DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex();
        DatabaseCommand command = DatabaseCommands.valueOf(args.get(commandPosition).asString()).getCommand(env, args);

        return executeNextCommand(command);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {

        return CompletableFuture.completedFuture(command.execute());
    }

    public ExecutionEnvironment getEnv() {
        //TODO implement
        return null;
    }
}