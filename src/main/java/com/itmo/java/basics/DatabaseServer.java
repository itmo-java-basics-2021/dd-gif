package com.itmo.java.basics;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.impl.DatabaseInitializationContextImpl;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class DatabaseServer {

    private final ExecutionEnvironment env;
    private final DatabaseServerInitializer initializer;

    private DatabaseServer(ExecutionEnvironment env, DatabaseServerInitializer initializer) {

        this.env = env;
        this.initializer = initializer;
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

        return new DatabaseServer(env, initializer);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {

        var args = message.getObjects();
        DatabaseCommand command;

        switch (args.get(1).asString()) {
            case "CREATE_DATABASE":
                command = DatabaseCommands.CREATE_DATABASE.getCommand(env, args);
                break;
            case "CREATE_TABLE":
                command = DatabaseCommands.CREATE_TABLE.getCommand(env, args);
                break;
            case "SET_KEY":
                command = DatabaseCommands.SET_KEY.getCommand(env, args);
                break;
            case "GET_KEY":
                command = DatabaseCommands.GET_KEY.getCommand(env, args);
                break;
            case "DELETE_KEY":
                command = DatabaseCommands.DELETE_KEY.getCommand(env, args);
                break;
            default:
                // TODO ???
                throw new IllegalStateException("Unexpected value: " + args.get(1).asString());
        }

        return executeNextCommand(command);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {

        return CompletableFuture.completedFuture(command.execute());
    }
}