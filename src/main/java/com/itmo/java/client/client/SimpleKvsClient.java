package com.itmo.java.client.client;


import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {

    private final String databaseName;
    private final Supplier<KvsConnection> connectionSupplier;

    /**
     * Констурктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания коннекшена к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {

        this.databaseName = databaseName;
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {

        try {
            KvsCommand command = new CreateDatabaseKvsCommand(databaseName);

            return sendCommandToServer(command).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {

        try {
            KvsCommand command = new CreateTableKvsCommand(databaseName, tableName);

            return sendCommandToServer(command).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {

        try {
            KvsCommand command = new GetKvsCommand(databaseName, tableName, key);

            return sendCommandToServer(command).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {

        try {
            KvsCommand command = new SetKvsCommand(databaseName, tableName, key, value);

            return sendCommandToServer(command).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {

        try {
            KvsCommand command = new DeleteKvsCommand(databaseName, tableName, key);

            return sendCommandToServer(command).asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(e.getMessage(), e);
        }
    }

    private RespObject sendCommandToServer(KvsCommand command) throws ConnectionException, DatabaseExecutionException {

        RespObject result = connectionSupplier.get().send(command.getCommandId(), command.serialize());

        if (result.isError()) {
            throw new DatabaseExecutionException(result.asString());
        }

        return result;
    }
}