package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RespReader implements AutoCloseable {

    private final InputStream is;
    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {

        this.is = is;
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {

        return is.available() > 0;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {

        if (!hasArray()) {
            // TODO exception message
            throw new EOFException("eof");
        }

        switch (is.read()) {
            case '$':
                return readBulkString();
            case '-':
                return readError();
            case '*':
                return readArray();
            case '!':
                return readCommandId();
            default:
                return null;
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {

        if (!hasArray()) {
            // TODO exception message
            throw new EOFException("eof");
        }

        List<Byte> message = new ArrayList<>();

        byte someByte;
        while ((someByte = (byte) is.read()) != CR) {
            message.add(someByte);
        }

        is.read();

        byte[] msg = new byte[message.size()];
        for (int i = 0; i < message.size(); i++) {
            msg[i] = message.get(i);
        }

        return new RespError(msg);
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {

        if (!hasArray()) {
            // TODO exception message
            throw new EOFException("eof");
        }

        byte[] b = new byte[4];
        int tmp = 3;
        byte someByte;
        while ((someByte = (byte) is.read()) != CR) {
            b[tmp] = someByte;
            tmp--;
        }
        int count = ByteBuffer.wrap(b).getInt();

        is.read();

        byte[] data = new byte[count];
        if (count != 0) {
            for (int i = 0; i < count; i++) {
                data[i] = (byte) is.read();
            }

            is.read();
            is.read();
        }

        return new RespBulkString(data);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {

        if (!hasArray()) {
            // TODO exception message
            throw new EOFException("eof");
        }

        byte[] b = new byte[4];
        int tmp = 3;
        byte someByte;
        while ((someByte = (byte) is.read()) != CR) {
            b[tmp] = someByte;
            tmp--;
        }
        int count = ByteBuffer.wrap(b).getInt();

        is.read();

        RespObject[] objects = new RespObject[count];
        for (int i = 0; i < count; i++) {
            byte code = (byte) is.read();

            switch (code) {
                case '$':
                    objects[i] = readBulkString();
                    break;
                case '-':
                    objects[i] = readError();
                    break;
                case '!':
                    objects[i] = readCommandId();
                    break;
            }
        }

        return new RespArray(objects);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {

        if (!hasArray()) {
            // TODO exception message
            throw new EOFException("eof");
        }

        return new RespCommandId(ByteBuffer
                .wrap(
                new byte[] {
                        (byte) is.read(),
                        (byte) is.read(),
                        (byte) is.read(),
                        (byte) is.read()})
                .getInt()
        );
    }


    @Override
    public void close() throws IOException {
        //TODO implement
    }
}
