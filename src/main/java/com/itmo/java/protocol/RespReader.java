package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RespReader implements AutoCloseable {

    private final PushbackInputStream is;
    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {

        this.is = new PushbackInputStream(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {

        byte b = (byte) is.read();
        is.unread(b);

        return b == '*';
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {

        if (is.available() > 0) {
            // TODO exception message
            throw new EOFException(String.valueOf(is.available()));
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
                // TODO exception message
                throw new IOException("unknown object");
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {

        int asd = is.available();
        if (asd == 0) {
            return new RespError(new byte[0]);
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

        int asd = is.available();
        if (asd == 0) {
            // TODO exception message
            throw new EOFException(String.valueOf(asd) + "vibrosbulkstring");
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

        int asd = is.available();
        if (asd == 0) {
            // TODO exception message
            throw new EOFException(String.valueOf(asd) + "vibrosarray");
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

        int asd = is.available();
        if (asd == 0) {
            // TODO exception message
            throw new EOFException(String.valueOf(asd) + "vibroscommandid");
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

        is.close();
    }
}
