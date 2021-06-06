package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    private final RespObject[] objects;

    /**
     * Код объекта
     */
    public static final byte CODE = '*';

    public RespArray(RespObject... objects) {

        this.objects = objects;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {

        return Arrays.stream(objects)
                .map(RespObject::asString)
                .collect(Collectors.joining(" "));
    }

    @Override
    public void write(OutputStream os) throws IOException {

        os.write(CODE);
        os.write(String.valueOf(objects.length).getBytes(StandardCharsets.UTF_8));
        os.write(CRLF);

        for (RespObject object : objects) {
            object.write(os);
        }
    }

    public List<RespObject> getObjects() {

        return Arrays.asList(objects);
    }
}
