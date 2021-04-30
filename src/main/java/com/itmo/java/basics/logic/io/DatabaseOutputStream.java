package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Записывает данные в БД
 */
public class DatabaseOutputStream extends DataOutputStream {

    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    /**
     * Записывает в БД в следующем формате:
     * - Размер ключа в байтахб используя {@link WritableDatabaseRecord#getKeySize()}
     * - Ключ
     * - Размер записи в байтах {@link WritableDatabaseRecord#getValueSize()}
     * - Запись
     * Например при использовании UTF_8,
     * "key" : "value"
     * 3key5value
     * Метод вернет 10
     *
     * @param databaseRecord запись
     * @return размер записи
     * @throws IOException если запись не удалась
     */
    public long write(WritableDatabaseRecord databaseRecord) throws IOException {
        if (databaseRecord.getValue() == null) {
            writeInt(databaseRecord.getKeySize());
            out.write(databaseRecord.getKey());
            writeInt(databaseRecord.getValueSize());
        } else {
            writeInt(databaseRecord.getKeySize());
            out.write(databaseRecord.getKey());
            writeInt(databaseRecord.getValueSize());
            out.write(databaseRecord.getValue());
        }
        return databaseRecord.size();
    }
}