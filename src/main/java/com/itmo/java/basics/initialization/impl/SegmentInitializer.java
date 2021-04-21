package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.RemoveDatabaseRecord;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;


public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path path = context.currentSegmentContext().getSegmentPath().
                resolve(context.currentSegmentContext().getSegmentName());

        try (DatabaseInputStream dbis = new DatabaseInputStream(new FileInputStream(String.valueOf(path)))) {
            var result = dbis.readDbUnit();

            while (result.isPresent()) {
                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(new String(result.get().getKey()),
                        new SegmentOffsetInfoImpl(context.currentSegmentContext().getCurrentSize()));
                context.currentSegmentContext().setCurrentSize(result.get().size());
                result = dbis.readDbUnit();
            }
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when trying to initialize segment %s",
                    context.currentSegmentContext().getSegmentName()), e);
        }

        Segment segment = SegmentImpl.initializeFromContext(context.currentSegmentContext());
        context.currentTableContext().getTableIndex().onIndexedEntityUpdated(
                context.currentSegmentContext().getSegmentName(), segment);
        context.currentTableContext().updateCurrentSegment(segment);
    }
}
