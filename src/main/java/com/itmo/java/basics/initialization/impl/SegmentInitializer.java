package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
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
        File segment = context.currentSegmentContext().getSegmentPath().toFile();
        if (!segment.exists() || !segment.isFile() || !segment.canRead()) {
            throw new DatabaseException(String.format("Something went wrong when trying to initialize segment %s",
                    segment.getName()));
        }

        Path path = context.currentSegmentContext().getSegmentPath();

        try (DatabaseInputStream dbis = new DatabaseInputStream(new FileInputStream(String.valueOf(path)))) {
            Optional<DatabaseRecord> result;
            ArrayList<String> keys = new ArrayList<>();
            int size = 0;

            while (dbis.available() > 0) {
                result = dbis.readDbUnit();
                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(new String(result.get().getKey()),
                        new SegmentOffsetInfoImpl(size));
                size += result.get().size();
                keys.add(new String(result.get().getKey()));
            }

            if (size > 0) {
                context = new InitializationContextImpl(context.executionEnvironment(), context.currentDbContext(),
                        context.currentTableContext(),
                        new SegmentInitializationContextImpl(
                                context.currentSegmentContext().getSegmentName(),
                                context.currentSegmentContext().getSegmentPath(),
                                size,
                                context.currentSegmentContext().getIndex()));
            }

            Segment initializedSegment = SegmentImpl.initializeFromContext(context.currentSegmentContext());

            context.currentTableContext().updateCurrentSegment(initializedSegment);
            for (var key : keys) {
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key,
                        initializedSegment);
            }
        } catch (IOException e) {
            throw new DatabaseException(String.format("IO exception when trying to initialize segment %s",
                    context.currentSegmentContext().getSegmentName()), e);
        }
    }
}