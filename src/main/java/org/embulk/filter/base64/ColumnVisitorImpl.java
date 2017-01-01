package org.embulk.filter.base64;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Base64;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

import org.embulk.filter.base64.Base64FilterPlugin.Base64ColumnTask;
import org.embulk.filter.base64.Base64FilterPlugin.PluginTask;

public class ColumnVisitorImpl
        implements ColumnVisitor
{
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final Map<String, Base64ColumnTask> base64ColumnMap;

    ColumnVisitorImpl(PluginTask task, PageReader reader, PageBuilder builder)
    {
        this.pageReader = reader;
        this.pageBuilder = builder;
        this.base64ColumnMap = getBase64ColumnMap(task.getColumns());
    }

    private static Map<String, Base64ColumnTask> getBase64ColumnMap(
            List<Base64ColumnTask> columnTasks)
    {
        Map<String, Base64ColumnTask> m = new HashMap<>();
        for (Base64ColumnTask columnTask : columnTasks) {
            m.put(columnTask.getName(), columnTask);
        }
        return m;
    }

    private Base64ColumnTask getTask(Column column)
    {
        String colName = column.getName();
        return base64ColumnMap.get(colName);
    }

    private String executeTask(Base64ColumnTask task, Column column)
    {
        boolean doEncode = task.getDoEncode().get();
        boolean doDecode = task.getDoDecode().get();
        // encode
        if (doEncode) {
            String raw = pageReader.getString(column);
            return Base64.getEncoder().encodeToString(raw.getBytes());
        }
        // decode
        //else if (doDecode) {
        else {
            String encoded = pageReader.getString(column);
            return new String(Base64.getDecoder().decode(encoded));
        }
    }

    @Override
    public void booleanColumn(Column outputColumn)
    {
        if (pageReader.isNull(outputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setBoolean(
                outputColumn, pageReader.getBoolean(outputColumn));
        }
    }

    @Override
    public void longColumn(Column outputColumn)
    {
        if (pageReader.isNull(outputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setLong(
                outputColumn, pageReader.getLong(outputColumn));
        }
    }

    @Override
    public void doubleColumn(Column outputColumn)
    {
        if (pageReader.isNull(outputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setDouble(
                outputColumn, pageReader.getDouble(outputColumn));
        }
    }

    @Override
    public void stringColumn(Column outputColumn)
    {
        if (pageReader.isNull(outputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            Base64ColumnTask task = getTask(outputColumn);
            // when there is no task executed on this column
            if (task == null) {
                pageBuilder.setString(
                    outputColumn, pageReader.getString(outputColumn));
            // when there is a task
            }
            else {
                String str = executeTask(task, outputColumn);
                pageBuilder.setString(outputColumn, str);
            }
        }
    }

    @Override
    public void timestampColumn(Column outputColumn)
    {
        if (pageReader.isNull(outputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setTimestamp(
                outputColumn, pageReader.getTimestamp(outputColumn));
        }
    }

    @Override
    public void jsonColumn(Column outputColumn)
    {
        if (pageReader.isNull(outputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setJson(
                outputColumn, pageReader.getJson(outputColumn));
        }
    }
}
