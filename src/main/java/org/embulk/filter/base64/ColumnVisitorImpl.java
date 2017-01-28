package org.embulk.filter.base64;

import org.embulk.filter.base64.Base64FilterPlugin.ColumnTask;
import org.embulk.filter.base64.Base64FilterPlugin.PluginTask;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnVisitorImpl implements ColumnVisitor
{
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final Map<String, ColumnTask> columnTaskMap;

    ColumnVisitorImpl(PluginTask task, PageReader reader, PageBuilder builder)
    {
        this.pageReader = reader;
        this.pageBuilder = builder;
        this.columnTaskMap = getColumnTaskMap(task.getColumns());
    }

    private Map<String, ColumnTask> getColumnTaskMap(
            List<ColumnTask> columnTasks)
    {
        Map<String, ColumnTask> m = new HashMap<>();
        for (ColumnTask columnTask : columnTasks) {
            m.put(columnTask.getName(), columnTask);
        }
        return m;
    }

    private ColumnTask getTask(Column column)
    {
        String colName = column.getName();
        return columnTaskMap.get(colName);
    }

    private String executeTask(ColumnTask task, Column column)
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
            ColumnTask task = getTask(outputColumn);
            if (task == null) {
                pageBuilder.setString(
                    outputColumn, pageReader.getString(outputColumn));
            }
            else {
                pageBuilder.setString(
                    outputColumn, executeTask(task, outputColumn));
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
