package org.embulk.filter.base64;

import org.embulk.filter.base64.Base64FilterPlugin.ColumnTask;
import org.embulk.filter.base64.Base64FilterPlugin.PluginTask;
import org.embulk.spi.Column;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Base64Filter
{
    private PluginTask task;
    private final Map<String, ColumnTask> columnTaskMap;

    Base64Filter(PluginTask task)
    {
        this.task = task;
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

    public String doFilter(Column column, String inputValue)
    {
        ColumnTask colTask = getTask(column);
        if (colTask == null) {
            return inputValue;
        }
        boolean doEncode = colTask.getDoEncode().get();
        boolean doDecode = colTask.getDoDecode().get();
        // encode
        if (doEncode && ! doDecode) {
            return Base64.getEncoder().encodeToString(inputValue.getBytes());
        }
        // decode
        return new String(Base64.getDecoder().decode(inputValue));
    }
}
