package org.embulk.filter.base64;

import com.google.common.io.BaseEncoding;

import org.embulk.filter.base64.Base64FilterPlugin.ColumnTask;
import org.embulk.filter.base64.Base64FilterPlugin.PluginTask;
import org.embulk.spi.Column;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        String encoding = colTask.getEncoding();
        boolean doEncode = colTask.getDoEncode().get();
        boolean doDecode = colTask.getDoDecode().get();
        // encode
        if (doEncode && ! doDecode) {
            byte[] inputAsBytes = inputValue.getBytes();
            String encoded = null;
            if (Objects.equals(encoding, "Base64")) {
                if (colTask.getDoUrlsafe().get()) {
                    encoded = BaseEncoding.base64Url().encode(inputAsBytes);
                }
                else {
                    encoded = BaseEncoding.base64().encode(inputAsBytes);
                }
            }
            else if (Objects.equals(encoding, "Base32")) {
                if (colTask.getUseHex().get()) {
                    encoded = BaseEncoding.base32Hex().encode(inputAsBytes);
                }
                else {
                    encoded = BaseEncoding.base32().encode(inputAsBytes);
                }
            }
            else if (Objects.equals(encoding, "Base16")) {
                encoded = BaseEncoding.base16().encode(inputAsBytes);
            }
            return encoded;
        }
        // decode
        else {
            byte[] decodedAsBytes = null;
            if (Objects.equals(encoding, "Base64")) {
                if (colTask.getDoUrlsafe().get()) {
                    decodedAsBytes = BaseEncoding.base64Url().decode(inputValue);
                }
                else {
                    decodedAsBytes = BaseEncoding.base64().decode(inputValue);
                }
            }
            else if (Objects.equals(encoding, "Base32")) {
                if (colTask.getUseHex().get()) {
                    decodedAsBytes = BaseEncoding.base32Hex().decode(inputValue);
                }
                else {
                    decodedAsBytes = BaseEncoding.base32().decode(inputValue);
                }
            }
            else if (Objects.equals(encoding, "Base16")) {
                decodedAsBytes = BaseEncoding.base16().decode(inputValue);
            }
            return new String(decodedAsBytes);
        }
    }
}
