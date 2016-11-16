package org.embulk.filter.base64;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Base64;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class Base64FilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("columns")
        public List<Base64ColumnTask> getColumns();
    }

    public interface Base64ColumnTask
            extends Task
    {
        @Config("name")
        public String getName();

        @Config("encode")
        @ConfigDefault("false")
        public Optional<Boolean> getDoEncode();

        @Config("decode")
        @ConfigDefault("false")
        public Optional<Boolean> getDoDecode();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        for (Base64ColumnTask base64ColTask: task.getColumns()) {
            // throws exception if the column name does not exist
            inputSchema.lookupColumn(base64ColTask.getName());
        }
        Schema outputSchema = inputSchema;
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        final Map<String, Base64ColumnTask> base64ColumnMap = getBase64ColumnMap(task.getColumns());

        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(
                Exec.getBufferAllocator(), outputSchema, output);
            private ColumnVisitorImpl visitor = new ColumnVisitorImpl(builder);

            @Override
            public void add(Page page)
            {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    for (Column column: inputSchema.getColumns()) {
                        String colName = column.getName();
                        Type colType = column.getType();
                        Base64ColumnTask colTask = base64ColumnMap.get(colName);
                        // columns where nothing to be done
                        if (colTask == null) {
                            column.visit(visitor);
                            continue;
                        }
                        Boolean doEncode = colTask.getDoEncode().get();
                        Boolean doDecode = colTask.getDoDecode().get();
                        Boolean bothTrue = doEncode && doDecode;
                        Boolean bothFalse = !doEncode && !doDecode;
                        if (bothTrue || bothFalse) {
                            String errMsg = "Specify either 'encode: true' or 'decode: true'";
                            throw new Base64ValidateException(errMsg);
                        }
                        if (!Types.STRING.equals(colType)) {
                            String errMsg = "Type of input columns must be string";
                            throw new Base64ValidateException(errMsg);
                        }
                        // encode
                        if (doEncode) {
                            String raw = reader.getString(column);
                            String encoded = Base64.getEncoder().encodeToString(raw.getBytes());
                            builder.setString(column, encoded);
                        }
                        // decode
                        if (doDecode) {
                            String encoded = reader.getString(column);
                            String decoded = new String(Base64.getDecoder().decode(encoded));
                            builder.setString(column, decoded);
                        }
                    }
                    builder.addRecord();
                }
            }

            @Override
            public void finish()
            {
                builder.finish();
            }

            @Override
            public void close()
            {
                builder.close();
            }

            class ColumnVisitorImpl implements ColumnVisitor
            {
                private final PageBuilder builder;

                ColumnVisitorImpl(PageBuilder builder) {
                    this.builder = builder;
                }

                @Override
                public void booleanColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    } else {
                        builder.setBoolean(outputColumn, reader.getBoolean(outputColumn));
                    }
                }

                @Override
                public void longColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    } else {
                        builder.setLong(outputColumn, reader.getLong(outputColumn));
                    }
                }

                @Override
                public void doubleColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    } else {
                        builder.setDouble(outputColumn, reader.getDouble(outputColumn));
                    }
                }

                @Override
                public void stringColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    } else {
                        builder.setString(outputColumn, reader.getString(outputColumn));
                    }
                }

                @Override
                public void timestampColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    } else {
                        builder.setTimestamp(outputColumn, reader.getTimestamp(outputColumn));
                    }
                }

                @Override
                public void jsonColumn(Column outputColumn)
                {
                    if (reader.isNull(outputColumn)) {
                        builder.setNull(outputColumn);
                    } else {
                        builder.setJson(outputColumn, reader.getJson(outputColumn));
                    }
                }
            }
        };
    }

    static Map<String, Base64ColumnTask> getBase64ColumnMap(
            List<Base64ColumnTask> columnTasks)
    {
        Map<String, Base64ColumnTask> m = new HashMap<>();
        for (Base64ColumnTask columnTask: columnTasks) {
            m.put(columnTask.getName(), columnTask);
        }
        return m;
    }

    static class Base64ValidateException
            extends DataException
    {
        Base64ValidateException(String message)
        {
            super(message);
        }
    }
}
