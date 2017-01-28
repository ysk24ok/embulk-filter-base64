package org.embulk.filter.base64;

import com.google.common.base.Optional;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import java.util.List;

public class Base64FilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("columns")
        public List<ColumnTask> getColumns();
    }

    public interface ColumnTask
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

    public void validate(PluginTask pluginTask, Schema inputSchema)
    {
        for (ColumnTask task : pluginTask.getColumns()) {
            // throws exception when the column does not exist
            inputSchema.lookupColumn(task.getName());
            boolean doEncode = task.getDoEncode().get();
            boolean doDecode = task.getDoDecode().get();
            boolean bothTrue = doEncode && doDecode;
            boolean bothFalse = !doEncode && !doDecode;
            if (bothTrue || bothFalse) {
                String errMsg = "Specify either 'encode: true' or 'decode: true'";
                throw new DataException(errMsg);
            }
        }
        for (Column column : inputSchema.getColumns()) {
            Type colType = column.getType();
            if (!Types.STRING.equals(colType)) {
                String errMsg = "Type of input columns must be string";
                throw new DataException(errMsg);
            }
        }
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        validate(task, inputSchema);
        Schema outputSchema = inputSchema;
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        PageBuilder pageBuilder = new PageBuilder(
            Exec.getBufferAllocator(), outputSchema, output);
        PageReader pageReader = new PageReader(inputSchema);
        ColumnVisitorImpl visitor = new ColumnVisitorImpl(
            task, pageReader, pageBuilder);

        return new PageOutputImpl(
            pageReader, pageBuilder, outputSchema, visitor);
    }

    public static class PageOutputImpl implements PageOutput
    {
        private PageReader pageReader;
        private PageBuilder pageBuilder;
        private Schema outputSchema;
        private ColumnVisitorImpl visitor;

        PageOutputImpl(PageReader pageReader, PageBuilder pageBuilder, Schema outputSchema, ColumnVisitorImpl visitor)
        {
            this.pageReader = pageReader;
            this.pageBuilder = pageBuilder;
            this.outputSchema = outputSchema;
            this.visitor = visitor;
        }

        @Override
        public void add(Page page)
        {
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                outputSchema.visitColumns(visitor);
                pageBuilder.addRecord();
            }
        }

        @Override
        public void finish()
        {
            pageBuilder.finish();
        }

        @Override
        public void close()
        {
            pageBuilder.close();
        }
    };
}
