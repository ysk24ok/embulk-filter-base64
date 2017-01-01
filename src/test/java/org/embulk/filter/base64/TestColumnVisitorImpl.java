package org.embulk.filter.base64;

import java.util.List;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Pages;
import org.embulk.filter.base64.Base64FilterPlugin.PluginTask;
import org.embulk.filter.base64.ColumnVisitorImpl;
import static org.embulk.filter.base64.TestBase64FilterPlugin.taskFromYamlString;

import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestColumnVisitorImpl
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    public class PageOutputImpl implements PageOutput
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
    }

    private List<Object[]> filter(PluginTask task, Schema inputSchema, Object... objects)
    {
        MockPageOutput output = new MockPageOutput();
        Schema outputSchema = inputSchema;
        PageBuilder pageBuilder = new PageBuilder(
            runtime.getBufferAllocator(), outputSchema, output);
        PageReader pageReader = new PageReader(inputSchema);
        ColumnVisitorImpl visitor = new ColumnVisitorImpl(
            task, pageReader, pageBuilder);

        List<Page> pages = PageTestUtils.buildPage(
            runtime.getBufferAllocator(), inputSchema, objects);
        PageOutput mockPageOutput = new PageOutputImpl(
            pageReader, pageBuilder, outputSchema, visitor);
        for (Page page : pages) {
            mockPageOutput.add(page);
        }
        mockPageOutput.finish();
        mockPageOutput.close();
        return Pages.toObjects(outputSchema, output.pages);
    }

    @Test
    public void testExecuteTask_encode()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: to encode, type: string, encode: true}"
        );
        Schema inputSchema = Schema.builder()
            .add("to encode", Types.STRING)
            .build();
        List<Object[]> records = filter(task, inputSchema,
                "John",
                "David"
        );
        assertEquals(2, records.size());
        assertEquals("Sm9obg==", records.get(0)[0]);
        assertEquals("RGF2aWQ=", records.get(1)[0]);
    }

    @Test
    public void testExecuteTask_decode()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: to decode, type: string, decode: true}"
        );
        Schema inputSchema = Schema.builder()
            .add("to decode", Types.STRING)
            .build();
        List<Object[]> records = filter(task, inputSchema,
                "Sm9obg==",
                "RGF2aWQ="
        );
        assertEquals(2, records.size());
        assertEquals("John", records.get(0)[0]);
        assertEquals("David", records.get(1)[0]);
    }
}
