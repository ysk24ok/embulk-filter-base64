package org.embulk.filter.base64;

import org.embulk.EmbulkTestRuntime;
import org.embulk.filter.base64.Base64FilterPlugin.PageOutputImpl;
import org.embulk.filter.base64.Base64FilterPlugin.PluginTask;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Pages;

import org.junit.Rule;
import org.junit.Test;

import static org.embulk.filter.base64.TestBase64FilterPlugin.taskFromYamlString;
import static org.junit.Assert.assertEquals;

import java.util.List;

public class TestColumnVisitorImpl
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private List<Object[]> filter(PluginTask task, Schema inputSchema, Object... objects)
    {
        MockPageOutput output = new MockPageOutput();
        Schema outputSchema = inputSchema;
        PageBuilder pageBuilder = new PageBuilder(
            runtime.getBufferAllocator(), outputSchema, output);
        PageReader pageReader = new PageReader(inputSchema);
        Base64Filter filter = new Base64Filter(task);
        ColumnVisitorImpl visitor = new ColumnVisitorImpl(
            pageReader, pageBuilder, filter);

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
