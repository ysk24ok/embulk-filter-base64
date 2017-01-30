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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.filter.base64.TestBase64FilterPlugin.taskFromYamlString;
import static org.junit.Assert.assertEquals;

import java.util.List;

public class TestColumnVisitorImpl
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private Schema inputSchemaToEncode;
    private Schema inputSchemaToDecode;

    @Before
    public void createResource()
    {
        inputSchemaToEncode = Schema.builder()
            .add("id", Types.LONG)
            .add("to encode", Types.STRING)
            .build();
        inputSchemaToDecode = Schema.builder()
            .add("id", Types.LONG)
            .add("to decode", Types.STRING)
            .build();
    }

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
    public void testExecuteTask_encodeByBase64()
    {
        // implicit
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to encode, encode: true}"
        );
        List<Object[]> records = filter(task, inputSchemaToEncode,
            Long.valueOf(100), "A0?B1>",
            Long.valueOf(101), "ab?cd~"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("QTA/QjE+", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("YWI/Y2R+", records.get(1)[1]);
        // explicit
        task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to encode, encode: true, encoding: Base64}"
        );
        records = filter(task, inputSchemaToEncode,
            Long.valueOf(100), "A0?B1>",
            Long.valueOf(101), "ab?cd~"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("QTA/QjE+", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("YWI/Y2R+", records.get(1)[1]);
    }

    @Test
    public void testExecuteTask_encodeByBase64Url()
    {
        // implicit
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to encode, encode: true, urlsafe: true}"
        );
        List<Object[]> records = filter(task, inputSchemaToEncode,
            Long.valueOf(100), "A0?B1>",
            Long.valueOf(101), "ab?cd~"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("QTA_QjE-", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("YWI_Y2R-", records.get(1)[1]);
        // explicit
        task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to encode, encode: true, encoding: Base64, urlsafe: true}"
        );
        records = filter(task, inputSchemaToEncode,
            Long.valueOf(100), "A0?B1>",
            Long.valueOf(101), "ab?cd~"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("QTA_QjE-", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("YWI_Y2R-", records.get(1)[1]);
    }

    @Test
    public void testExecuteTask_encodeByBase32()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to encode, encode: true, encoding: Base32}"
        );
        List<Object[]> records = filter(task, inputSchemaToEncode,
            Long.valueOf(100), "A0?B1>",
            Long.valueOf(101), "ab?cd~"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("IEYD6QRRHY======", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("MFRD6Y3EPY======", records.get(1)[1]);
    }

    @Test
    public void testExecuteTask_encodeByBase32Hex()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to encode, encode: true, encoding: Base32, hex: true}"
        );
        List<Object[]> records = filter(task, inputSchemaToEncode,
            Long.valueOf(100), "A0?B1>",
            Long.valueOf(101), "ab?cd~"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("84O3UGHH7O======", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("C5H3UOR4FO======", records.get(1)[1]);
    }

    @Test
    public void testExecuteTask_encodeByBase16()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to encode, encode: true, encoding: Base16}"
        );
        List<Object[]> records = filter(task, inputSchemaToEncode,
            Long.valueOf(100), "A0?B1>",
            Long.valueOf(101), "ab?cd~"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("41303F42313E", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("61623F63647E", records.get(1)[1]);
    }

    @Test
    public void testExecuteTask_decodeByBase64()
    {
        // implicit
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to decode, decode: true}"
        );
        List<Object[]> records = filter(task, inputSchemaToDecode,
            Long.valueOf(100), "QTA/QjE+",
            Long.valueOf(101), "YWI/Y2R+"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("A0?B1>", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("ab?cd~", records.get(1)[1]);
        // explicit
        task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to decode, decode: true, encoding: Base64}"
        );
        records = filter(task, inputSchemaToDecode,
            Long.valueOf(100), "QTA/QjE+",
            Long.valueOf(101), "YWI/Y2R+"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("A0?B1>", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("ab?cd~", records.get(1)[1]);
    }

    @Test
    public void testExecuteTask_decodeByBase64Url()
    {
        // implicit
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to decode, decode: true, urlsafe: true}"
        );
        List<Object[]> records = filter(task, inputSchemaToDecode,
            Long.valueOf(100), "QTA_QjE-",
            Long.valueOf(101), "YWI_Y2R-"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("A0?B1>", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("ab?cd~", records.get(1)[1]);
        // explicit
        task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to decode, decode: true, encoding: Base64, urlsafe: true}"
        );
        records = filter(task, inputSchemaToDecode,
            Long.valueOf(100), "QTA_QjE-",
            Long.valueOf(101), "YWI_Y2R-"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("A0?B1>", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("ab?cd~", records.get(1)[1]);
    }

    @Test
    public void testExecuteTask_decodeByBase32()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to decode, decode: true, encoding: Base32}"
        );
        List<Object[]> records = filter(task, inputSchemaToDecode,
            Long.valueOf(100), "IEYD6QRRHY======",
            Long.valueOf(101), "MFRD6Y3EPY======"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("A0?B1>", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("ab?cd~", records.get(1)[1]);
    }

    @Test
    public void testExecuteTask_decodeByBase32Hex()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to decode, decode: true, encoding: Base32, hex: true}"
        );
        List<Object[]> records = filter(task, inputSchemaToDecode,
            Long.valueOf(100), "84O3UGHH7O======",
            Long.valueOf(101), "C5H3UOR4FO======"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("A0?B1>", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("ab?cd~", records.get(1)[1]);
    }

    @Test
    public void testExecuteTask_decodeByBase16()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: id}",
            "  - {name: to decode, decode: true, encoding: Base16}"
        );
        List<Object[]> records = filter(task, inputSchemaToDecode,
            Long.valueOf(100), "41303F42313E",
            Long.valueOf(101), "61623F63647E"
        );
        assertEquals(2, records.size());
        assertEquals(Long.valueOf(100), records.get(0)[0]);
        assertEquals("A0?B1>", records.get(0)[1]);
        assertEquals(Long.valueOf(101), records.get(1)[0]);
        assertEquals("ab?cd~", records.get(1)[1]);
    }
}
