package org.embulk.filter.base64;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.filter.base64.Base64FilterPlugin.PluginTask;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.type.Types;

import org.junit.Rule;
import org.junit.Test;

public class TestBase64FilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    public Base64FilterPlugin plugin = new Base64FilterPlugin();

    public static PluginTask taskFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        ConfigSource config = loader.fromYamlString(yamlString);
        return config.loadConfig(PluginTask.class);
    }

    @Test(expected = SchemaConfigException.class)
    public void testValidate_noColumn()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: to encode, type: string, encode: true}",
            "  - {name: to decode, type: string, decode: true}"
        );
        Schema inputSchema = Schema.builder()
            .add("to encode", Types.STRING)
            .build();
        plugin.validate(task, inputSchema);
    }

    @Test(expected = DataException.class)
    public void testValidate_bothSpecified()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: to encode, type: string, encode: true, decode: true}"
        );
        Schema inputSchema = Schema.builder()
            .add("to encode", Types.STRING)
            .build();
        plugin.validate(task, inputSchema);
    }

    @Test(expected = DataException.class)
    public void testValidate_bothNotSpecified()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: to encode, type: string}"
        );
        Schema inputSchema = Schema.builder()
            .add("to encode", Types.STRING)
            .build();
        plugin.validate(task, inputSchema);
    }

    @Test(expected = DataException.class)
    public void testValidate_invalidInputType()
    {
        PluginTask task = taskFromYamlString(
            "type: base64",
            "columns:",
            "  - {name: to encode, type: string, type: double}"
        );
        Schema inputSchema = Schema.builder()
            .add("to encode", Types.DOUBLE)
            .build();
        plugin.validate(task, inputSchema);
    }
}
