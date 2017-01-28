package org.embulk.filter.base64;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

public class ColumnVisitorImpl implements ColumnVisitor
{
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final Base64Filter filter;

    ColumnVisitorImpl(PageReader reader, PageBuilder builder, Base64Filter filter)
    {
        this.pageReader = reader;
        this.pageBuilder = builder;
        this.filter = filter;
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
            String outputValue = filter.doFilter(
                outputColumn, pageReader.getString(outputColumn));
            pageBuilder.setString(outputColumn, outputValue);
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
