package com.alibaba.otter.canal.spring.boot;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.File;
import java.util.Collections;

public class BinaryLogFileReaderTest {

    private static final String FILE_PATH = "D:/tmp/binglog-000028-2.sql";


    public static void main(String[] args) throws Exception {

        File binlogFile = new File("E:\\mysql-bin.000028") ;
        EventDeserializer eventDeserializer = new EventDeserializer() ;
        // 设置兼容性模式
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
        try (BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, eventDeserializer)) {
            for (Event event; (event = reader.readEvent()) != null; ) {
                EventData data = event.getData();
                // 判断事件的类型
                /*if (data instanceof WriteRowsEventData) {
                    WriteRowsEventData ed = (WriteRowsEventData) data;
                    System.out.printf("新增事件：%s%n", ed.getIncludedColumns().toString());
                    List<Serializable[]> rows = ed.getRows();
                    rows.forEach(row -> {
                        for (Serializable s : row) {
                            if (s instanceof byte[]) {
                                byte[] bs = (byte[]) s;
                                System.err.print(new String(bs) + "\t");
                            } else {
                                System.err.print(s + "\t");
                            }
                        }
                        System.out.println();
                    });
                } else */if (data instanceof QueryEventData) {
                    QueryEventData ed = (QueryEventData) data;
                    if (!ed.getDatabase().equals("ty_portfolio_new")) {
                        continue;
                    }
                    if (!ed.getSql().contains("growth_template_page_user")) {
                        continue;
                    }
                    if (!ed.getSql().contains("1873614200041103404")) {
                        continue;
                    }
                    System.out.printf("查询事件：%s%n", ed.getSql());
                    FileUtils.writeLines(new File(FILE_PATH), Collections.singletonList(ed.getSql()), true);
                } else if (data instanceof DeleteRowsEventData) {
                    DeleteRowsEventData ed = (DeleteRowsEventData) data;
                    System.err.println("删除事件");
                } else if (data instanceof UpdateRowsEventData) {
                    UpdateRowsEventData ed = (UpdateRowsEventData) data;
                    System.err.println("更新事件");
                } else if (data instanceof RowsQueryEventData) {
                    RowsQueryEventData ed = (RowsQueryEventData) data;
                    System.err.println("行查询事件");
                } else if (data instanceof TableMapEventData) {
                    TableMapEventData ed = (TableMapEventData) data;
                    String database = ed.getDatabase();
                    String table = ed.getTable();
                    System.out.printf("数据库: %s, 表名: %s%n", database, table);
                }
            }
        }
    }
}
