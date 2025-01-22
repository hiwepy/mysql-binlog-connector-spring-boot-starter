package com.alibaba.otter.canal.spring.boot;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class BinaryLogClientTest {

    public static void main(String[] args) throws Exception {

        BinaryLogClient client = new BinaryLogClient("127.0.0.1", 3306, "test", "root", "123456");
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                EventHeader header = event.getHeader() ;
                switch(header.getEventType()) {
                    case EXT_WRITE_ROWS:
                        WriteRowsEventData writeData = event.getData() ;
                        List<Serializable[]> rows = writeData.getRows() ;
                        for (Serializable row : rows) {
                            if (row.getClass().isArray()) {
                                printRow(row);
                            }
                        }
                        break ;
                    case EXT_UPDATE_ROWS:
                        UpdateRowsEventData updateData = event.getData() ;
                        BitSet columns = updateData.getIncludedColumns() ;
                        System.err.printf("更新列: %s%n", columns) ;
                        List<Map.Entry<Serializable[], Serializable[]>> updateRows = updateData.getRows() ;
                        for (Map.Entry<Serializable[], Serializable[]> entry : updateRows) {
                            printRow(entry.getKey()) ;
                            System.out.println(">>>>>>>>>>>>>>>>>>>>>before") ;
                            printRow(entry.getValue()) ;
                            System.out.println(">>>>>>>>>>>>>>>>>>>>>after") ;
                        }
                        break ;
                    case EXT_DELETE_ROWS:
                        DeleteRowsEventData deleteData = event.getData() ;
                        List<Serializable[]> deleteRow = deleteData.getRows() ;
                        for (Serializable row : deleteRow) {
                            if (row.getClass().isArray()) {
                                printRow(row);
                            }
                        }
                        break ;
                    case TABLE_MAP:
                        TableMapEventData data = event.getData() ;
                        System.out.printf("变更表: %s.%s%n", data.getDatabase(), data.getTable()) ;
                        break ;
                    default:
                        break ;
                }
            }
            private void printRow(Serializable row) {
                Serializable[] ss = (Serializable[]) row ;
                for (Serializable s : ss) {
                    if (s.getClass().isArray()) {
                        System.out.print(new String((byte[])s) + "\t") ;
                    } else {
                        System.out.print(s + "\t") ;
                    }
                }
                System.out.println() ;
            }
        });
        client.connect();
    }
}
