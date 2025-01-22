package com.alibaba.otter.canal.spring.boot;



import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.StrUtil;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class App {

    private static Map<Long, TableMapInfo> tableMapInfoMap = new HashMap<Long, TableMapInfo>();
    private static Map<String, List<ColumnInfo>> tableNameToColumnNamesMap = new HashMap<>();
    private static final String FILE_PATH = "D:/tmp/binglog-004826.sql";

    public static void main(String[] args) throws Exception {
        String filePath = "E:\\mysql-bin.000027";
        File binlogFile = new File(filePath);
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, eventDeserializer);
        ArrayList<String> sqlList = new ArrayList<>();

        for (Event event; (event = reader.readEvent()) != null; ) {
            // 解析表结构
            EventType eventType = event.getHeader().getEventType();
            EventData data = event.getData();
            if (eventType == EventType.TABLE_MAP) {
                TableMapEventData tableMapData = event.getData();
                long tableId = tableMapData.getTableId();
                TableMapInfo tableMapInfo = new TableMapInfo();
                tableMapInfo.database = tableMapData.getDatabase();
                tableMapInfo.table = tableMapData.getTable();
                tableMapInfoMap.put(tableId, tableMapInfo);
            }

            if (eventType == EventType.WRITE_ROWS) {
                WriteRowsEventData writeRowsData = (WriteRowsEventData) data;
                long tableId = writeRowsData.getTableId();
                TableMapInfo tableMapInfo = tableMapInfoMap.get(tableId);
                String database = tableMapInfo.database;
                String table = tableMapInfo.table;
                List<ColumnInfo> columnInfos = tableNameToColumnNamesMap.get((database + "." + table).toLowerCase());
                if (CollUtil.isEmpty(columnInfos)) {
                    continue;
                }
                for (Object[] row : writeRowsData.getRows()) {
                    List<String> dataList = getDataStr(row);
                    StringBuilder sb = new StringBuilder();
                    sb.append("INSERT INTO ").append(("\"" + database + "\".\"" + table).toUpperCase() + "\" (");
                    for (int i = 0; i < dataList.size(); i++) {
                        int finalI = i;
                        ColumnInfo columnInfo = columnInfos.stream().filter(item -> item.getInx() == finalI).collect(Collectors.toList()).get(0);
                        sb.append("\"").append(columnInfo.colName.toUpperCase().trim()).append("\"").append(",");
                    }
                    sb.deleteCharAt(sb.length() - 1);
                    sb.append(" )values (");
                    for (int i = 0; i < dataList.size(); i++) {
                        String val = dataList.get(i);
                        int finalI = i;
                        ColumnInfo columnInfo = columnInfos.stream().filter(item -> item.getInx() == finalI).collect(Collectors.toList()).get(0);
                        String dataType = columnInfo.getDataType();
                        String resVal = getVal(val, dataType);
                        sb.append("'").append(resVal).append("'").append(",");
                    }
                    sb.deleteCharAt(sb.length() - 1);
                    sb.append(");\n");
                    System.out.println(sb);
                    /*
                    synchronized (sqlList) {
                        sqlList.add(sb.toString());
                        if (sqlList.size() > 1000) {
                            writeToFile(FILE_PATH, sqlList);
                            sqlList.clear();
                        }
                    }*/
                }
            } else if (eventType == EventType.UPDATE_ROWS) {
                UpdateRowsEventData updateRowsData = (UpdateRowsEventData) data;
                long tableId = updateRowsData.getTableId();
                TableMapInfo tableMapInfo = tableMapInfoMap.get(tableId);
                String database = tableMapInfo.database;
                String table = tableMapInfo.table;
                List<ColumnInfo> columnInfos = tableNameToColumnNamesMap.get((database + "." + table).toLowerCase());
                if (CollUtil.isEmpty(columnInfos)) {
                    continue;
                }
                for (Map.Entry<Serializable[], Serializable[]> row : updateRowsData.getRows()) {
                    Serializable[] beforeUpdate = row.getKey();
                    Serializable[] afterUpdate = row.getValue();

                    List<String> beforeData = getDataStr(beforeUpdate);
                    List<String> afterData = getDataStr(afterUpdate);
                    StringBuilder sb = new StringBuilder();
                    sb.append("UPDATE ").append(("\"" + database + "\".\"" + table + "\" SET ").toUpperCase());
                    for (int i = 0; i < afterData.size(); i++) {
                        if (!afterData.get(i).equals(beforeData.get(i))) {
                            int finalI = i;
                            ColumnInfo columnInfo = columnInfos.stream().filter(item -> item.getInx() == finalI).collect(Collectors.toList()).get(0);
                            String val = afterData.get(i);
                            String dataType = columnInfo.getDataType();
                            String resVal = getVal(val, dataType);
                            sb.append("\"").append(columnInfo.colName.toUpperCase().trim()).append("\"=")
                                    .append("'").append(resVal).append("'").append(",");
                        }
                    }
                    sb.deleteCharAt(sb.length() - 1);
                    sb.append(" where ");
                    for (int i = 0; i < afterData.size(); i++) {
                        if (ObjUtil.isNotEmpty(afterData.get(i)) && afterData.get(i).equals(beforeData.get(i))) {
                            int finalI = i;
                            ColumnInfo columnInfo = columnInfos.stream().filter(item -> item.getInx() == finalI).collect(Collectors.toList()).get(0);
                            String val = afterData.get(i);
                            String dataType = columnInfo.getDataType();
                            String resVal = getVal(val, dataType);
                            sb.append("\"").append(columnInfo.colName.toUpperCase().trim()).append("\"=")
                                    .append("'").append(resVal).append("'").append(" AND ");
                        }
                    }
                    sb.delete(sb.length() - 5, sb.length());
                    sb.append(";\n");
                    System.out.println(sb);
                    synchronized (sqlList) {
                        sqlList.add(sb.toString());
                        if (sqlList.size() > 1000) {
                            writeToFile(FILE_PATH, sqlList);
                            sqlList.clear();
                        }
                    }
                }
            } else if (eventType == EventType.DELETE_ROWS) {

                DeleteRowsEventData deleteRowsData = (DeleteRowsEventData) data;
                long tableId = deleteRowsData.getTableId();
                TableMapInfo tableMapInfo = tableMapInfoMap.get(tableId);
                String database = tableMapInfo.database;
                String table = tableMapInfo.table;
                List<ColumnInfo> columnInfos = tableNameToColumnNamesMap.get((database + "." + table).toLowerCase());
                if (CollUtil.isEmpty(columnInfos)) {
                    continue;
                }
                StringBuilder sb = new StringBuilder();
                sb.append("DELETE FROM ").append(("\"" + database + "\".\"" + table).toUpperCase() + "\"  WHERE ");
                for (Object[] row : deleteRowsData.getRows()) {
                    List<String> dataList = getDataStr(row);
                    for (int i = 0; i < dataList.size(); i++) {
                        int finalI = i;
                        ColumnInfo columnInfo = columnInfos.stream().filter(item -> item.getInx() == finalI).collect(Collectors.toList()).get(0);
                        String val = dataList.get(i);
                        String dataType = columnInfo.getDataType();
                        String resVal = getVal(val, dataType);
                        sb.append("\"").append(columnInfo.colName.toUpperCase().trim()).append("\"=")
                                .append("'").append(resVal).append("'").append(" AND ");
                    }
                    sb.delete(sb.length() - 5, sb.length());
                    sb.append(";\n");
                    System.out.println(sb);
                    synchronized (sqlList) {
                        sqlList.add(sb.toString());
                        if (sqlList.size() > 1000) {
                            writeToFile(FILE_PATH, sqlList);
                            sqlList.clear();
                        }
                    }
                }
            }
        }
        reader.close();
    }

    private static String getVal(String val, String dataType) {
        if (StrUtil.isBlank(val) || "null".equalsIgnoreCase(val)) {
            return "NULL";
        }
        if (dataType.equalsIgnoreCase("date")) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDate startDate = LocalDate.of(1970, 1, 1);
            try {
                LocalDate endDate = startDate.plusDays(Long.parseLong(val));
                val = endDate.format(formatter);
            } catch (Exception e) {
                LocalDate localDate = LocalDate.ofEpochDay(Long.parseLong(val) / (1000 * 60 * 60 * 24));
                val = localDate.format(formatter);
            }


        } else if (dataType.equalsIgnoreCase("datetime") || dataType.equalsIgnoreCase("timestamp")) {
            LocalDateTime localDateTime = LocalDateTimeUtil.of(new Date(Long.parseLong(val)));
            val = localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
        Pattern compile = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})T(\\d{2}:\\d{2}:\\d{2})Z");
        Matcher matcher = compile.matcher(val);
        if (matcher.find()) {
            val = matcher.group(1) + " " + matcher.group(2);
        }
        return val;
    }

    public static List<String> getDataStr(Object[] data) {
        ArrayList<String> res = new ArrayList<>();
        for (Object datum : data) {
            if (datum instanceof byte[]) {
                res.add(new String((byte[]) datum, CharsetUtil.CHARSET_UTF_8));
            } else {
                res.add(datum + "");
            }
        }
        return res;
    }



    public static void writeToFile(String path, List<String> contents) {
        if (CollUtil.isEmpty(contents)) {
            return;
        }
        File file = new File(path);
        if (file == null || !file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true))) {
            for (int i = 0; i < contents.size(); i++) {
                bufferedWriter.write(contents.get(i));
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}


