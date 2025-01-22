package com.alibaba.otter.canal.spring.boot;

import lombok.*;

import java.io.Serializable;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class ColumnInfo implements Serializable {

    public int inx;
    public String colName;
    public String dataType;
    public String schema;
    public String table;
    public boolean isPKC;

    public ColumnInfo(String schema, String table, int idx, String colName, String dataType, boolean isPKC) {
        this.schema = schema;
        this.table = table;
        this.colName = colName;
        this.dataType = dataType;
        this.inx = idx;
        this.isPKC = isPKC;
    }
}
