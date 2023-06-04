package cn.edu.thssdb.schema;

import java.util.List;

public class ForeignKeyConstraint {
    public Table foreignTable;
    public List<String> localColumnList;
    public List<String> foreignColumnList;
    public ForeignKeyConstraint(Table foreignTable, List<String> localColumnList, List<String> foreignColumnList)
    {
        this.foreignTable = foreignTable;
        this.localColumnList = localColumnList;
        this.foreignColumnList = foreignColumnList;
    }
}
