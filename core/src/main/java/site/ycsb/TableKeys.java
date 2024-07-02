package site.ycsb;

import java.util.List;

public class TableKeys {
  String primaryKeyName;
  TableKeyType primaryKeyType;
  List<String> foreignKeyNames;
  List<TableKeyType> foreignKeyTypes;
}
