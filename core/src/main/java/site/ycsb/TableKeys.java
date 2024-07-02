package site.ycsb;

import java.util.ArrayList;
import java.util.List;

public class TableKeys {
  public String primaryKeyName;
  public TableKeyType primaryKeyType;
  public List<String> foreignKeyNames = new ArrayList<>();
  public List<TableKeyType> foreignKeyTypes = new ArrayList<>();

  public TableKeys create(String name, TableKeyType type) {
    this.primaryKeyName = name;
    this.primaryKeyType = type;
    return this;
  }

  public TableKeys addForeignKey(String name, TableKeyType type) {
    foreignKeyNames.add(name);
    foreignKeyTypes.add(type);
    return this;
  }
}
