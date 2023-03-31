package com.xq.litemapping;

import static android.database.Cursor.FIELD_TYPE_NULL;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LiteMapping {

    private final String dbName;
    private final String tableName;

    private final String primaryKeyName;

    private final Map<String,Class<?>> allKeyMap = new LinkedHashMap<>();

    private final InnerHelper innerHelper;

    public LiteMapping(Context context, String path, String autoPrimaryKeyName, Map<String,Class<?>> otherKeyMap, int version){
        this(context,path,true,new Pair<String,Class<?>>(autoPrimaryKeyName,Integer.class),otherKeyMap,version);
    }

    public LiteMapping(Context context, String path, Pair<String,Class<?>> primaryKeyPair, Map<String,Class<?>> otherKeyMap, int version){
        this(context,path,false,primaryKeyPair,otherKeyMap,version);
    }

    public LiteMapping(Context context, String path, boolean autoincrement, Pair<String,Class<?>> primaryKeyPair, Map<String,Class<?>> otherKeyMap, int version){

        String[] array = path.split("/");
        this.dbName = array[0];
        this.tableName = array[1];

        this.primaryKeyName = primaryKeyPair.first;

        this.allKeyMap.put(primaryKeyPair.first,primaryKeyPair.second);
        this.allKeyMap.putAll(otherKeyMap);

        this.innerHelper = new InnerHelper(context,version,autoincrement,primaryKeyPair,otherKeyMap);
    }

    public boolean insertById(Object id){
        return insertById(id,new LinkedHashMap<String,Object>());
    }

    public boolean insertById(Object id,Map<String,?> columns){
        Map<String,Object> columnsWithId = new LinkedHashMap<>(columns);
        columnsWithId.put(primaryKeyName,id);
        return insert(columnsWithId) >= 0;
    }

    public long insert(Map<String,?> columns){
        //
        ContentValues contentValues = new ContentValues();
        mapToContentValues(columns,contentValues);
        //
        return innerHelper.getWritableDatabase().insert(tableName,null,contentValues);
    }

    public List<Boolean> insertAllById(List<?> ids){
        //
        SQLiteDatabase db = innerHelper.getWritableDatabase();
        //
        try {
            List<Boolean> successList = new ArrayList<>(ids.size());
            db.beginTransaction();
            for (Object id : ids) {
                ContentValues contentValues = new ContentValues();
                putToContentValue(primaryKeyName,id,contentValues);
                successList.add(db.insert(tableName,null,contentValues) >= 0);
            }
            db.setTransactionSuccessful();
            return successList;
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            db.endTransaction();
        }
    }

    public List<Boolean> insertAllById(Map<?,Map<String,?>> columnsIdMap){
        //
        SQLiteDatabase db = innerHelper.getWritableDatabase();
        //
        try {
            List<Boolean> successList = new ArrayList<>(columnsIdMap.size());
            db.beginTransaction();
            for (Map.Entry<?,Map<String,?>> entry: columnsIdMap.entrySet()) {
                //
                Object id = entry.getKey();
                Map<String,?> columns = entry.getValue();
                //
                ContentValues contentValues = new ContentValues();
                putToContentValue(primaryKeyName,id,contentValues);
                mapToContentValues(columns,contentValues);
                successList.add(db.insert(tableName,null,contentValues) >= 0);
            }
            db.setTransactionSuccessful();
            return successList;
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            db.endTransaction();
        }
    }

    public List<Boolean> insertAllById(List<?> ids,Map<String,?> columns){
        //
        SQLiteDatabase db = innerHelper.getWritableDatabase();
        //
        try {
            List<Boolean> successList = new ArrayList<>(ids.size());
            db.beginTransaction();
            for (Object id : ids) {
                ContentValues contentValues = new ContentValues();
                putToContentValue(primaryKeyName,id,contentValues);
                mapToContentValues(columns,contentValues);
                successList.add(db.insert(tableName,null,contentValues) >= 0);
            }
            db.setTransactionSuccessful();
            return successList;
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            db.endTransaction();
        }
    }

    public List<Long> insertAll(List<Map<String,?>> columnsList){
        //
        SQLiteDatabase db = innerHelper.getWritableDatabase();
        //
        try {
            List<Long> idList = new ArrayList<>(columnsList.size());
            db.beginTransaction();
            for (Map<String,?> column : columnsList) {
                ContentValues contentValues = new ContentValues();
                mapToContentValues(column,contentValues);
                idList.add(db.insert(tableName,null,contentValues));
            }
            db.setTransactionSuccessful();
            return idList;
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            db.endTransaction();
        }
    }

    public List<Long> batchInsert(int count,Map<String,?> columns){
        //
        SQLiteDatabase db = innerHelper.getWritableDatabase();
        //
        try {
            List<Long> idList = new ArrayList<>(count);
            db.beginTransaction();
            for (int i=0;i<count;i++) {
                ContentValues contentValues = new ContentValues();
                mapToContentValues(columns,contentValues);
                idList.add(db.insert(tableName,null,contentValues));
            }
            db.setTransactionSuccessful();
            return idList;
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            db.endTransaction();
        }
    }

    public boolean delete(Object id){
        return innerHelper.getWritableDatabase().delete(tableName,String.format("%s = ?", primaryKeyName),objArrayToStringArray(new Object[]{id})) == 1;
    }

    public List<Boolean> deleteAll(List<?> ids){
        //
        SQLiteDatabase db = innerHelper.getWritableDatabase();
        //
        try {
            List<Boolean> successList = new ArrayList<>(ids.size());
            db.beginTransaction();
            for (Object id : ids) {
                successList.add(db.delete(tableName,String.format("%s = ?", primaryKeyName),objArrayToStringArray(new Object[]{id})) == 1);
            }
            db.setTransactionSuccessful();
            return successList;
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            db.endTransaction();
        }
    }

    public boolean update(Object id, Map<String,?> columns){
        //
        ContentValues contentValues = new ContentValues();
        mapToContentValues(columns,contentValues);
        //
        return innerHelper.getWritableDatabase().update(tableName,contentValues,String.format("%s = ?", primaryKeyName),objArrayToStringArray(new Object[]{id})) == 1;
    }

    public List<Boolean> updateAll(List<?> ids, Map<String,?> columns){
        //
        ContentValues contentValues = new ContentValues();
        mapToContentValues(columns,contentValues);
        //
        SQLiteDatabase db = innerHelper.getWritableDatabase();
        //
        try {
            List<Boolean> successList = new ArrayList<>(ids.size());
            db.beginTransaction();
            for (Object id : ids) {
                successList.add(db.update(tableName,contentValues,String.format("%s = ?", primaryKeyName),objArrayToStringArray(new Object[]{id})) == 1);
            }
            db.setTransactionSuccessful();
            return successList;
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            db.endTransaction();
        }
    }

    public List<?> queryId(){
        return queryId(new QueryArgument());
    }

    public List<?> queryId(QueryArgument queryArgument){
        Pair<String,String[]> selectionPair = conditionToSelection(queryArgument.getConditions(),queryArgument.getConditionLink());
        return queryIdListByCursor(innerHelper.getReadableDatabase().query(tableName,new String[]{primaryKeyName},selectionPair.first,selectionPair.second,null,null,orderColumnAndReverseToOrderBy(queryArgument.getOrderColumn(),queryArgument.isReverse()),pageAndSizeToLimit(queryArgument.getPage(),queryArgument.getPageSize())));
    }

    public List<Map<String,?>> query(){
        return query(new QueryArgument());
    }

    public List<Map<String,?>> query(QueryArgument queryArgument){
        Pair<String,String[]> selectionPair = conditionToSelection(queryArgument.getConditions(),queryArgument.getConditionLink());
        return queryDataListByCursor(innerHelper.getReadableDatabase().query(tableName,new String[]{"*"},selectionPair.first,selectionPair.second,null,null,orderColumnAndReverseToOrderBy(queryArgument.getOrderColumn(),queryArgument.isReverse()),pageAndSizeToLimit(queryArgument.getPage(),queryArgument.getPageSize())));
    }

    private String pageAndSizeToLimit(Integer page,Integer pageSize){
        if (pageSize == null && page == null){
            return null;
        }
        if (page == null){
            return String.valueOf(pageSize);
        }
        return String.format("%s, %s",page*pageSize,pageSize);
    }

    private Pair<String,String[]> conditionToSelection(Condition[] conditions,ConditionLink conditionLink){
        if (conditions == null){
            return new Pair<>(null,null);
        }
        String[] selectionArgs = new String[conditions.length];
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i< conditions.length; i++){
            Condition condition = conditions[i];
            //
            selectionArgs[i] = condition.getValue() == null?"":condition.getValue().toString();
            //
            stringBuilder.append(condition.getKey());
            stringBuilder.append(" ");
            stringBuilder.append(getCompareTypeCharacter(condition.getCompare()));
            stringBuilder.append(" ");
            stringBuilder.append("?");
            if (i < conditions.length-1){
                stringBuilder.append(" ");
                stringBuilder.append(getConditionLinkCharacter(conditionLink));
                stringBuilder.append(" ");
            }
        }
        return new Pair<>(stringBuilder.toString(),selectionArgs);
    }

    private final Map<Condition.CompareType,String> compareTypeStringMap = new HashMap<>();{
        compareTypeStringMap.put(Condition.CompareType.LessThan,"<");
        compareTypeStringMap.put(Condition.CompareType.LessThanOrEqualTo,"<=");
        compareTypeStringMap.put(Condition.CompareType.EqualTo,"=");
        compareTypeStringMap.put(Condition.CompareType.GreaterThanOrEqualTo,">=");
        compareTypeStringMap.put(Condition.CompareType.GreaterThan,">");
        compareTypeStringMap.put(Condition.CompareType.NotEqualTo,"!=");
    }
    private String getCompareTypeCharacter(Condition.CompareType compareType){
        return compareTypeStringMap.get(compareType);
    }

    private final Map<ConditionLink,String> conditionLinkStringMap = new HashMap<>();{
        conditionLinkStringMap.put(ConditionLink.And,"and");
        conditionLinkStringMap.put(ConditionLink.Or,"or");
    }
    private String getConditionLinkCharacter(ConditionLink conditionLink){
        return conditionLinkStringMap.get(conditionLink);
    }

    private String orderColumnAndReverseToOrderBy(String orderColumn,Boolean reverse){
        if (orderColumn == null && reverse == null){
            return null;
        }
        if (reverse == null){
            return orderColumn;
        }
        return String.format("%s %s",orderColumn,reverse?"desc":"asc");
    }

    public boolean contain(Object id){
        Cursor cursor = null;
        try {
            cursor = innerHelper.getReadableDatabase().query(tableName,new String[]{"*"},String.format("%s = ?",primaryKeyName),objArrayToStringArray(new Object[]{id}),null,null,null,null);
            return cursor.getCount() == 1;
        } finally {
            if (cursor != null){
                cursor.close();
            }
        }
    }

    public Map<String,?> queryById(Object id){
        try {
            return queryDataListByCursor(innerHelper.getReadableDatabase().query(tableName,new String[]{"*"},String.format("%s = ?",primaryKeyName),objArrayToStringArray(new Object[]{id}),null,null,null,null)).get(0);
        } catch (IndexOutOfBoundsException e){
            return null;
        }
    }

    public List<Map<String,?>> queryByIdList(List<?> ids){
        //
        SQLiteDatabase db = innerHelper.getWritableDatabase();
        //
        try {
            List<Map<String,?>> list = new ArrayList<>(ids.size());
            db.beginTransaction();
            for (Object id : ids) {
                list.addAll(queryDataListByCursor(db.query(tableName,new String[]{"*"},String.format("%s = ?",primaryKeyName),objArrayToStringArray(new Object[]{id}),null,null,null,null)));
            }
            db.setTransactionSuccessful();
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            db.endTransaction();
        }
    }

    private String[] objArrayToStringArray(Object[] objArray){
        String[] stringArray = new String[objArray.length];
        for (int i=0;i<stringArray.length;i++){
            stringArray[i] = objArray[i] == null? null : objArray[i].toString();
        }
        return stringArray;
    }

    private void mapToContentValues(Map<String,?> map,ContentValues contentValues){
        for (Map.Entry<String,?> entry : map.entrySet()){
            putToContentValue(entry.getKey(),entry.getValue(),contentValues);
        }
    }

    private boolean putToContentValue(String key, Object value,ContentValues contentValues){
        if (value == null){
            contentValues.putNull(key);
        } else {
            if (allKeyMap.containsKey(key)){
                MimeType mimeType = getMimeType(allKeyMap.get(key));
                switch (mimeType){
                    case Boolean:
                    case Char:
                    case String:
                        contentValues.put(key,value.toString());
                        break;
                    case Byte:
                        contentValues.put(key,((Number) value).byteValue());
                        break;
                    case Short:
                        contentValues.put(key,((Number) value).shortValue());
                        break;
                    case Int:
                        contentValues.put(key,((Number) value).intValue());
                        break;
                    case Long:
                        contentValues.put(key,((Number) value).longValue());
                        break;
                    case Float:
                        contentValues.put(key,((Number) value).floatValue());
                        break;
                    case Double:
                        contentValues.put(key,((Number) value).doubleValue());
                        break;
                    case Blob:
                        contentValues.put(key,(byte[])value);
                        break;
                    default:
                        return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    private List<?> queryIdListByCursor(Cursor cursor){
        try {
            List<Object> list = new ArrayList<>(cursor.getCount());
            while (cursor.moveToNext()){
                list.add(getValueFromCursor(cursor,primaryKeyName,cursor.getColumnIndex(primaryKeyName)).second);
            }
            return list;
        } finally {
            cursor.close();
        }
    }

    private List<Map<String,?>>queryDataListByCursor(Cursor cursor){
        try {
            List<Map<String,?>> list = new ArrayList<>(cursor.getCount());
            while (cursor.moveToNext()){
                Map<String,Object> map = new LinkedHashMap<>(cursor.getColumnCount());
                list.add(map);
                for (int i=0;i<cursor.getColumnCount();i++){
                    String name = cursor.getColumnName(i);
                    Pair<Boolean,Object> pair = getValueFromCursor(cursor,name,i);
                    if (pair.first){
                        map.put(name,pair.second);
                    }
                }
            }
            return list;
        } finally {
            cursor.close();
        }
    }

    private Pair<Boolean,Object> getValueFromCursor(Cursor cursor,String name,int columnIndex){
        if(cursor.getType(columnIndex) == FIELD_TYPE_NULL){
            return new Pair<>(true,null);
        } else {
            if (allKeyMap.containsKey(name)){
                switch (getMimeType(allKeyMap.get(name))){
                    case Byte: return new Pair<Boolean,Object>(true,cursor.getInt(columnIndex));
                    case Short: return new Pair<Boolean,Object>(true,cursor.getShort(columnIndex));
                    case Int: return new Pair<Boolean,Object>(true,cursor.getInt(columnIndex));
                    case Long: return new Pair<Boolean,Object>(true,cursor.getLong(columnIndex));
                    case Float: return new Pair<Boolean,Object>(true,cursor.getFloat(columnIndex));
                    case Double: return new Pair<Boolean,Object>(true,cursor.getDouble(columnIndex));
                    case Boolean: return new Pair<Boolean,Object>(true,Boolean.valueOf(cursor.getString(columnIndex)));
                    case Char: return new Pair<Boolean,Object>(true,cursor.getString(columnIndex).toCharArray()[0]);
                    case String: return new Pair<Boolean,Object>(true,cursor.getString(columnIndex));
                    case Blob: return new Pair<Boolean,Object>(true,cursor.getBlob(columnIndex));
                }
            }
        }
        return new Pair<>(false,null);
    }

    private final Map<Class<?>,MimeType> mimeTypeMap = new HashMap<>();{
        mimeTypeMap.put(byte.class, MimeType.Byte);
        mimeTypeMap.put(Byte.class, MimeType.Byte);
        mimeTypeMap.put(short.class, MimeType.Short);
        mimeTypeMap.put(Short.class, MimeType.Short);
        mimeTypeMap.put(int.class, MimeType.Int);
        mimeTypeMap.put(Integer.class, MimeType.Int);
        mimeTypeMap.put(long.class, MimeType.Long);
        mimeTypeMap.put(Long.class, MimeType.Long);
        mimeTypeMap.put(float.class, MimeType.Float);
        mimeTypeMap.put(Float.class, MimeType.Float);
        mimeTypeMap.put(double.class, MimeType.Double);
        mimeTypeMap.put(Double.class, MimeType.Double);
        mimeTypeMap.put(boolean.class, MimeType.Boolean);
        mimeTypeMap.put(Boolean.class, MimeType.Boolean);
        mimeTypeMap.put(char.class, MimeType.Char);
        mimeTypeMap.put(Character.class, MimeType.Char);
        mimeTypeMap.put(String.class, MimeType.String);
        mimeTypeMap.put(byte[].class, MimeType.Blob);
    }
    private MimeType getMimeType(Class<?> cla){
        return mimeTypeMap.get(cla);
    }

    private enum MimeType{
        Byte,
        Short,
        Int,
        Long,
        Float,
        Double,
        Boolean,
        Char,
        String,
        Blob,
    }

    private class InnerHelper extends SQLiteOpenHelper {

        private final Context context;

        private final String KEY_ALL_COLUMN = "AllColumn";

        private final boolean autoincrement;
        private final Pair<String,Class<?>> primaryKeyPair;
        private final Map<String,Class<?>> otherKeyMap;

        public InnerHelper(Context context,int version,boolean autoincrement,Pair<String,Class<?>> primaryKeyPair,Map<String,Class<?>> otherKeyMap) {
            super(context,dbName+".db",null,version);
            this.context = context;
            this.autoincrement = autoincrement;
            this.primaryKeyPair = primaryKeyPair;
            this.otherKeyMap = otherKeyMap;
        }

        @Override
        public void onCreate(SQLiteDatabase db) {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(primaryKeyPair.first);
            stringBuilder.append(" ");
            stringBuilder.append(getClassTypeInSQL(primaryKeyPair.second));
            stringBuilder.append(" ");
            stringBuilder.append("primary key");
            if (autoincrement){
                stringBuilder.append(" ");
                stringBuilder.append("autoincrement");
            }
            stringBuilder.append(",");
            for (Map.Entry<String,Class<?>> entry : otherKeyMap.entrySet()){
                stringBuilder.append(entry.getKey());
                stringBuilder.append(" ");
                stringBuilder.append(getClassTypeInSQL(entry.getValue()));
                stringBuilder.append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length()-1);

            db.execSQL(String.format("create table if not exists %s (%s);", tableName,stringBuilder));

            context.getSharedPreferences(getSPName(),Context.MODE_PRIVATE).edit().putStringSet(KEY_ALL_COLUMN,otherKeyMap.keySet()).apply();
        }

        private final Map<Class<?>,String> classTypeMap = new HashMap<>();{
            final String default_format = "%s default %s";
            final String integerType = "integer";
            final String decimalType = "decimal";
            final String booleanType = "char(5)";
            final String charType = "char(1)";
            final String textType = "text";
            final String blobType = "blob";
            classTypeMap.put(byte.class,String.format(default_format,integerType,0));
            classTypeMap.put(Byte.class,integerType);
            classTypeMap.put(short.class,String.format(default_format,integerType,0));
            classTypeMap.put(Short.class,integerType);
            classTypeMap.put(int.class,String.format(default_format,integerType,0));
            classTypeMap.put(Integer.class,integerType);
            classTypeMap.put(long.class,String.format(default_format,integerType,0));
            classTypeMap.put(Long.class,integerType);
            classTypeMap.put(float.class,String.format(default_format,decimalType,0));
            classTypeMap.put(Float.class,decimalType);
            classTypeMap.put(double.class,String.format(default_format,decimalType,0));
            classTypeMap.put(Double.class,decimalType);
            classTypeMap.put(boolean.class,String.format(default_format,booleanType,Boolean.valueOf(false).toString()));
            classTypeMap.put(Boolean.class,booleanType);
            classTypeMap.put(char.class,charType);
            classTypeMap.put(Character.class,charType);
            classTypeMap.put(String.class,textType);
            classTypeMap.put(byte[].class,blobType);
        }
        private String getClassTypeInSQL(Class<?> cla){
            return classTypeMap.get(cla);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            if (newVersion > oldVersion){
                Set<String> oldList = context.getSharedPreferences(getSPName(),Context.MODE_PRIVATE).getStringSet(KEY_ALL_COLUMN,new LinkedHashSet<String>());
                Set<String> newList = otherKeyMap.keySet();
                //新增的列
                Set<String> addColumnList = new LinkedHashSet<>(newList);
                addColumnList.removeAll(oldList);
                for (String key:addColumnList){
                    db.execSQL(String.format("alter table %s add column %s %s",tableName,key,getClassTypeInSQL(otherKeyMap.get(key))));
                }
                //由于Sqlite不支持删除列，以下代码暂时未实现
//                //移除的列
//                Set<String> dropColumnList = new LinkedHashSet<>(oldList);
//                dropColumnList.removeAll(newList);
//                for (String key:dropColumnList){
//                    db.execSQL(String.format("alter table %s drop column %s",tableName,key));
//                }
                //写入最新列信息
                context.getSharedPreferences(getSPName(),Context.MODE_PRIVATE).edit().putStringSet(KEY_ALL_COLUMN,otherKeyMap.keySet()).apply();
            }
        }

        private String getSPName(){
            return dbName + "."  + tableName;
        }

    }

}
