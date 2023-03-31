package com.xq.litemapping;

import java.io.Serializable;

public class Condition implements Serializable {

    private String key;
    private CompareType compare;
    private Object value;

    public Condition(String key, CompareType compare, Object value) {
        this.key = key;
        this.compare = compare;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public CompareType getCompare() {
        return compare;
    }

    public void setCompare(CompareType compare) {
        this.compare = compare;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public enum CompareType{
        LessThan,
        LessThanOrEqualTo,
        EqualTo,
        GreaterThanOrEqualTo,
        GreaterThan,
        NotEqualTo,
    }

}
