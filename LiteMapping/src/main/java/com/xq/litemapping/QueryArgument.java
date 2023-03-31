package com.xq.litemapping;

import java.io.Serializable;

public class QueryArgument implements Serializable {

    private Condition[] conditions;
    private ConditionLink conditionLink;

    private Integer page;
    private Integer pageSize;

    private String orderColumn;
    private Boolean reverse;

    public QueryArgument setCondition(Condition condition){
        return setConditions(new Condition[]{condition},ConditionLink.And);
    }

    public QueryArgument setConditions(Condition[] conditions,ConditionLink conditionLink) {
        this.conditions = conditions;
        this.conditionLink = conditionLink;
        return this;
    }

    public QueryArgument setPageAndSize(Integer page,Integer pageSize) {
        this.page = page;
        this.pageSize = pageSize;
        return this;
    }

    public QueryArgument setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    public QueryArgument setOrderBy(String orderColumn) {
        return setOrderBy(orderColumn,false);
    }

    public QueryArgument setOrderBy(String orderColumn,Boolean reverse) {
        this.orderColumn = orderColumn;
        this.reverse = reverse;
        return this;
    }

    public Condition[] getConditions() {
        return conditions;
    }

    public ConditionLink getConditionLink() {
        return conditionLink;
    }

    public Integer getPage() {
        return page;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public String getOrderColumn() {
        return orderColumn;
    }

    public Boolean isReverse() {
        return reverse;
    }
}
