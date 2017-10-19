package com.hzgc.dubbo.dynamicrepo;

import java.io.Serializable;

/**
 * 搜索过滤条件
 */
public class SearchFilter implements Serializable {
    // TODO: 2017-10-11 原始属性
   /* *//**
     * 过滤参数，对不同的人车属性进行拼接
     *//*
    private String param;
    *//**
     * true 筛选不符合条件的选项，false 筛选符合条件的选项
     *//*
    private boolean inverse = false;
    *//**
     * 与其他条件的拼接运算，默认是 OR 运算
     *//*
    private SearchOperation option = SearchOperation.OR;

    *//**
     * 搜索条件的拼接运算,默认为OR操作
     *//*
    public enum SearchOperation {
        AND,
        OR
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }

    public boolean isInverse() {
        return inverse;
    }

    public void setInverse(boolean inverse) {
        this.inverse = inverse;
    }

    public SearchOperation getOption() {
        return option;
    }

    public void setOption(SearchOperation option) {
        this.option = option;
    }

    @Override
    public String toString() {
        return "SearchFilter{" +
                "param='" + param + '\'' +
                ", inverse=" + inverse +
                ", option=" + option +
                '}';
    }
    */
    // TODO: 2017-10-11 修改后的属性
    /**
     * 过滤参数：比如头发颜色
     */
    private String param;
    /**
     * 参数类型：黄色、棕色、黑色、白色等
     */
    private String type;
    /**
     * 与其他条件的拼接运算，OR或者AND操作
     */
    private String logic;

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLogic() {
        return logic;
    }

    public void setLogic(String logic) {
        this.logic = logic;
    }

    @Override
    public String toString() {
        return "SearchFilter{" +
                "param='" + param + '\'' +
                ", type='" + type + '\'' +
                ", logic='" + logic + '\'' +
                '}';
    }
}
