package snc.boot.es;

import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class ESQueryBuilders implements ESCriterion{
    private List<QueryBuilder> list = new ArrayList<>();

    /**
     * 功能描述：MultiMatch 查询
     * @param value 值
     * @param fields 字段集合
     */
    public ESQueryBuilders multiMatch(Object value, List<String> fields){
        list.add((QueryBuilder) new ESSimpleExpression(value,fields).toBuilder());
        return this;
    }

    /**
     * 功能描述：Term 查询
     * @param field 字段名
     * @param value 值
     */
    public ESQueryBuilders term(String field, Object value) {
        list.add((QueryBuilder) new ESSimpleExpression (field, value, Operator.TERM).toBuilder());
        return this;
    }

    /**
     * 功能描述：Terms 查询
     * @param field 字段名
     * @param values 集合值
     */
    public ESQueryBuilders terms(String field, Collection<Object> values) {
        list.add((QueryBuilder) new ESSimpleExpression (field, values).toBuilder());
        return this;
    }

    /**
     * 功能描述：fuzzy 查询
     * @param field 字段名
     * @param value 值
     */
    public ESQueryBuilders fuzzy(String field, Object value) {
        list.add((QueryBuilder) new ESSimpleExpression (field, value, Operator.FUZZY).toBuilder());
        return this;
    }

    /**
     * 功能描述：Range 查询
     * @param from 起始值
     * @param to 末尾值
     */
    public ESQueryBuilders range(String field, Object from, Object to) {
        list.add((QueryBuilder) new ESSimpleExpression (field, from, to).toBuilder());
        return this;
    }

    /**
     * 功能描述：Range 查询
     * @param queryString 查询语句
     */
    public ESQueryBuilders queryString(String queryString) {
        list.add((QueryBuilder) new ESSimpleExpression (queryString, Operator.QUERY_STRING).toBuilder());
        return this;
    }

    public List<QueryBuilder> listBuilders() {
        return list;
    }
}
