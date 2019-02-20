package snc.boot.es;

import java.util.Collection;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
public class ESSimpleExpression {
    private String fieldName;       //属性名
    private Object value;           //对应值
    private Collection<Object> values;           //对应值
    private ESCriterion.Operator operator;      //计算符
    private Object from;
    private Object to;
    private List<String> fields = null;

    protected  ESSimpleExpression(String fieldName, Object value, ESCriterion.Operator operator) {
        this.fieldName = fieldName;
        this.value = value;
        this.operator = operator;
    }

    protected  ESSimpleExpression(String value, ESCriterion.Operator operator) {
        this.value = value;
        this.operator = operator;
    }

    protected ESSimpleExpression(String fieldName, Collection<Object> values) {
        this.fieldName = fieldName;
        this.values = values;
        this.operator = ESCriterion.Operator.TERMS;
    }

    protected ESSimpleExpression(Object value, List<String> fileds) {
        this.value = value;
        this.fields = fileds;
        this.operator = ESCriterion.Operator.MULTI;
    }

    protected ESSimpleExpression(String fieldName, Object from, Object to) {
        this.fieldName = fieldName;
        this.from = from;
        this.to = to;
        this.operator = ESCriterion.Operator.RANGE;
    }

    public Object toBuilder() {
        QueryBuilder qb = null;
        switch (operator) {
            case TERM:
                System.out.println(fieldName+"   "+value);
                qb = QueryBuilders.termQuery(fieldName, value);
                break;
            case TERMS:
                qb = QueryBuilders.termsQuery(fieldName, values);
                break;
            case RANGE:
                qb = QueryBuilders.rangeQuery(fieldName).from(from).to(to).includeLower(true).includeUpper(true);
                break;
            case FUZZY:
                qb = QueryBuilders.fuzzyQuery(fieldName, value);
                break;
            case QUERY_STRING:
                qb = QueryBuilders.queryStringQuery(value.toString());
                break;
            case MULTI:
                qb = QueryBuilders.multiMatchQuery(value,fields.toArray(new String[fields.size()]));
                break;
        }
        return qb;
    }
}
