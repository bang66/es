package snc.boot.es;

import java.util.List;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public interface ESCriterion {
    //TERM 单个条件查询　　TERMS多个条件查询　RANGE范围
    public enum Operator{
        TERM, TERMS, RANGE, FUZZY, QUERY_STRING, MISSING, MULTI
    }

    public enum MatchMode {
        START, END, ANYWHERE
    }

    public enum Projection {
        MAX, MIN, AVG, LENGTH, SUM, COUNT
    }

    public List<QueryBuilder> listBuilders();
}
