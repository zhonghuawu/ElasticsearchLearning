package com.huaa.Utils;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Date;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/9 16:59
 */

public class QueryHelper {

    private QueryHelper() {}

    public static QueryBuilder matchAllQuery() {
        return QueryBuilders.matchAllQuery();
    }

    public static QueryBuilder timeRangeQuery(Long from, Long to) {
        return QueryBuilders.rangeQuery("timestamp").from(DateUtil.format(from)).to(DateUtil.format(to));
    }

    public static QueryBuilder timeRangeQuery(Date from, Date to) {
        return QueryBuilders.rangeQuery("timestamp").from(DateUtil.format(from)).to(DateUtil.format(to));
    }

}
