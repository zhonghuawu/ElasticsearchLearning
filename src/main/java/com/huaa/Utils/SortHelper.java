package com.huaa.Utils;

import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/9 17:26
 */

public class SortHelper {

    private SortHelper() {}

    public static SortBuilder descSortByTimestamp() {
        return SortBuilders.fieldSort("timestamp").order(SortOrder.DESC);
    }

    public static SortBuilder ascSortByTimestamp() {
        return SortBuilders.fieldSort("timestamp").order(SortOrder.ASC);
    }

}
