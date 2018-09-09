package com.huaa.test.data;

import java.io.Serializable;

/**
 * Desc:
 *
 * @author Huaa
 * @date 2018/9/9 11:53
 */

abstract public class DataLog implements Serializable {

    private static final long serialVersionUID = -9223096685035476193L;

    public abstract String indexPostfixName();

}
