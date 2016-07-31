package com.pingnotes.support;

/**
 * Created by shaobo on 7/29/16.
 */
public abstract class HbaseBaseDo {
    public abstract String rowKey();

    public byte[] rowKeyBytes() {
        return rowKey() == null ? null : rowKey().getBytes();
    }

    public boolean hasRowKey() {
        return rowKey() != null && !rowKey().isEmpty();
    }
}
