package com.bdilab.flinketl.utils;

/**
 * 任务类型
 * @author hcy
 */
public enum TaskType {
    common, timing, trigger;

    public static TaskType getInstance(int ordinal) {
        if (ordinal < 0 || ordinal >= values().length) {
            throw new IndexOutOfBoundsException("Invalid ordinal");
        }
        return values()[ordinal];
    }
}
