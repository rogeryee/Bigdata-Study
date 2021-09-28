package com.yee.study.bigdata.zookeeper.example.lock;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Roger.Yi
 */
public class IdGenerator {

    /**
     * 自增序列
     */
    private static int SEQUENCE = 0;

    /**
     * 按照“年-月-日-小时-分钟-秒-自增长序列”的规则生编号
     * @return
     */
    public String genId() {
        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(now) + (SEQUENCE++);
    }
}
