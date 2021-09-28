package com.yee.study.bigdata.zookeeper.zkclient;

import java.io.Serializable;

/**
 * @author Roger.Yi
 */
public class User implements Serializable {
    private String name;

    public User() {}

    public User(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
