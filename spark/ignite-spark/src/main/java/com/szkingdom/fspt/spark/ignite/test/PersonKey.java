package com.szkingdom.fspt.spark.ignite.test;

import java.io.Serializable;

/**
 * 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：
 * 模块描述：
 * 开发作者：tang.peng
 * 创建日期：2019/5/9
 * 模块版本：1.0.0.0
 * ----------------------------------------------------------------
 * 修改日期      版本       作者           备注
 * 2019/5/9   1.0.0.0    tang.peng     创建
 * ----------------------------------------------------------------
 */
public class PersonKey implements Serializable {
    private String id ;
    private String name;

    public PersonKey( String name,String id) {
        this.id = id;
        this.name = name;
    }

    public PersonKey() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "PersonKey{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
