package com.example.demo;

import javax.persistence.*;

/**
 * Created by shuangmm on 2018/1/24
 */
@Entity
public class ExceptionOutput {
    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)

    private int id;
    private String fan_no;
    private String call_time;
    private int call_count;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setFan_no(String fan_no) {
        this.fan_no = fan_no;
    }

    @Override
    public String toString() {
        return "ExceptionOutput{" +
                "id=" + id +
                ", fan_no='" + fan_no + '\'' +
                ", call_time='" + call_time + '\'' +
                ", call_count=" + call_count +
                '}';
    }

    public void setCall_time(String call_time) {
        this.call_time = call_time;
    }

    public void setCall_count(int call_count) {
        this.call_count = call_count;
    }

    public String getFan_no() {
        return fan_no;
    }

    public int getCall_count() {
        return call_count;
    }



    public String getCall_time() {
        return call_time;
    }
}
