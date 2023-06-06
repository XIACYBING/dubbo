package com.demo;

import lombok.Data;

import java.io.Serializable;

/**
 * @author wang.yubin
 * @since 2023/6/6
 */
@Data
public class User implements Serializable {
    private int userId;
    private String name;
    private int age;
}
