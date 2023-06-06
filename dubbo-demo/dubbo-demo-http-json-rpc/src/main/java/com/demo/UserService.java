package com.demo;

/**
 * @author wang.yubin
 * @since 2023/6/6
 */
public interface UserService {

    User createUser(int userId, String name, int age);

    User getUser(int userId);

    String getUserName(int userId);

    int getUserId(String name);

    void deleteAll();

}
