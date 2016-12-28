package com.test;

/**
 * Created by raghr010 on 12/25/16.
 */
public class User implements Comparable<User>{

    private String name;
    private Integer followers;


    public User(String name, int followers) {
        this.name = name;
        this.followers = followers;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFollowers() {
        return followers;
    }

    public void setFollowers(int followers) {
        this.followers = followers;
    }

    @Override
    public String toString() {
        return getName() + "," + String.valueOf(followers);
    }

    public int compareTo(User other) {
        return this.followers.compareTo(other.followers);
    }
}
