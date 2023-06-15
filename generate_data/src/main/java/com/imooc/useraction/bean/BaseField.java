package com.imooc.useraction.bean;

/**
 * 基础公共字段
 * Created by xuwei
 */
public class BaseField {
    /**
     * 用户ID
     */
    private String uid;
    /**
     * 手机设备ID
     */
    private String xaid;
    /**
     *设备类型
     * 1:Android-APP, 2:IOS-APP, 3:PC
     */
    private int platform;
    /**
     * 大版本号
     */
    private String ver;
    /**
     * 子版本号
     */
    private String vercode;
    /**
     * 网络类型
     * 0:未知 , 1:WIFI, 2:2G , 3:3G, 4:4G, 5:5G
     */
    private int net;
    /**
     * 手机品牌
     */
    private String brand;
    /**
     * 机型
     */
    private String model;
    /**
     * 分辨率
     */
    private String display;
    /**
     * 操作系统版本号
     */
    private String osver;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getXaid() {
        return xaid;
    }

    public void setXaid(String xaid) {
        this.xaid = xaid;
    }

    public int getPlatform() {
        return platform;
    }

    public void setPlatform(int platform) {
        this.platform = platform;
    }

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public String getVercode() {
        return vercode;
    }

    public void setVercode(String vercode) {
        this.vercode = vercode;
    }

    public int getNet() {
        return net;
    }

    public void setNet(int net) {
        this.net = net;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getDisplay() {
        return display;
    }

    public void setDisplay(String display) {
        this.display = display;
    }

    public String getOsver() {
        return osver;
    }

    public void setOsver(String osver) {
        this.osver = osver;
    }
}
