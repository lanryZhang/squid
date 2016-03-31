package com.ifeng.entities;

import com.ifeng.mongo.DataLoader;
import com.ifeng.mongo.EntyCodec;

import java.io.Serializable;

/**
 * Created by zhanglr on 2016/3/29.
 */
public class IpsEntity extends EntyCodec implements Serializable {
    private String hostIp="";
    /**
     * 请求类型
     * 0 点播 1直播,2 手机直播
     */
    private String requestType = "";
    private String nodeIp = "";
    private String clientType = "";
    private String netName = "";
    private String province = "";
    private int requestNum = 0;
    private String cdnId = "";
    private String channelid = "";
    private String createDate = "";
    private String hm = "";
    private String dateTime = "";

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public String getCdnId() {
        return cdnId;
    }

    public void setCdnId(String cdnId) {
        this.cdnId = cdnId;
    }

    public String getChannelid() {
        return channelid;
    }

    public void setChannelid(String channelid) {
        this.channelid = channelid;
    }

    public String getClientType() {
        return clientType;
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }

    public String getCreateDate() {
        return createDate;
    }

    public void setCreateDate(String createDate) {
        this.createDate = createDate;
    }

    public String getHm() {
        return hm;
    }

    public void setHm(String hm) {
        this.hm = hm;
    }

    public String getHostIp() {
        return hostIp;
    }

    public void setHostIp(String hostIp) {
        this.hostIp = hostIp;
    }

    public String getNetName() {
        return netName;
    }

    public void setNetName(String netName) {
        this.netName = netName;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public void setNodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public int getRequestNum() {
        return requestNum;
    }

    public void setRequestNum(int requestNum) {
        this.requestNum = requestNum;
    }

    public String getRequestType() {
        return requestType;
    }

    public void setRequestType(String requestType) {
        this.requestType = requestType;
    }

    @Override
    public String toString(){
        return new StringBuilder().append(this.hostIp)
                .append(this.clientType).append(this.createDate)
                .append(this.hm).append(this.getNodeIp()).append(this.getRequestType()).toString();
    }
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IpsEntity)) {
            return false;
        }
        IpsEntity en = (IpsEntity) obj;
        return this.hostIp.equals(en.hostIp)
                && this.requestType.equals(en.requestType)
                && this.nodeIp.equals(en.nodeIp)
                && this.clientType.equals(en.clientType)
                && this.netName.equals(en.netName)
                && this.province.equals(en.province)
                && this.cdnId.equals(en.cdnId)
                && this.channelid.equals(en.channelid)
                && this.createDate.equals(en.createDate)
                && this.hm.equals(en.hm);
    }

    @Override
    public int hashCode(){
       return requestType.hashCode() * 31+nodeIp.hashCode() * 31
               +clientType.hashCode() * 31 +netName.hashCode() * 31
               +province.hashCode() * 31
               +cdnId.hashCode() * 31+channelid.hashCode() * 31
               +hostIp.hashCode() *31;
    }

    @Override
    public void decode(DataLoader loader) {
        this.cdnId = loader.getString("");
        this.hostIp = loader.getString("");
        this.channelid = loader.getString("");
        this.clientType = loader.getString("");
        this.createDate = loader.getString("");
    }
}
