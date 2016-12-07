package com.tscloud.presto.es;

import io.airlift.configuration.Config;

/**
 * Created by Administrator on 2016/11/15.
 */
public class ElasticsearchConfig {

    /**
     * 客户端设置
     */
    public static final String CLIENT_TRANSPORT_SNIFF = "client.transport.sniff";
    public static final String CLUSTER_NAME = "cluster.name";
    public static final String CLIENT_TRANSPORT_IGNORE_CLUSTRER_NAME = "client.transport.ignore_cluster_name";
    public static final String CLIENT_TRANSPORT_PING_TIMEOUT = "client.transport.ping_timeout";
    public static final String CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL = "client.transport.nodes_sampler_interval";
    public static final String CLIENT_HOSTS="client.hosts";
    /**
     * 服务端创建index时参数
     */
    public static final String SETTING_NUMBER_OF_SHARDS = "number_of_shards";
    public static final String SETTING_NUMBER_OF_RESPLICAS = "number_of_replicas";

    /**
     * 集群名称
     */
    private String clusterName;

    /**
     * 你可以设置client.transport.sniff为true来使客户端去嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端中，
     * 这样做的好处是一般你不用手动设置集群里所有集群的ip到连接客户端，它会自动帮你添加，并且自动发现新加入集群的机器。
     */
    private Boolean transportSniff = true;

    /**
     * 是否忽略集群名称
     */
    private Boolean ignoreClusterName = false;

    /**
     * ping的超时时间 默认5秒
     */
    private Integer pingTimeOut;

    /**
     * ping的间隔 默认5秒
     */
    private Integer interval;

    /**
     * 主机
     */
    private String hosts;

    /**
     * 分片数
     */
    private int shardsNum=3;

    /**
     * 副本数
     */
    private int replicasNum=3;

    @Config(CLUSTER_NAME)
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Config(CLIENT_TRANSPORT_SNIFF)
    public void setTransportSniff(boolean transportSniff) {
        this.transportSniff = transportSniff;
    }

    @Config(CLIENT_TRANSPORT_IGNORE_CLUSTRER_NAME)
    public void setIgnoreClusterName(boolean ignoreClusterName) {
        this.ignoreClusterName = ignoreClusterName;
    }

    @Config(CLIENT_TRANSPORT_PING_TIMEOUT)
    public void setPingTimeOut(int pingTimeOut) {
        this.pingTimeOut = pingTimeOut;
    }

    @Config(CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL)
    public void setInterval(int interval) {
        this.interval = interval;
    }

    @Config(CLIENT_HOSTS)
    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    @Config(SETTING_NUMBER_OF_SHARDS)
    public void setShardsNum(int shardsNum) {
        this.shardsNum = shardsNum;
    }

    @Config(SETTING_NUMBER_OF_RESPLICAS)
    public void setReplicasNum(int replicasNum) {
        this.replicasNum = replicasNum;
    }

    public String getClusterName() {
        return clusterName;
    }

    public Boolean getTransportSniff() {
        return transportSniff;
    }

    public Boolean getIgnoreClusterName() {
        return ignoreClusterName;
    }

    public Integer getPingTimeOut() {
        return pingTimeOut;
    }

    public Integer getInterval() {
        return interval;
    }

    public String getHosts() {
        return hosts;
    }

    public int getShardsNum() {
        return shardsNum;
    }

    public int getReplicasNum() {
        return replicasNum;
    }
}
