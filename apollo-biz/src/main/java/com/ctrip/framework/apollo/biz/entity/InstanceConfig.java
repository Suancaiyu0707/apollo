package com.ctrip.framework.apollo.biz.entity;

import com.google.common.base.MoreObjects;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;

/**
 * @author Jason Song(song_s@ctrip.com)
 * 记录 Instance 对 Namespace 的配置的获取情况
 * instanceId + configAppId + ConfigNamespaceName 组成唯一索引，因为一个 Instance 可以使用多个 Namespace 。
 */
@Entity
@Table(name = "InstanceConfig")
public class InstanceConfig {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "Id")
  private long id;
  /**
   * Instance 编号，指向 {@link Instance#id}
   */
  @Column(name = "InstanceId")
  private long instanceId;

  @Column(name = "ConfigAppId", nullable = false)
  private String configAppId;

  @Column(name = "ConfigClusterName", nullable = false)
  private String configClusterName;
  /**
   * Namespace 名字
   */
  @Column(name = "ConfigNamespaceName", nullable = false)
  private String configNamespaceName;
  /**
   * Release Key ，对应 Release.releaseKey 字段。
   */
  @Column(name = "ReleaseKey", nullable = false)
  private String releaseKey;

  @Column(name = "ReleaseDeliveryTime", nullable = false)
  private Date releaseDeliveryTime;

  @Column(name = "DataChange_CreatedTime", nullable = false)
  private Date dataChangeCreatedTime;

  @Column(name = "DataChange_LastTime")
  private Date dataChangeLastModifiedTime;

  @PrePersist
  protected void prePersist() {
    if (this.dataChangeCreatedTime == null) {
      dataChangeCreatedTime = new Date();
    }
    if (this.dataChangeLastModifiedTime == null) {
      dataChangeLastModifiedTime = dataChangeCreatedTime;
    }
  }

  @PreUpdate
  protected void preUpdate() {
    this.dataChangeLastModifiedTime = new Date();
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(long instanceId) {
    this.instanceId = instanceId;
  }

  public String getConfigAppId() {
    return configAppId;
  }

  public void setConfigAppId(String configAppId) {
    this.configAppId = configAppId;
  }

  public String getConfigNamespaceName() {
    return configNamespaceName;
  }

  public void setConfigNamespaceName(String configNamespaceName) {
    this.configNamespaceName = configNamespaceName;
  }

  public String getReleaseKey() {
    return releaseKey;
  }

  public void setReleaseKey(String releaseKey) {
    this.releaseKey = releaseKey;
  }

  public Date getDataChangeCreatedTime() {
    return dataChangeCreatedTime;
  }

  public void setDataChangeCreatedTime(Date dataChangeCreatedTime) {
    this.dataChangeCreatedTime = dataChangeCreatedTime;
  }

  public Date getDataChangeLastModifiedTime() {
    return dataChangeLastModifiedTime;
  }

  public void setDataChangeLastModifiedTime(Date dataChangeLastModifiedTime) {
    this.dataChangeLastModifiedTime = dataChangeLastModifiedTime;
  }

  public String getConfigClusterName() {
    return configClusterName;
  }

  public void setConfigClusterName(String configClusterName) {
    this.configClusterName = configClusterName;
  }

  public Date getReleaseDeliveryTime() {
    return releaseDeliveryTime;
  }

  public void setReleaseDeliveryTime(Date releaseDeliveryTime) {
    this.releaseDeliveryTime = releaseDeliveryTime;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("id", id)
        .add("configAppId", configAppId)
        .add("configClusterName", configClusterName)
        .add("configNamespaceName", configNamespaceName)
        .add("releaseKey", releaseKey)
        .add("dataChangeCreatedTime", dataChangeCreatedTime)
        .add("dataChangeLastModifiedTime", dataChangeLastModifiedTime)
        .toString();
  }
}
