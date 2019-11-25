package com.ctrip.framework.apollo.biz.grayReleaseRule;

import com.ctrip.framework.apollo.common.dto.GrayReleaseRuleItemDTO;

import java.util.Set;

/**
 * @author Jason Song(song_s@ctrip.com)
 *
 * 灰度发布规则的缓存对象
 */
public class GrayReleaseRuleCache {
  /***
   * 规则id
   */
  private long ruleId;
  /***
   * 灰度分支名称
   */
  private String branchName;
  /***
   * 命名空间
   */
  private String namespaceName;
  /***
   * 灰度id
   */
  private long releaseId;
  /***
   * 版本
   */
  private long loadVersion;
  /**
   * 分支状态
   */
  private int branchStatus;
  /***
   * 灰度发布的配置条目
   */

  private Set<GrayReleaseRuleItemDTO> ruleItems;

  public GrayReleaseRuleCache(long ruleId, String branchName, String namespaceName, long
      releaseId, int branchStatus, long loadVersion, Set<GrayReleaseRuleItemDTO> ruleItems) {
    this.ruleId = ruleId;
    this.branchName = branchName;
    this.namespaceName = namespaceName;
    this.releaseId = releaseId;
    this.branchStatus = branchStatus;
    this.loadVersion = loadVersion;
    this.ruleItems = ruleItems;
  }

  public long getRuleId() {
    return ruleId;
  }

  public Set<GrayReleaseRuleItemDTO> getRuleItems() {
    return ruleItems;
  }

  public String getBranchName() {
    return branchName;
  }

  public int getBranchStatus() {
    return branchStatus;
  }

  public long getReleaseId() {
    return releaseId;
  }

  public long getLoadVersion() {
    return loadVersion;
  }

  public void setLoadVersion(long loadVersion) {
    this.loadVersion = loadVersion;
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public boolean matches(String clientAppId, String clientIp) {
    for (GrayReleaseRuleItemDTO ruleItem : ruleItems) {
      //如果规则对应的appId和clientIp都匹配，则返回true
      if (ruleItem.matches(clientAppId, clientIp)) {
        return true;
      }
    }
    return false;
  }
}
