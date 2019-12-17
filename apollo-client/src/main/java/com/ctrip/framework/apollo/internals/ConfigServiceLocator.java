package com.ctrip.framework.apollo.internals;

import com.ctrip.framework.apollo.core.ServiceNameConsts;
import com.ctrip.framework.foundation.Foundation;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.exceptions.ApolloConfigException;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.ctrip.framework.apollo.util.http.HttpRequest;
import com.ctrip.framework.apollo.util.http.HttpResponse;
import com.ctrip.framework.apollo.util.http.HttpUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import com.google.gson.reflect.TypeToken;

public class ConfigServiceLocator {
  private static final Logger logger = LoggerFactory.getLogger(ConfigServiceLocator.class);
  private HttpUtil m_httpUtil;
  private ConfigUtil m_configUtil;
  /**
   * ServiceDTO 数组的缓存
   */
  private AtomicReference<List<ServiceDTO>> m_configServices;
  private Type m_responseType;
  /**
   * 定时任务 ExecutorService
   */
  private ScheduledExecutorService m_executorService;
  private static final Joiner.MapJoiner MAP_JOINER = Joiner.on("&").withKeyValueSeparator("=");
  private static final Escaper queryParamEscaper = UrlEscapers.urlFormParameterEscaper();

  /**
   * Create a config service locator.
   */
  public ConfigServiceLocator() {
    List<ServiceDTO> initial = Lists.newArrayList();
    m_configServices = new AtomicReference<>(initial);
    m_responseType = new TypeToken<List<ServiceDTO>>() {
    }.getType();
    m_httpUtil = ApolloInjector.getInstance(HttpUtil.class);
    m_configUtil = ApolloInjector.getInstance(ConfigUtil.class);
    this.m_executorService = Executors.newScheduledThreadPool(1,
        ApolloThreadFactory.create("ConfigServiceLocator", true));
    // 初始拉取 Config Service 地址
    initConfigServices();
  }

  /***
   * 1、根据系统或者环境变量配置获得 config service 地址列表。如果有的话直接返回
   * 2、如果第1步未获得config service列表，则根据meta service从注册中心上拉取更新 config service 地址列表
   * 3、开启一个定时任务，定时调用 tryUpdateConfigServices 刷新 config service 地址列表
   */
  private void initConfigServices() {
    // 从环境变量里获取相应的config service配置
    //系统属性获取apollo.configService > 操作系统变量里的APOLLO_CONFIGSERVICE >从/opt/settings/server.properties配置里获取 apollo.configService
    List<ServiceDTO> customizedConfigServices = getCustomizedConfigService();
    //如果配置文件里配置config service，则直接返回
    if (customizedConfigServices != null) {
      setConfigServices(customizedConfigServices);
      return;
    }

    // 从meta Service上获取config service
    this.tryUpdateConfigServices();
    //开启定时任务，定时拉取config servcie路径，定时时间5分钟
    this.schedulePeriodicRefresh();
  }

  /***
   * 获得config service集群列表
   * @return
   * 1、从系统属性获取apollo.configService
   * 2、从操作系统变量里获取 APOLLO_CONFIGSERVICE
   * 3、从/opt/settings/server.properties配置里获取 apollo.configService
   */
  private List<ServiceDTO> getCustomizedConfigService() {
    // 1. Get from System Property
    String configServices = System.getProperty("apollo.configService");
    if (Strings.isNullOrEmpty(configServices)) {
      // 2. Get from OS environment variable
      configServices = System.getenv("APOLLO_CONFIGSERVICE");
    }
    if (Strings.isNullOrEmpty(configServices)) {
      // 3. Get from server.properties
      configServices = Foundation.server().getProperty("apollo.configService", null);
    }

    if (Strings.isNullOrEmpty(configServices)) {
      return null;
    }

    logger.warn("Located config services from apollo.configService configuration: {}, will not refresh config services from remote meta service!", configServices);

    // mock service dto list
    String[] configServiceUrls = configServices.split(",");
    List<ServiceDTO> serviceDTOS = Lists.newArrayList();

    for (String configServiceUrl : configServiceUrls) {
      configServiceUrl = configServiceUrl.trim();
      ServiceDTO serviceDTO = new ServiceDTO();
      serviceDTO.setHomepageUrl(configServiceUrl);
      serviceDTO.setAppName(ServiceNameConsts.APOLLO_CONFIGSERVICE);
      serviceDTO.setInstanceId(configServiceUrl);
      serviceDTOS.add(serviceDTO);
    }

    return serviceDTOS;
  }

  /**
   * Get the config service info from remote meta server.
   *
   * @return the services dto
   * 1、如果如果本地内存m_configServices为空，则根据meta service更新 config services列表(最多重试次数)，并返回最新的config service 列表
   * 2、如果如果本地内存m_configServices不为空，直接返回
   */
  public List<ServiceDTO> getConfigServices() {
    if (m_configServices.get().isEmpty()) {
      updateConfigServices();
    }

    return m_configServices.get();
  }

  /***
   * 尝试更新 config service 地址列表，从meta service上拉取config service 地址列表
   * @return
   */
  private boolean tryUpdateConfigServices() {
    try {
      updateConfigServices();
      return true;
    } catch (Throwable ex) {
      //ignore
    }
    return false;
  }

  /***
   * 开启定时任务，定时拉取config servcie路径，定时时间5分钟
   */
  private void schedulePeriodicRefresh() {
    this.m_executorService.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            logger.debug("refresh config services");
            Tracer.logEvent("Apollo.MetaService", "periodicRefresh");
            tryUpdateConfigServices();
          }
        }, m_configUtil.getRefreshInterval(), m_configUtil.getRefreshInterval(),
        m_configUtil.getRefreshIntervalTimeUnit());
  }

  /***
   * 根据meta service更新 config services列表(最多重试次数)
   * 1、获得meta service地址url
   * 2、从 meta service地址上获取 config service 地址列表
   */
  private synchronized void updateConfigServices() {
    String url = assembleMetaServiceUrl();

    HttpRequest request = new HttpRequest(url);
    int maxRetries = 2;
    Throwable exception = null;

    for (int i = 0; i < maxRetries; i++) {
      Transaction transaction = Tracer.newTransaction("Apollo.MetaService", "getConfigService");
      transaction.addData("Url", url);
      try {
        HttpResponse<List<ServiceDTO>> response = m_httpUtil.doGet(request, m_responseType);
        transaction.setStatus(Transaction.SUCCESS);
        List<ServiceDTO> services = response.getBody();
        if (services == null || services.isEmpty()) {
          logConfigService("Empty response!");
          continue;
        }
        setConfigServices(services);
        return;
      } catch (Throwable ex) {
        Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(ex));
        transaction.setStatus(ex);
        exception = ex;
      } finally {
        transaction.complete();
      }

      try {
        m_configUtil.getOnErrorRetryIntervalTimeUnit().sleep(m_configUtil.getOnErrorRetryInterval());
      } catch (InterruptedException ex) {
        //ignore
      }
    }

    throw new ApolloConfigException(
        String.format("Get config services failed from %s", url), exception);
  }

  /***
   * 从meat service上获得config service后，要设置到m_configServices
   * @param services
   */
  private void setConfigServices(List<ServiceDTO> services) {
    m_configServices.set(services);
    //日志输出加载到 config service
    logConfigServices(services);
  }

  /***
   * 获得meta service的地址
   *
   * @return
   * 1、获得meta server地址：apollo.meta=http://localhost:8070
   * 2、获得appid ：app.id=1000001
   * 3、获得本地ip
   */
  private String assembleMetaServiceUrl() {
    String domainName = m_configUtil.getMetaServerDomainName();
    String appId = m_configUtil.getAppId();
    String localIp = m_configUtil.getLocalIp();

    Map<String, String> queryParams = Maps.newHashMap();
    queryParams.put("appId", queryParamEscaper.escape(appId));
    if (!Strings.isNullOrEmpty(localIp)) {
      queryParams.put("ip", queryParamEscaper.escape(localIp));
    }

    return domainName + "/services/config?" + MAP_JOINER.join(queryParams);
  }

  private void logConfigServices(List<ServiceDTO> serviceDtos) {
    for (ServiceDTO serviceDto : serviceDtos) {
      logConfigService(serviceDto.getHomepageUrl());
    }
  }

  private void logConfigService(String serviceUrl) {
    Tracer.logEvent("Apollo.Config.Services", serviceUrl);
  }
}
