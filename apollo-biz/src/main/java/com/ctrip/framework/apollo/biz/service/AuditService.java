package com.ctrip.framework.apollo.biz.service;

import com.ctrip.framework.apollo.biz.entity.Audit;
import com.ctrip.framework.apollo.biz.repository.AuditRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 操作 ApolloConfigDB 库下的audit：日志审计表
 */
@Service
public class AuditService {

  private final AuditRepository auditRepository;

  public AuditService(final AuditRepository auditRepository) {
    this.auditRepository = auditRepository;
  }

  /**
   * 根据操作人查询审计记录
   * @param owner
   * @return
   */
  List<Audit> findByOwner(String owner) {
    return auditRepository.findByOwner(owner);
  }

  /**
   * 查询一条 audit 表记录
   * @param owner 操作人
   * @param entity 表名
   * @param op 操作类型
   * @return
   */
  List<Audit> find(String owner, String entity, String op) {
    return auditRepository.findAudits(owner, entity, op);
  }

  /***
   * 插入一条 audit 审计记录
   * @param entityName 表名
   * @param entityId 记录ID(对应entityName表中的主键id)
   * @param op 操作类型
   * @param owner  操作人
   */
  @Transactional
  void audit(String entityName, Long entityId, Audit.OP op, String owner) {
    Audit audit = new Audit();
    audit.setEntityName(entityName);
    audit.setEntityId(entityId);
    audit.setOpName(op.name());
    audit.setDataChangeCreatedBy(owner);
    auditRepository.save(audit);
  }

  /**
   * 插入一条 audit表记录
   * @param audit
   */
  @Transactional
  void audit(Audit audit){
    auditRepository.save(audit);
  }
}
