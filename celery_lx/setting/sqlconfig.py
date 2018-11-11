# 查找订阅信息
def sql_wechat_sub():
    SQL_WECHAT_SUB="select id,open_id,billno from wechat_subscribe where if_finish='0'"
    return SQL_WECHAT_SUB


def sql_wechat_sub_only_billno():
    SQL_WECHAT_SUB_ONLY_BILLNO="select distinct billno from wechat_subscribe where if_finish='0'"
    return SQL_WECHAT_SUB_ONLY_BILLNO


# 查找wechat_message,wechat_subsribe中配对信息
def sql_wechat_message(node_id):
    SQL_WECHAT_MESSAGE = "select wechat_subscribe.open_id,wechat_message.billno,wechat_message.ctnno from wechat_subscribe, wechat_message where wechat_subscribe.id = wechat_message.sub_id(+) and wechat_message.node_id='%s'" % node_id
    return SQL_WECHAT_MESSAGE


# 节点sql语句
class sql_mes():
    def __init__(self,billno,ctnno):
        self.billno = billno
        self.ctnno = ctnno

    # 空箱出闸口
    def sql_kxczk(self):
        SQL_KXCZK = "SELECT IYC_OUTTM,OTA_TRK_TRKNO AS MES_CONTENT,IYC_CNTRNO AS CTNNO,BILLNO FROM PSPRD.ALL_IE_PLAN_VM WHERE BILLNO = '%s' AND IYC_OUTTM BETWEEN 	SYSDATE - interval '2880' minute AND SYSDATE"%self.billno
        return SQL_KXCZK

    # 出口重箱预录
    def sql_ckzxyl(self):
        SQL_CKZXYL = "SELECT WCG_BILLNO AS BILLNO,WCT_CNTRNO AS CTNNO,'' AS MES_CONTENT FROM PSPRD.ALL_WEB_YULU_BAK_VM WHERE WCG_BILLNO = '%s' AND to_Date(WCG_INSTM,'yyyy-mm-dd hh24:mi:ss') BETWEEN SYSDATE - interval '2880' minute AND SYSDATE"%self.billno
        return SQL_CKZXYL

    # 重箱落位
    def sql_zxlw_tos(self):
        SQL_ZXLW="select IYC_CNTRNO AS CTNNO,IYC_YLOCATION AS MES_CONTENT, CGD_BILLNO AS BILLNO from PSPRD.ALL_XIANGHUO_BAK_VW WHERE iyc_inymode='出口重箱进场' and iyc_type='在场箱' and CGD_BILLNO='%s' and IYC_INYTM BETWEEN SYSDATE - interval '2880' minute and SYSDATE"%self.billno
        return SQL_ZXLW

    def sql_zxlw_zl(self):
        SQL_ZXLW="SELECT * from zl_kk where xh='%s' and tdh='%s' "%(self.ctnno,self.billno)
        return SQL_ZXLW

    # 运抵
    def sql_yd(self):
        SQL_YD="select rq,xh as CTNNO,tdh as BILLNO,'' as MES_CONTENT from top_zl_yd_h WHERE  tdh='%s' and result_code='信息中心-入库成功'"%(self.billno)
        return SQL_YD

    # 海关放行
    def sql_hgfx_out(self):
        SQL_HGFX="select top 1 srrq,tdh as BILLNO from zl_haig where tdh='%s' "%(self.ctnno,self.billno)
        return SQL_HGFX

    # 船舶离港
    def sql_cblg(self):
        SQL_CBLG="select DISTINCT CGD_BILLNO AS BILLNO,VLS_VCHNNM ||','|| VOC_EXPVOYAGE as MES_CONTENT from PSPRD.ALL_XIANGHUO_BAK_VW where  CGD_BILLNO='%s' and IYC_OUTYMODE='装船出场' and IYC_TYPE='出场箱'"%(self.billno)
        return SQL_CBLG

    # 进口转关单已获取
    # 读取同一提单号下转关单的箱子数量，计划数量
    def sql_zgdhq(self):
        SQL_ZGDHQ="select distinct tdh as BILLNO,'' as MES_CONTENT from zl_jkdh where tdh='%s' and srrq BETWEEN dateadd(mi,-15,GETDATE()) AND GETDATE()"%(self.billno)
        return SQL_ZGDHQ

    # 卸船完工
    # 数数量，实际数量
    def sql_zgd_count(self):
        SQL_ZGD_COUNT="select COUNT(*）as NUM from zl_jkdh where  tdh='%s' and srrq BETWEEN 	dateadd(mi,-15,GETDATE()) AND GETDATE()"%self.billno
        return SQL_ZGD_COUNT

    def sql_xcwg(self):
        SQL_XCWG="select IYC_CNTRNO as CTNNO,CGD_BILLNO AS BILLNO from PSPRD.ALL_XIANGHUO_BAK_VW WHERE CGD_BILLNO='%s' and iyc_inymode='卸船进场' and iyc_type='在场箱' ORDER BY IYC_INYTM DESC"%self.billno
        return SQL_XCWG

    # 进口核销完成
    def sql_jkhx(self):
        SQL_JKHX="select jkrq ,tdh as BILLNO,'' AS MES_CONTENT from zl_jkdh where tdh='%s' and zt='海关回执' "%(self.billno)
        return SQL_JKHX

    # 海关放行
    def sql_hgfx_in(self):
        SQL_HGFX="select fxrq from zl_jkdh where tdh='%s' and xh='%s' and h_fx='ok'"%(self.billno,self.ctnno)
        return SQL_HGFX

    # 提重计划
    def sql_tzjh(self):
        SQL_TZJH="select DISTINCT PSPRD.ALL_PLAN_CONTAINERS_VM.PLANNO as MES_CONTENT , BILLNO from PSPRD.ALL_PLAN_CONTAINERS_VM WHERE billno='%s'  AND inymode='提进口重箱'"%(self.billno,self.ctnno)
        return  SQL_TZJH

    # 提箱出场
    def sql_txcc(self):
        SQL_TXCC="select IYC_OUTTM,iyc_cntrno as BILLNO,CGD_BILLNO AS BILLNO from PSPRD.ALL_XIANGHUO_BAK_VW where iyc_cntrno='%s' and cgd_billno ='%s' and iyc_outymode='提进口重箱' and iyc_type='出场箱' and IYC_OUTTM is not null"%(self.billno,self.ctnno)
        return SQL_TXCC