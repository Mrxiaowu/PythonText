create or replace package body pkg_etl_data_pms is

  /******************************************************************
    *功能模块： 实时成交表|采集数据清洗
    *功能描述：
    *创建人：
    *创建时间：
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_match(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    update t_ods_pms_match t
       set t.stkid = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
                     t.stkcode;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    insert into ods_pms_match
      select *
        from t_ods_pms_match t1
       where not exists (select 1
                from ods_pms_match t2
               where t1.matchsno = t2.matchsno
                 and t1.trddate = t2.trddate);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;

  end etl_ods_pms_match;

  /******************************************************************
    *功能模块： 银行间成交表|采集数据清洗
    *功能描述：
    *创建人：
    *创建时间：
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/

  procedure etl_ods_pms_bankmatch(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);

    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    update t_ods_pms_bankmatch t
       set t.stkid = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
                     t.stkcode;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    insert into ods_pms_bankmatch
      select *
        from t_ods_pms_bankmatch t1
       where not exists (select 1
                from ods_pms_bankmatch t2
               where t1.matchsno = t2.matchsno
                 and t1.trddate = t2.trddate);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;

  end etl_ods_pms_bankmatch;

  /******************************************************************
    *功能模块： 场外业务确认表|采集数据清洗
    *功能描述：
    *创建人：
    *创建时间：
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/

  procedure etl_ods_pms_outconfirm(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);

    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    update t_ods_pms_outconfirm t
       set t.stkid = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
                     t.stkcode;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    /*    c_sql := 'truncate table t_ods_pms_outconfirm';
    execute immediate c_sql;*/
    c_sql := 'truncate table ods_pms_outconfirm';
    execute immediate c_sql;

    /*    c_sql := 'insert into t_ods_pms_outconfirm (sno, busidate, busitime, fundid, projectid, combid, secuid, stkid, market,' ||
             'stkcode, stkname, in_stkid, stktype, bsflag, busitype, busiprocesstype, investtype, balance, earnestbal, amount,' ||
             'price, realmatchqty, realmatchamt, fee_ratio, fee, status, dealstatus, modi_datetime, operid, exceedflag, rivalid,' ||
             'interest, fee_dealflag, settletype, exec_sno, warrant_code, warrant_qty, warrant_combid, regular_code, combid_in,' ||
             'projectid_in, seat_in, secuid_in, logsno, logsno1, depositsno, bonus, investappsheetno, remark)' ||
             '(select sno, busidate, busitime, fundid, projectid, combid, secuid, stkid, market,' ||
             'stkcode, stkname, in_stkid, stktype, bsflag, busitype, busiprocesstype, investtype, balance, earnestbal, amount,' ||
             'price, realmatchqty, realmatchamt, fee_ratio, fee, status, dealstatus, modi_datetime, operid, exceedflag, rivalid,' ||
             'interest, fee_dealflag, settletype, exec_sno, warrant_code, warrant_qty, warrant_combid, regular_code, combid_in,' ||
             'projectid_in, seat_in, secuid_in, logsno, logsno1, depositsno, nvl(bonus,'' ''), '''' as investappsheetno, remark ' ||
             'from outconfirm' || c_dblinkid || ')';
    execute immediate c_sql;*/

    c_sql := 'insert into ods_pms_outconfirm select * from t_ods_pms_outconfirm';
    execute immediate c_sql;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;

  end etl_ods_pms_outconfirm;

  /******************************************************************
    *功能模块： 基金证券持仓表|采集数据清洗
    *功能描述：
    *创建人：
    *创建时间：
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/

  procedure etl_ods_pms_fundstock(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);

    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    update t_ods_pms_fundstock t
       set t.stkid = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
                     t.stkcode;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    /*    c_sql := 'truncate table t_ods_pms_fundstock';
    execute immediate c_sql;
    c_sql := 'truncate table ods_pms_fundstock';
    execute immediate c_sql; */

    /*c_sql := 'insert into t_ods_pms_fundstock (lastdate, fundid, schemeid, ft_hedgeflag, direction, investtype, secuid,' ||
             'stkid, market, stkcode, lastqty, td_adjustqty, buyqty, saleqty, buysettleqty, salesettleqty, buybal, salebal,' ||
             'buybalincfee, salebalincfee, lastimpawnqty, td_t0_impawnqty, etfapplyqty, etfredeemqty, td_cost, lastcost,' ||
             'td_profit, lastprofit, td_feecost, lastfeecost, td_feeprofit, lastfeeprofit, td_intrcost, lastintrcost,' ||
             'td_intrprofit, lastintrprofit, turncost, ft_margin, ft_openprice, ft_closeprofit, ft_lastfloatprofit,' ||
             'ft_holdprice, ft_closeprofit_d, totalpaidqty, td_paidqty, totalpaidbal, td_paidbal, td_bonusintr, ar_bonusintr,' ||
             'prebonusqty, prepaidqty, newstkqty, newstkbal, undernewstkqty, nextdaymarketqty, lastrqsalebal, tdrqsalebal,' ||
             'combstkqty, nomarginstkqty, changeno, hedgeqty, coverqty, totalbuyoutqty, tradingsecurities, totalbuyoutrzqty)' ||
             '(select lastdate, fundid, schemeid, ft_hedgeflag, direction, investtype, secuid,' ||
             'stkid, market, stkcode, lastqty, td_adjustqty, buyqty, saleqty, buysettleqty, salesettleqty, buybal, salebal,' ||
             'buybalincfee, salebalincfee, lastimpawnqty, td_t0_impawnqty, etfapplyqty, etfredeemqty, td_cost, lastcost,' ||
             'td_profit, lastprofit, td_feecost, lastfeecost, td_feeprofit, lastfeeprofit, td_intrcost, lastintrcost,' ||
             'td_intrprofit, lastintrprofit, turncost, ft_margin, ft_openprice, ft_closeprofit, ft_lastfloatprofit,' ||
             'ft_holdprice, ft_closeprofit_d, totalpaidqty, td_paidqty, totalpaidbal, td_paidbal, td_bonusintr, ar_bonusintr,' ||
             'prebonusqty, prepaidqty, newstkqty, newstkbal, undernewstkqty, nextdaymarketqty, lastrqsalebal, tdrqsalebal,' ||
             'combstkqty, nomarginstkqty, changeno, 0 as hedgeqty, 0 as coverqty, totalbuyoutqty, tradingsecurities, totalbuyoutrzqty ' ||
             'from fundstock' || c_dblinkid || ')';
    execute immediate c_sql;*/

    insert into ods_pms_fundstock
      select *
        from t_ods_pms_fundstock t
       where not exists (select 1
                from ods_pms_fundstock f
               where f.LASTDATE = t.LASTDATE
                 and f.FUNDID = t.fundid
                 and f.SCHEMEID = t.schemeid
                 and f.FT_HEDGEFLAG = t.FT_HEDGEFLAG
                 and f.DIRECTION = t.direction
                 and f.INVESTTYPE = t.investtype
                 and f.SEAT = t.seat
                 and f.SECUID = t.secuid
                 and f.STKID = f.stkid);

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;

  end etl_ods_pms_fundstock;

  /******************************************************************
    *功能模块： 基金资产表|采集数据清洗
    *功能描述：
    *创建人：
    *创建时间：
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/

  procedure etl_ods_pms_projectasset(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);

    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    /*    c_sql := 'truncate table t_ods_pms_projectasset';
    execute immediate c_sql;*/
    c_sql := 'truncate table ods_pms_projectasset';
    execute immediate c_sql;

    /*    c_sql := 'insert into t_ods_pms_projectasset (lastdate, fundid, projectid, lastbal, adjustbal, buybal, salebal, buysettlebal,' ||
             'salesettlebal, t1_dvpbuybal, t1_dvpsalebal, td_newstkbal, newstkbal, td_paidbal, totalpaidbal, new_paidbal,' ||
             'payoutbal, rcvbal, stkasset, bondasset, fundasset, warrantasset, bb_saleasset, bb_buyasset, td_bbsaleasset,' ||
             'td_bbbuyasset, otherasset, ft_lastbal, ft_adjustbal, ft_fee, ft_closeprofit, ft_floatprofit, ft_lastfloatprofit,' ||
             'ft_closeprofit_d, ft_floatprofit_d, ft_fd_fund, ft_margin, td_profit, sumprofit, projectvalue, projecttotalvalue,' ||
             'projectshare, nav, lastprojectvalue, lasttotalvalue, lastnav, weekyield, tenthousandunityield, defaultflag,' ||
             'lastrqsalebal, tdrqsalebal, changeno, ft_buypremium, ft_salepremium, etfissueapplyrepbal, etfissueredemrepbal,totalinvestbal)' ||
             '(select lastdate, fundid||projectid, projectid, lastbal, adjustbal, buybal, salebal, buysettlebal,' ||
             'salesettlebal, t1_dvpbuybal, t1_dvpsalebal, td_newstkbal, newstkbal, td_paidbal, totalpaidbal, new_paidbal,' ||
             'payoutbal, rcvbal, stkasset, bondasset, fundasset, warrantasset, bb_saleasset, bb_buyasset, td_bbsaleasset,' ||
             'td_bbbuyasset, otherasset, ft_lastbal, ft_adjustbal, ft_fee, ft_closeprofit, ft_floatprofit, ft_lastfloatprofit,' ||
             'ft_closeprofit_d, ft_floatprofit_d, ft_fd_fund, ft_margin, td_profit, sumprofit, projectvalue, projecttotalvalue,' ||
             'projectshare, nav, lastprojectvalue, lasttotalvalue, lastnav, weekyield, tenthousandunityield, defaultflag, ' ||
             'lastrqsalebal, tdrqsalebal, changeno, 0 as ft_buypremium, 0 as ft_salepremium, etfissueapplyrepbal, etfissueredemrepbal, totalinvestbal ' ||
             'from projectasset' || c_dblinkid || ')';
    execute immediate c_sql;*/

    c_sql := 'insert into ods_pms_projectasset select * from t_ods_pms_projectasset';
    execute immediate c_sql;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;

  end etl_ods_pms_projectasset;

  /******************************************************************
    *功能模块： 组合证券持仓表|采集数据清洗
    *功能描述：
    *创建人：
    *创建时间：
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/

  procedure etl_ods_pms_combstkbal(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);

    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    update t_ods_pms_combstkbal t
       set t.stkid = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
                     t.stkcode;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    /*c_sql := 'truncate table t_ods_pms_combstkbal';
    execute immediate c_sql;*/
    c_sql := 'truncate table ods_pms_combstkbal';
    execute immediate c_sql;

    /* c_sql := 'insert into t_ods_pms_combstkbal (fundid, projectid, combid, schemeid, ft_hedgeflag, direction, investtype,' ||
             'secuid, stkid, market, stkcode, lastqty, td_adjustqty, buyqty, saleqty, buysettleqty, salesettleqty, buybal,' ||
             'salebal, buybalincfee, salebalincfee, lastimpawnqty, td_t0_impawnqty, etfapplyqty, etfredeemqty, td_cost,' ||
             'lastcost, td_profit, lastprofit, td_feecost, lastfeecost, td_feeprofit, lastfeeprofit, td_intrcost, lastintrcost,' ||
             'td_intrprofit, lastintrprofit, turncost, ft_margin, ft_openprice, ft_closeprofit, ft_lastfloatprofit, ft_holdprice,' ||
             'ft_closeprofit_d, totalpaidqty, td_paidqty, totalpaidbal, td_paidbal, td_bonusintr, ar_bonusintr, prebonusqty,' ||
             'prepaidqty, newstkqty, newstkbal, undernewstkqty, nextdaymarketqty, lastrqsalebal, tdrqsalebal, combstkqty,' ||
             'nomarginstkqty, changeno, hedgeqty, coverqty, totalbuyoutqty, tradingsecurities, totalbuyoutrzqty, etfissueapplyqty, etfissueredemqty)' ||
             '(select fundid, projectid, combid, schemeid, ft_hedgeflag, direction, investtype,' ||
             'secuid, stkid, market, stkcode, lastqty, td_adjustqty, buyqty, saleqty, buysettleqty, salesettleqty, buybal,' ||
             'salebal, buybalincfee, salebalincfee, lastimpawnqty, td_t0_impawnqty, etfapplyqty, etfredeemqty, td_cost,' ||
             'lastcost, td_profit, lastprofit, td_feecost, lastfeecost, td_feeprofit, lastfeeprofit, td_intrcost, lastintrcost,' ||
             'td_intrprofit, lastintrprofit, turncost, ft_margin, ft_openprice, ft_closeprofit, ft_lastfloatprofit, ft_holdprice,' ||
             'ft_closeprofit_d, totalpaidqty, td_paidqty, totalpaidbal, td_paidbal, td_bonusintr, ar_bonusintr, prebonusqty,' ||
             'prepaidqty, newstkqty, newstkbal, undernewstkqty, nextdaymarketqty, lastrqsalebal, tdrqsalebal, combstkqty,' ||
             'nomarginstkqty, changeno, hedgeqty, 0 as coverqty, totalbuyoutqty, tradingsecurities, totalbuyoutrzqty, etfissueapplyqty, etfissueredemqty ' ||
             'from combstkbal' || c_dblinkid || ')';
    execute immediate c_sql;*/

    c_sql := 'insert into ods_pms_combstkbal select * from t_ods_pms_combstkbal';
    execute immediate c_sql;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;

  end etl_ods_pms_combstkbal;

  /******************************************************************
    *功能模块： 项目资产表|采集数据清洗
    *功能描述：
    *创建人：huxiang
    *创建时间：2019年3月6日15:54:09
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_fundasset(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ods_pms_fundasset';
    execute immediate c_sql;

    /* c_sql := 'truncate table t_ods_pms_fundasset';
    execute immediate c_sql;

    c_sql := ' insert into t_ods_pms_fundasset ' ||
             ' select lastdate, fundid||projectid as fundid,lastbal, adjustbal, ' ||
             ' buybal,salebal,buysettlebal,salesettlebal,t1_dvpbuybal,t1_dvpsalebal, ' ||
             ' td_newstkbal,newstkbal,td_paidbal,totalpaidbal,new_paidbal,payoutbal, ' ||
             ' rcvbal,stkasset,bondasset,fundasset,warrantasset,bb_saleasset,bb_buyasset, ' ||
             ' td_bbsaleasset,td_bbbuyasset,otherasset,ft_lastbal,ft_adjustbal,ft_fee, ' ||
             ' ft_closeprofit,ft_floatprofit,ft_lastfloatprofit,ft_closeprofit_d, ' ||
             ' ft_floatprofit_d,ft_fd_fund,ft_margin,td_profit,sumprofit,weekyield,  ' ||
             ' tenthousandunityield,projectvalue,projecttotalvalue,nav,projectshare,  ' ||
             ' lastprojectvalue,lasttotalvalue,lastnav,'''' as totalnav,'''' as unitdividends, ' ||
             ' '''' as totalunitdividends,'''' as td_investbal,'''' as td_drawbal,totalinvestbal, ' ||
             ' '''' as totaldrawbal,lastrqsalebal,tdrqsalebal,changeno,'''' as ft_prebuypremium, ' ||
             ' '''' as ft_buypremium,'''' as ft_presalepremium,'''' as ft_salepremium,'''' as lasttotalnav, ' ||
             ' '''' as firstnav,'''' as highnav,'''' as hightotalnav,'''' as refnav1,'''' as balance1, ' ||
             ' '''' as refbalance,'''' as priorbalance,'''' as guarantrate,'''' as powerfactor,'''' as refrate1, ' ||
             ' '''' as refnav2,'''' as gurantbalance,'''' as balance2,'''' as refrate2,etfissueapplyrepbal, ' ||
             ' etfissueredemrepbal from projectasset' || c_dblinkid;

    execute immediate c_sql;*/
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    insert into ods_pms_fundasset
      select * from t_ods_pms_fundasset;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_fundasset;

  /******************************************************************
    *功能模块： 证券锁定表|采集数据清洗
    *功能描述： 证券锁定表先不管，问了投资交易的同事，说是没有，我们先不采集。 20190307
    *创建人：
    *创建时间：
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_lockstkbal(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ods_pms_lockstkbal';
    execute immediate c_sql;

    c_sql := 'truncate table t_ods_pms_lockstkbal';
    execute immediate c_sql;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_lockstkbal;

  /******************************************************************
    *功能模块： 回购合约表|采集数据清洗
    *功能描述：
    *创建人：huxiang
    *创建时间：2019年3月6日15:10:41
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_bb_contract(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    update t_ods_pms_bb_contract t
       set t.stkid = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
                     t.stkcode;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    insert into ods_pms_bb_contract
      select *
        from t_ods_pms_bb_contract t1
       where not exists (select 1
                from ods_pms_bb_contract t2
               where t1.cleardate = t2.cleardate
                 and t1.sno = t2.sno);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_bb_contract;

  /******************************************************************
    *功能模块： 存款合约表|采集数据清洗
    *功能描述：
    *创建人：
    *创建时间：
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_depositcontract(errcode out int,
                                        errmsg  out varchar2) is

    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    insert into ods_pms_depositcontract
      select *
        from t_ods_pms_depositcontract t1
       where not exists (select 1
                from ods_pms_depositcontract t2
               where t1.depositsno = t2.depositsno);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_depositcontract;

  /******************************************************************
    *功能模块： 交易对手|采集数据清洗
    *功能描述：
    *创建人：huxiang
    *创建时间：2019年3月6日15:10:21
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_traderival(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    insert into ods_pms_traderival
      select *
        from t_ods_pms_traderival t1
       where not exists (select 1
                from ods_pms_traderival t2
               where t1.rivalid = t2.rivalid);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_traderival;

  /******************************************************************
    *功能模块： 实时交易对手拓展信息成交表|采集数据清洗
    *功能描述：
    *创建人：huxiang
    *创建时间：2019年3月7日15:44:18
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_traderival_extend(errcode out int,
                                          errmsg  out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    delete from ods_pms_traderival_extend t
     where exists (select 1
              from t_ods_pms_traderival_extend e
             where t.rivalid = e.rivalid);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    insert into ods_pms_traderival_extend
      select * from t_ods_pms_traderival_extend;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_traderival_extend;

  /******************************************************************
    *功能模块： 成交回购质押表|采集数据清洗
    *功能描述：
    *创建人：xiangpc
    *创建时间：2019年5月20日16:35:00
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_matchbbimpawn(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    update t_ods_pms_matchbbimpawn t
       set t.market = pkg_pub.f_get_dictmap('rdt',
                                            '0',
                                            '***',
                                            '30',
                                            t.market);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    insert into ods_pms_matchbbimpawn
      select *
        from t_ods_pms_matchbbimpawn t1
       where not exists (select 1
                from ods_pms_matchbbimpawn t2
               where t1.matchdate = t2.matchdate
                 and t1.matchsno = t2.matchsno
                 and t1.stkid = t2.stkid
                 and t1.direction = t2.direction);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_matchbbimpawn;

  /******************************************************************
    *功能模块： 基金表|采集数据清洗
    *功能描述：
    *创建人：xiangpc
    *创建时间：2019年5月21日16:35:00
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_fund(errcode out int, errmsg out varchar2) is
    c_sql   varchar2(4000);
    v_count integer;
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ODS_PMS_FUND';
    execute immediate c_sql;

    insert into ods_pms_fund
      select * from t_ods_pms_fund;

    for i in (select t2.fundcode, t2.fundid from ods_pms_fund t2) loop
      v_count := 0;
      select count(*)
        into v_count
        from rdt_fund r
       where r.FUND_CODE = i.fundcode;
      if v_count > 0 then
        --产品存在
        select count(*)
          into v_count
          from rdt_fund_value r
         where r.pcode = i.fundcode
           and r.fname = 'fundid';
        if v_count > 0 then
          update rdt_fund_value t
             set t.fvalue = i.fundid
           where t.pcode = i.fundcode
             and t.fname = 'fundid'
             and t.mflag = '0';
        else
          insert into rdt_fund_value
            (pcode, fname, fvalue)
          values
            (i.fundcode, 'fundid', i.fundid);
        end if;
      end if;
    end loop;

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_fund;

  /******************************************************************
    *功能模块： 银行信息表|采集数据清洗
    *功能描述：
    *创建人：xiangpc
    *创建时间：2019年6月3日15:35:00
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_bankinfo(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ODS_PMS_BANKINFO';
    execute immediate c_sql;

    insert into ods_pms_bankinfo
      select * from t_ods_pms_bankinfo;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_bankinfo;

  /******************************************************************
    *功能模块： 债券信息|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年7月1日
    *修改人：huxin
    *修改时间：2019-07-20
    *修改原因:新增转储到RDT表
  *****************************************************************/
  procedure etl_ods_pms_bondproperty(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ods_pms_bondproperty';
    execute immediate c_sql;

    update t_ods_pms_bondproperty t
       set t.stkid = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
                     t.stkcode;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    insert into ods_pms_bondproperty
      select * from t_ods_pms_bondproperty
        where lastdate = (select max(lastdate) from t_ods_pms_bondproperty);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    --转储到RDT_SR_BOND_QUOTA
    insert into rdt_sr_bond_quota
      (stkid,
       enddate,
       begindate,
       termtomaturity,
       vpyield,
       vpaduration,
       vpconvexity,
       coefficient,
       creditspread,
       longyield,
       shortyield,
       longterm,
       shortterm,
       longduration,
       shortduration,
       datasource,
       insertby,
       inserttime,
       updateby,
       updatetime)
      select t.stkid,
             t.lastdate,
             t.begincalcdate,
             t.leftterm,
             t.yearrate as vpyield,
             t.maturityadjust,
             t.VALUATIONCONVEXITY,
             '' as coefficient,
             '' as creditspread,
             '' as longyield,
             '' as shortyield,
             '' as longterm,
             '' as shortterm,
             '' as longduration,
             '' as shortduration,
             'tz' as datasource,
             '8888' as insertby,
             SYSDATE as inserttime,
             '8888' as updateby,
             SYSDATE as updatetime
        from t_ods_pms_bondproperty t
        where not exists(select 1 from rdt_sr_bond_quota t1
         where t1.stkid = t.stkid and t1.enddate = t.lastdate);

    --转储到RDT_SR_SEC_GRADE(外部评级)
    MERGE INTO rdt_sr_sec_grade t1
    USING (select t2.stkid,
                  t2.lastdate gradeday,
                  '11' gradetype,
                  ' ' grader,
                  pkg_pub.f_get_dictmap('rdt','0','***','pjzh',t2.outappraise) outappraise
             from t_ods_pms_bondproperty t2) t3
    on (t1.stkid = t3.stkid and t1.gradetype = t3.gradetype and t1.gradeday = t3.gradeday
        and t1.grader = t3.grader)
    WHEN MATCHED THEN
      UPDATE
         set t1.gradecode  = t3.outappraise,
             t1.updateby   = '8888',
             t1.updatetime = SRF_FORMATSYSDATE
    WHEN NOT MATCHED THEN
      INSERT
        (t1.stkid,
         t1.gradeday,
         t1.gradetype,
         t1.grader,
         t1.gradecode,
         t1.datasource,
         t1.insertby,
         t1.inserttime,
         t1.updateby,
         t1.updatetime)
      values
        (t3.stkid,
         t3.gradeday,
         t3.gradetype,
         t3.grader,
         t3.outappraise,
         'tz',
         '8888',
         SRF_FORMATSYSDATE,
         '8888',
         SRF_FORMATSYSDATE);

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_bondproperty;

  /******************************************************************
    *功能模块： 证券信息表|采集数据清洗
    *功能描述：
    *创建人：yix
    *创建时间：2019-07-06
    *修改人：huxin
    *修改时间：2019-07-20
    *修改原因: 新增转储到rdt表
  *****************************************************************/
  procedure etl_ods_pms_stock(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    n_colflag integer;
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    MERGE INTO ODS_PMS_STOCK a
    USING (select t.MARKET,
                  t.STKCODE,
                  t.STKNAME,
                  pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||t.stkcode STKID,
                  t.FRZSTKID,
                  pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t1.market) ||t1.stkcode LINKSTKID,
                  t.LINKSTKID1,
                  t.LINKSTKID2,
                  t.STKTYPE,
                  t.MONEYTYPE,
                  t.TRDID,
                  t.SPELLID,
                  t.STKSTATUS,
                  t.STKLEVEL,
                  t.STKRIGHT,
                  t.OUTCODE1,
                  t.OUTCODE2,
                  t.OUTCODE3,
                  t.OUTCODE4,
                  t.STOPFLAG,
                  t.MAXRISEVALUE,
                  t.MAXDOWNVALUE,
                  t.FIXPRICE,
                  t.LIMITPRICEFLAG,
                  t.DEFAULTPRICETYPE,
                  t.MAXRISERATE,
                  t.MAXDOWNRATE,
                  t.PT_ORDERTYPES,
                  t.KEEPMODE,
                  t.TICKETPRICE,
                  t.MTKCALFLAG,
                  t.BSFLAG,
                  t.CANCELFLAG,
                  t.PRICEFLAG,
                  t.STKOVERDRAW,
                  t.PRICEUNIT,
                  t.BUYUNIT,
                  t.SALEUNIT,
                  t.EXCHUNIT,
                  t.MAXQTY,
                  t.MINQTY,
                  t.TYPEUNIT,
                  t.MINBUYQTY,
                  t.LONGSTOPFLAG,
                  t.ORIGRIGHTPERSON,
                  t.ISSUERID,
                  t.STKKIND,
                  t.RIGHTDATE,
                  t.ISSUEDATE,
                  t.MARKETDATE,
                  t.UTTERDATE,
                  t.ISSUERASSET,
                  t.FUNDKIND,
                  t.PT_BSFLAGS,
                  t.PT_ORDERSOURCES,
                  t.PT_INSTRUCTIONRIGHTS,
                  t.MIXEDTYPE,
                  t.BUSICLASS,
                  t.REGORG,
                  t.LASTDATE,
                  t.CHANGENO,
                  t.RETURNFLAG,
                  t.SUBSTKTYPE,
                  t.STKPLATE
             from T_ODS_PMS_STOCK t
            left join t_ods_pms_stock t1
              on t.LINKSTKID = t1.stkid) o
    on (a.STKCODE = o.STKCODE and a.MARKET = o.MARKET)
    WHEN MATCHED THEN
      UPDATE
         set a.STKNAME              = o.STKNAME,
             a.STKID                = o.STKID,
             a.FRZSTKID             = o.FRZSTKID,
             a.LINKSTKID            = o.LINKSTKID,
             a.LINKSTKID1           = o.LINKSTKID1,
             a.LINKSTKID2           = o.LINKSTKID2,
             a.STKTYPE              = o.STKTYPE,
             a.MONEYTYPE            = o.MONEYTYPE,
             a.TRDID                = o.TRDID,
             a.SPELLID              = o.SPELLID,
             a.STKSTATUS            = o.STKSTATUS,
             a.STKLEVEL             = o.STKLEVEL,
             a.STKRIGHT             = o.STKRIGHT,
             a.OUTCODE1             = o.OUTCODE1,
             a.OUTCODE2             = o.OUTCODE2,
             a.OUTCODE3             = o.OUTCODE3,
             a.OUTCODE4             = o.OUTCODE4,
             a.STOPFLAG             = o.STOPFLAG,
             a.MAXRISEVALUE         = o.MAXRISEVALUE,
             a.MAXDOWNVALUE         = o.MAXDOWNVALUE,
             a.FIXPRICE             = o.FIXPRICE,
             a.LIMITPRICEFLAG       = o.LIMITPRICEFLAG,
             a.DEFAULTPRICETYPE     = o.DEFAULTPRICETYPE,
             a.MAXRISERATE          = o.MAXRISERATE,
             a.MAXDOWNRATE          = o.MAXDOWNRATE,
             a.PT_ORDERTYPES        = o.PT_ORDERTYPES,
             a.KEEPMODE             = o.KEEPMODE,
             a.TICKETPRICE          = o.TICKETPRICE,
             a.MTKCALFLAG           = o.MTKCALFLAG,
             a.BSFLAG               = o.BSFLAG,
             a.CANCELFLAG           = o.CANCELFLAG,
             a.PRICEFLAG            = o.PRICEFLAG,
             a.STKOVERDRAW          = o.STKOVERDRAW,
             a.PRICEUNIT            = o.PRICEUNIT,
             a.BUYUNIT              = o.BUYUNIT,
             a.SALEUNIT             = o.SALEUNIT,
             a.EXCHUNIT             = o.EXCHUNIT,
             a.MAXQTY               = o.MAXQTY,
             a.MINQTY               = o.MINQTY,
             a.TYPEUNIT             = o.TYPEUNIT,
             a.MINBUYQTY            = o.MINBUYQTY,
             a.LONGSTOPFLAG         = o.LONGSTOPFLAG,
             a.ORIGRIGHTPERSON      = o.ORIGRIGHTPERSON,
             a.ISSUERID             = o.ISSUERID,
             a.STKKIND              = o.STKKIND,
             a.RIGHTDATE            = o.RIGHTDATE,
             a.ISSUEDATE            = o.ISSUEDATE,
             a.MARKETDATE           = o.MARKETDATE,
             a.UTTERDATE            = o.UTTERDATE,
             a.ISSUERASSET          = o.ISSUERASSET,
             a.FUNDKIND             = o.FUNDKIND,
             a.PT_BSFLAGS           = o.PT_BSFLAGS,
             a.PT_ORDERSOURCES      = o.PT_ORDERSOURCES,
             a.PT_INSTRUCTIONRIGHTS = o.PT_INSTRUCTIONRIGHTS
    WHEN NOT MATCHED THEN
      INSERT
        (a.MARKET,
         a.STKCODE,
         a.STKNAME,
         a.STKID,
         a.FRZSTKID,
         a.LINKSTKID,
         a.LINKSTKID1,
         a.LINKSTKID2,
         a.STKTYPE,
         a.MONEYTYPE,
         a.TRDID,
         a.SPELLID,
         a.STKSTATUS,
         a.STKLEVEL,
         a.STKRIGHT,
         a.OUTCODE1,
         a.OUTCODE2,
         a.OUTCODE3,
         a.OUTCODE4,
         a.STOPFLAG,
         a.MAXRISEVALUE,
         a.MAXDOWNVALUE,
         a.FIXPRICE,
         a.LIMITPRICEFLAG,
         a.DEFAULTPRICETYPE,
         a.MAXRISERATE,
         a.MAXDOWNRATE,
         a.PT_ORDERTYPES,
         a.KEEPMODE,
         a.TICKETPRICE,
         a.MTKCALFLAG,
         a.BSFLAG,
         a.CANCELFLAG,
         a.PRICEFLAG,
         a.STKOVERDRAW,
         a.PRICEUNIT,
         a.BUYUNIT,
         a.SALEUNIT,
         a.EXCHUNIT,
         a.MAXQTY,
         a.MINQTY,
         a.TYPEUNIT,
         a.MINBUYQTY,
         a.LONGSTOPFLAG,
         a.ORIGRIGHTPERSON,
         a.ISSUERID,
         a.STKKIND,
         a.RIGHTDATE,
         a.ISSUEDATE,
         a.MARKETDATE,
         a.UTTERDATE,
         a.ISSUERASSET,
         a.FUNDKIND,
         a.PT_BSFLAGS,
         a.PT_ORDERSOURCES,
         a.PT_INSTRUCTIONRIGHTS,
         a.MIXEDTYPE,
         a.BUSICLASS,
         a.REGORG,
         a.LASTDATE,
         a.CHANGENO,
         a.RETURNFLAG,
         a.SUBSTKTYPE,
         a.STKPLATE)
      values
        (o.MARKET,
         o.STKCODE,
         o.STKNAME,
         o.STKID,
         o.FRZSTKID,
         o.LINKSTKID,
         o.LINKSTKID1,
         o.LINKSTKID2,
         o.STKTYPE,
         o.MONEYTYPE,
         o.TRDID,
         o.SPELLID,
         o.STKSTATUS,
         o.STKLEVEL,
         o.STKRIGHT,
         o.OUTCODE1,
         o.OUTCODE2,
         o.OUTCODE3,
         o.OUTCODE4,
         o.STOPFLAG,
         o.MAXRISEVALUE,
         o.MAXDOWNVALUE,
         o.FIXPRICE,
         o.LIMITPRICEFLAG,
         o.DEFAULTPRICETYPE,
         o.MAXRISERATE,
         o.MAXDOWNRATE,
         o.PT_ORDERTYPES,
         o.KEEPMODE,
         o.TICKETPRICE,
         o.MTKCALFLAG,
         o.BSFLAG,
         o.CANCELFLAG,
         o.PRICEFLAG,
         o.STKOVERDRAW,
         o.PRICEUNIT,
         o.BUYUNIT,
         o.SALEUNIT,
         o.EXCHUNIT,
         o.MAXQTY,
         o.MINQTY,
         o.TYPEUNIT,
         o.MINBUYQTY,
         o.LONGSTOPFLAG,
         o.ORIGRIGHTPERSON,
         o.ISSUERID,
         o.STKKIND,
         o.RIGHTDATE,
         o.ISSUEDATE,
         o.MARKETDATE,
         o.UTTERDATE,
         o.ISSUERASSET,
         o.FUNDKIND,
         o.PT_BSFLAGS,
         o.PT_ORDERSOURCES,
         o.PT_INSTRUCTIONRIGHTS,
         o.MIXEDTYPE,
         o.BUSICLASS,
         o.REGORG,
         o.LASTDATE,
         o.CHANGENO,
         o.RETURNFLAG,
         o.SUBSTKTYPE,
         o.STKPLATE);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    --转储到rdt_sr_security表
    --来源表：ods_pms_STOCK
    select count(*) into n_colflag from etl_data_configure t
    where t.tablename = 'SecuMain' and t.status ='2';
    if n_colflag = 0 then
      MERGE INTO rdt_sr_security t1
      USING (select t2.stkid,
                    pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t2.market) market,
                    t2.stkcode,
                    t2.stkname,
                    t2.frzstkid,
                    t2.linkstkid,
                    t2.linkstkid1,
                    t2.linkstkid2,
                    t2.fundkind,
                    srf_trans_sectype3(t2.stktype) stktype,
                    t2.ticketprice,
                    t2.regorg,
                    t2.marketdate,
                    t2.ISSUERID
               from ODS_PMS_STOCK t2) t3
      on (t1.stkid = t3.stkid)
      WHEN MATCHED THEN
        UPDATE
           set t1.market      = t3.market,
               t1.stkcode     = t3.stkcode,
               t1.stkname     = t3.stkname,
               t1.frzstkid    = t3.frzstkid,
               t1.linkstkid   = t3.linkstkid,
               t1.linkstkid1  = t3.linkstkid1,
               t1.linkstkid2  = t3.linkstkid2,
               t1.fundkind    = t3.fundkind,
               t1.stktype     = t3.stktype,
               t1.ticketprice = t3.ticketprice,
               t1.regorg      = t3.regorg,
               t1.listeddate  = t3.marketdate,
               t1.companycode = t3.ISSUERID
      WHEN NOT MATCHED THEN
        INSERT
          (t1.stkid,
           t1.market,
           t1.stkcode,
           t1.stkname,
           t1.frzstkid,
           t1.linkstkid,
           t1.linkstkid1,
           t1.linkstkid2,
           t1.fundkind,
           t1.stktype,
           t1.ticketprice,
           t1.regorg,
           t1.listeddate,
           t1.companycode)
        values
          (t3.stkid,
           t3.market,
           t3.stkcode,
           t3.stkname,
           t3.frzstkid,
           t3.linkstkid,
           t3.linkstkid1,
           t3.linkstkid2,
           t3.fundkind,
           t3.stktype,
           t3.ticketprice,
           t3.regorg,
           t3.marketdate,
           t3.ISSUERID);
      	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    end if;

    --转储到RDT_SR_FUND_BASEINFO
    select count(*) into n_colflag from etl_data_configure t
    where t.tablename = 'MF_FundArchives' and t.status ='2';
    if n_colflag = 0 then
      MERGE INTO rdt_sr_fund_baseinfo t1
      USING (select t2.stkid,
                    pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t2.market) market,
                    t2.stkcode,
                    t2.stkname,
                    t2.fundkind,
                    srf_trans_sectype3(t2.stktype) stktype,
                    t2.marketdate,
                    t2.ticketprice
               from ODS_PMS_STOCK t2
               where t2.stktype in ('4','5','E','F','L','N')) t3
      on (t1.stkid = t3.stkid)
      WHEN MATCHED THEN
        UPDATE
           set t1.market      = t3.market,
               t1.stkcode     = t3.stkcode,
               t1.stkname     = t3.stkname,
               t1.fundkind    = t3.fundkind,
               t1.stktype     = t3.stktype,
               t1.ticketprice = t3.ticketprice,
               t1.marketdate  = t3.marketdate
      WHEN NOT MATCHED THEN
        INSERT
          (t1.stkid,
           t1.market,
           t1.stkcode,
           t1.stkname,
           t1.fundkind,
           t1.stktype,
           t1.ticketprice,
           t1.marketdate)
        values
          (t3.stkid,
           t3.market,
           t3.stkcode,
           t3.stkname,
           t3.fundkind,
           t3.stktype,
           t3.ticketprice,
           t3.marketdate);
    end if;

    --转储到RDT_SR_BOND_BASEINFO
    select count(*) into n_colflag from etl_data_configure t
    where t.tablename = 'Bond_BasicInfo' and t.status ='2';
    if n_colflag = 0 then
      MERGE INTO rdt_sr_bond_baseinfo t1
      USING (select t2.stkid,
                    pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t2.market) market,
                    t2.stkcode,
                    t2.stkname,
                    srf_trans_sectype3(t2.stktype) stktype,
                    t3.issueprice,
                    t3.ticketprice,
                    t3.yearrate,
                    t3.bondintr,
                    t3.begincalcdate,
                    --0 零息债券 1 贴现债券  2 固定利率债券，3 浮动利率债券 4 一次性还本付息债券
                    decode(t3.bondtype,'0','0','1','2','2','0','3','3','0') bondtype,
                    ' ' calstype,
                    ' ' payflag,
                    t3.payinteval,  --字段转换
                    t2.issuerid,
                    t3.enddate
               from ODS_PMS_STOCK t2, ods_pms_bondproperty t3
              where t2.stkid = t3.stkid
              and t2.stktype in ('1','2','8','9','A','B','C','M','h','i','j','k','K','s','a','b','c','d','z')) t4
      on (t1.stkid = t4.stkid)
      WHEN MATCHED THEN
        UPDATE
           set t1.market      = t4.market,
               t1.stkcode     = t4.stkcode,
               t1.stkname     = t4.stkname,
               t1.stktype     = t4.stktype,
               t1.bondtype    = t4.bondtype,
               t1.issueprice  = t4.issueprice,
               t1.ticketprice = t4.ticketprice,
              -- t1.intrrate    = t4.bondintr,
               t1.intrrate    = t4.yearrate,
               t1.begcalcdate = t4.begincalcdate,
               t1.endcalcdate = t4.enddate,
               t1.paytype     = t4.payinteval, --可能需要转成字典
               t1.issuercode  = t4.issuerid,
               t1.end_date    = t4.enddate
      WHEN NOT MATCHED THEN
        INSERT
          (t1.stkid,
           t1.market,
           t1.stkcode,
           t1.stkname,
           t1.stktype,
           t1.issueprice,
           t1.ticketprice,
           t1.intrrate,
           t1.begcalcdate,
           t1.endcalcdate,
           t1.bondtype,
           t1.calstype,
           t1.payflag,
           t1.paytype,
           t1.issuercode,
           t1.end_date)
        values
          (t4.stkid,
           t4.market,
           t4.stkcode,
           t4.stkname,
           t4.stktype,
           t4.issueprice,
           t4.ticketprice,
         --  t4.bondintr,
           t4.yearrate,
           t4.begincalcdate,
           t4.enddate,
           t4.bondtype,
           t4.payflag,
           t4.payflag,
           t4.payinteval,
           t4.issuerid,
           t4.enddate);
    end if;

    --转储到RDT_SR_BOND_ISSUER
    select count(*) into n_colflag from etl_data_configure t
    where t.tablename = 'Bond_Issue' and t.status ='2';
    if n_colflag = 0 then
      MERGE INTO rdt_sr_bond_issuer t1
      USING (select t2.stkid,
                    t2.marketdate listeddate,
                    t2.issuedate,
                    t4.issuerid,
                    t2.existlimit,
                    t2.issueprice
               from ods_pms_bondproperty t2,
                    ods_pms_stock        t4
              where t2.stkid = t4.stkid) t5
      on (t1.stkid = t5.stkid)
      WHEN MATCHED THEN
        UPDATE
           set t1.issdate    = t5.issuedate,
               t1.listeddate = t5.listeddate,
               t1.issuercode = t5.issuerid,
               t1.companycode= t5.issuerid,
               t1.maturity   = t5.existlimit,
               t1.issueprice = t5.issueprice
      WHEN NOT MATCHED THEN
        INSERT
          (t1.stkid,
           t1.listeddate,
           t1.issdate,
           t1.issuercode,
           t1.companycode,
           t1.maturity,
           t1.issueprice,
           t1.datasource,
           t1.insertby,
           t1.inserttime,
           t1.updateby,
           t1.updatetime)
        values
          (t5.stkid,
           t5.listeddate,
           t5.issuedate,
           t5.issuerid,
           t5.issuerid,
           t5.existlimit,
           t5.issueprice,
           'tzjy',
           '8888',
           SRF_FORMATSYSDATE,
           '8888',
           SRF_FORMATSYSDATE);
    end if;

    --转储到rdt_sr_bondbid
    select count(*) into n_colflag from etl_data_configure t
    where t.tablename = 'Bond_Bid' and t.status ='2';
    if n_colflag = 0 then
      MERGE INTO rdt_sr_bondbid t1
      USING (select t2.stkid,
                    t2.market,
                    t2.stkcode,
                    t2.issuedate,
                    t4.issuerid,
                    t2.issueprice,
                    t2.yearrate,
                    t2.bondintr
               from ods_pms_bondproperty t2,
                    ods_pms_stock        t4
              where t2.stkid = t4.stkid) t5
      on (t1.stkid = t5.stkid)
      WHEN MATCHED THEN
        UPDATE
           set t1.issuedate  = t5.issuedate,
               t1.issuercode = t5.issuerid,
               t1.issueprice = t5.issueprice,
               t1.couponrate = t5.bondintr,
               t1.referrenceyield = t5.yearrate
      WHEN NOT MATCHED THEN
        INSERT
          (t1.stkid,
           t1.market,
           t1.stkcode,
           t1.issuedate,
           t1.issuercode,
           t1.issueprice,
           t1.couponrate,
           t1.referrenceyield,
           t1.issuetype,
           t1.bidtype,
           t1.bidcategory,
           t1.datasource,
           t1.insertby,
           t1.inserttime,
           t1.updateby,
           t1.updatetime)
        values
          (t5.stkid,
           t5.market,
           t5.stkcode,
           t5.issuedate,
           t5.issuerid,
           t5.issueprice,
           t5.bondintr,
           t5.yearrate,
           0,
           0,
           0,
           'tzjy',
           '8888',
           SRF_FORMATSYSDATE,
           '8888',
           SRF_FORMATSYSDATE);
    end if;
  	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_stock;

  /******************************************************************
    *功能模块： 场外债券成交监管报送表|采集数据清洗
    *功能描述：
    *创建人：yix
    *创建时间：2019-07-06
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_jgbs_bondmatch(errcode out int,
                                       errmsg  out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    update T_ODS_PMS_JGBS_BONDMATCH t
       set t.stkid = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
                     t.stkcode;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    MERGE INTO ODS_PMS_JGBS_BONDMATCH a
    USING (select MATCHSNO,
                  TRDDATE,
                  FUNDID,
                  FUNDCODE,
                  FUNDNAME,
                  FULLNAME,
                  FUNDTYPE,
                  PROJECTID,
                  COMBID,
                  MARKET,
                  STKCODE,
                  STKID,
                  STKNAME,
                  BUSICLASS,
                  BSFLAG,
                  RPTBSFLAG,
                  STKTYPE,
                  MIXEDTYPES,
                  APPRAISE,
                  MAINAPPRAISE,
                  BONDINTR,
                  MATCHTIME,
                  MATCHPRICE,
                  MATCHFULLPRICE,
                  MATCHQTY,
                  MATCHAMT,
                  CLEARAMT,
                  CLEARSPEED,
                  BB_RATE,
                  FIRST_CLEARDATE,
                  SECOND_CLEARDATE,
                  RIVALID,
                  RIVALNAME,
                  RIVALTYPE,
                  REMARK
             from T_ODS_PMS_JGBS_BONDMATCH t) o
    on (a.MATCHSNO = o.MATCHSNO and a.TRDDATE = o.TRDDATE and a.BSFLAG = o.BSFLAG)
    WHEN MATCHED THEN
      UPDATE
         set a.FUNDID           = o.FUNDID,
             a.FUNDCODE         = o.FUNDCODE,
             a.FUNDNAME         = o.FUNDNAME,
             a.FULLNAME         = o.FULLNAME,
             a.FUNDTYPE         = o.FUNDTYPE,
             a.PROJECTID        = o.PROJECTID,
             a.COMBID           = o.COMBID,
             a.MARKET           = o.MARKET,
             a.STKCODE          = o.STKCODE,
             a.STKID            = o.STKID,
             a.STKNAME          = o.STKNAME,
             a.BUSICLASS        = o.BUSICLASS,
             a.RPTBSFLAG        = o.RPTBSFLAG,
             a.STKTYPE          = o.STKTYPE,
             a.MIXEDTYPES       = o.MIXEDTYPES,
             a.APPRAISE         = o.APPRAISE,
             a.MAINAPPRAISE     = o.MAINAPPRAISE,
             a.BONDINTR         = o.BONDINTR,
             a.MATCHTIME        = o.MATCHTIME,
             a.MATCHPRICE       = o.MATCHPRICE,
             a.MATCHFULLPRICE   = o.MATCHFULLPRICE,
             a.MATCHQTY         = o.MATCHQTY,
             a.MATCHAMT         = o.MATCHAMT,
             a.CLEARAMT         = o.CLEARAMT,
             a.CLEARSPEED       = o.CLEARSPEED,
             a.BB_RATE          = o.BB_RATE,
             a.FIRST_CLEARDATE  = o.FIRST_CLEARDATE,
             a.SECOND_CLEARDATE = o.SECOND_CLEARDATE,
             a.RIVALID          = o.RIVALID,
             a.RIVALNAME        = o.RIVALNAME,
             a.RIVALTYPE        = o.RIVALTYPE,
             a.REMARK           = o.REMARK
    WHEN NOT MATCHED THEN
      INSERT
        (a.MATCHSNO,
         a.TRDDATE,
         a.FUNDID,
         a.FUNDCODE,
         a.FUNDNAME,
         a.FULLNAME,
         a.FUNDTYPE,
         a.PROJECTID,
         a.COMBID,
         a.MARKET,
         a.STKCODE,
         a.STKID,
         a.STKNAME,
         a.BUSICLASS,
         a.BSFLAG,
         a.RPTBSFLAG,
         a.STKTYPE,
         a.MIXEDTYPES,
         a.APPRAISE,
         a.MAINAPPRAISE,
         a.BONDINTR,
         a.MATCHTIME,
         a.MATCHPRICE,
         a.MATCHFULLPRICE,
         a.MATCHQTY,
         a.MATCHAMT,
         a.CLEARAMT,
         a.CLEARSPEED,
         a.BB_RATE,
         a.FIRST_CLEARDATE,
         a.SECOND_CLEARDATE,
         a.RIVALID,
         a.RIVALNAME,
         a.RIVALTYPE,
         a.REMARK)
      values
        (o.MATCHSNO,
         o.TRDDATE,
         o.FUNDID,
         o.FUNDCODE,
         o.FUNDNAME,
         o.FULLNAME,
         o.FUNDTYPE,
         o.PROJECTID,
         o.COMBID,
         o.MARKET,
         o.STKCODE,
         o.STKID,
         o.STKNAME,
         o.BUSICLASS,
         o.BSFLAG,
         o.RPTBSFLAG,
         o.STKTYPE,
         o.MIXEDTYPES,
         o.APPRAISE,
         o.MAINAPPRAISE,
         o.BONDINTR,
         o.MATCHTIME,
         o.MATCHPRICE,
         o.MATCHFULLPRICE,
         o.MATCHQTY,
         o.MATCHAMT,
         o.CLEARAMT,
         o.CLEARSPEED,
         o.BB_RATE,
         o.FIRST_CLEARDATE,
         o.SECOND_CLEARDATE,
         o.RIVALID,
         o.RIVALNAME,
         o.RIVALTYPE,
         o.REMARK);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_jgbs_bondmatch;

  /******************************************************************
    *功能模块： 基金场外债券持仓监管报送表|采集数据清洗
    *功能描述：
    *创建人：yix
    *创建时间：2019-07-06
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_jgbs_fundbondbal(errcode out int,
                                         errmsg  out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    update T_ODS_PMS_JGBS_FUNDBONDBAL t
       set t.stkid = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
                     t.stkcode;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    MERGE INTO ODS_PMS_JGBS_FUNDBONDBAL a
    USING (select LASTDATE,
                  FUNDID,
                  FUNDCODE,
                  FUNDNAME,
                  FULLNAME,
                  FUNDTYPE,
                  MARKET,
                  STKCODE,
                  STKID,
                  SCHEMEID,
                  FT_HEDGEFLAG,
                  INVESTTYPE,
                  DIRECTION,
                  LASTQTY,
                  TD_ADJUSTQTY,
                  BUYQTY,
                  SALEQTY,
                  TICKETPRICE,
                  CLOSEPRICE,
                  MKTCLNPRIC,
                  STKTYPE,
                  MIXEDTYPES,
                  APPRAISE,
                  MAINAPPRAISE,
                  TRDID
             from T_ODS_PMS_JGBS_FUNDBONDBAL t) o
    on (a.LASTDATE = o.LASTDATE and a.FUNDID = o.FUNDID and a.STKID = o.STKID and a.SCHEMEID = o.SCHEMEID and a.FT_HEDGEFLAG = o.FT_HEDGEFLAG and a.INVESTTYPE = o.INVESTTYPE and a.DIRECTION = o.DIRECTION)
    WHEN MATCHED THEN
      UPDATE
         set a.FUNDCODE     = o.FUNDCODE,
             a.FUNDNAME     = o.FUNDNAME,
             a.FULLNAME     = o.FULLNAME,
             a.FUNDTYPE     = o.FUNDTYPE,
             a.MARKET       = o.MARKET,
             a.STKCODE      = o.STKCODE,
             a.LASTQTY      = o.LASTQTY,
             a.TD_ADJUSTQTY = o.TD_ADJUSTQTY,
             a.BUYQTY       = o.BUYQTY,
             a.SALEQTY      = o.SALEQTY,
             a.TICKETPRICE  = o.TICKETPRICE,
             a.CLOSEPRICE   = o.CLOSEPRICE,
             a.MKTCLNPRIC   = o.MKTCLNPRIC,
             a.STKTYPE      = o.STKTYPE,
             a.MIXEDTYPES   = o.MIXEDTYPES,
             a.APPRAISE     = o.APPRAISE,
             a.MAINAPPRAISE = o.MAINAPPRAISE,
             a.TRDID        = o.TRDID
    WHEN NOT MATCHED THEN
      INSERT
        (a.LASTDATE,
         a.FUNDID,
         a.FUNDCODE,
         a.FUNDNAME,
         a.FULLNAME,
         a.FUNDTYPE,
         a.MARKET,
         a.STKCODE,
         a.STKID,
         a.SCHEMEID,
         a.FT_HEDGEFLAG,
         a.INVESTTYPE,
         a.DIRECTION,
         a.LASTQTY,
         a.TD_ADJUSTQTY,
         a.BUYQTY,
         a.SALEQTY,
         a.TICKETPRICE,
         a.CLOSEPRICE,
         a.MKTCLNPRIC,
         a.STKTYPE,
         a.MIXEDTYPES,
         a.APPRAISE,
         a.MAINAPPRAISE,
         a.TRDID)
      values
        (o.LASTDATE,
         o.FUNDID,
         o.FUNDCODE,
         o.FUNDNAME,
         o.FULLNAME,
         o.FUNDTYPE,
         o.MARKET,
         o.STKCODE,
         o.STKID,
         o.SCHEMEID,
         o.FT_HEDGEFLAG,
         o.INVESTTYPE,
         o.DIRECTION,
         o.LASTQTY,
         o.TD_ADJUSTQTY,
         o.BUYQTY,
         o.SALEQTY,
         o.TICKETPRICE,
         o.CLOSEPRICE,
         o.MKTCLNPRIC,
         o.STKTYPE,
         o.MIXEDTYPES,
         o.APPRAISE,
         o.MAINAPPRAISE,
         o.TRDID);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_jgbs_fundbondbal;

  /******************************************************************
    *功能模块： 基金资产监管报送表|采集数据清洗
    *功能描述：
    *创建人：yix
    *创建时间：2019-07-06
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_jgbs_fundasset(errcode out int,
                                       errmsg  out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    --更新
    MERGE INTO ODS_PMS_JGBS_FUNDASSET a
    USING (select LASTDATE,
                  FUNDID,
                  FUNDCODE,
                  FUNDNAME,
                  FULLNAME,
                  FUNDTYPE,
                  FUNDVALUE,
                  FUNDTOTALVALUE,
                  NAV,
                  FUNDSHARE,
                  LASTFUNDVALUE,
                  LASTFUNDTOTALVALUE,
                  LASTNAV,
                  BB_SALEASSET,
                  BB_BUYASSET,
                  TD_BBSALEASSET,
                  TD_BBBUYASSET,
                  BONDASSET
             from T_ODS_PMS_JGBS_FUNDASSET t) o
    on (a.LASTDATE = o.LASTDATE and a.FUNDID = o.FUNDID)
    WHEN MATCHED THEN
      UPDATE
         set a.FUNDCODE           = o.FUNDCODE,
             a.FUNDNAME           = o.FUNDNAME,
             a.FULLNAME           = o.FULLNAME,
             a.FUNDTYPE           = o.FUNDTYPE,
             a.FUNDVALUE          = o.FUNDVALUE,
             a.FUNDTOTALVALUE     = o.FUNDTOTALVALUE,
             a.NAV                = o.NAV,
             a.FUNDSHARE          = o.FUNDSHARE,
             a.LASTFUNDVALUE      = o.LASTFUNDVALUE,
             a.LASTFUNDTOTALVALUE = o.LASTFUNDTOTALVALUE,
             a.LASTNAV            = o.LASTNAV,
             a.BB_SALEASSET       = o.BB_SALEASSET,
             a.BB_BUYASSET        = o.BB_BUYASSET,
             a.TD_BBSALEASSET     = o.TD_BBSALEASSET,
             a.TD_BBBUYASSET      = o.TD_BBBUYASSET,
             a.BONDASSET          = o.BONDASSET
    WHEN NOT MATCHED THEN
      INSERT
        (a.LASTDATE,
         a.FUNDID,
         a.FUNDCODE,
         a.FUNDNAME,
         a.FULLNAME,
         a.FUNDTYPE,
         a.FUNDVALUE,
         a.FUNDTOTALVALUE,
         a.NAV,
         a.FUNDSHARE,
         a.LASTFUNDVALUE,
         a.LASTFUNDTOTALVALUE,
         a.LASTNAV,
         a.BB_SALEASSET,
         a.BB_BUYASSET,
         a.TD_BBSALEASSET,
         a.TD_BBBUYASSET,
         a.BONDASSET)
      values
        (o.LASTDATE,
         o.FUNDID,
         o.FUNDCODE,
         o.FUNDNAME,
         o.FULLNAME,
         o.FUNDTYPE,
         o.FUNDVALUE,
         o.FUNDTOTALVALUE,
         o.NAV,
         o.FUNDSHARE,
         o.LASTFUNDVALUE,
         o.LASTFUNDTOTALVALUE,
         o.LASTNAV,
         o.BB_SALEASSET,
         o.BB_BUYASSET,
         o.TD_BBSALEASSET,
         o.TD_BBBUYASSET,
         o.BONDASSET);

    COMMIT;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_jgbs_fundasset;

  /******************************************************************
    *功能模块： 交易日历表|采集数据清洗
    *功能描述：
    *创建人：yix
    *创建时间：2019-07-06
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_trdcalendar(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ods_pms_trdcalendar';
    execute immediate c_sql;

    insert into ods_pms_trdcalendar
      select * from t_ods_pms_trdcalendar t1;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_trdcalendar;

  /******************************************************************
    *功能模块： 财务数据|采集数据清洗
    *功能描述：
    *创建人：yix
    *创建时间：2019-07-06
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_fadata(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    MERGE INTO ODS_PMS_FADATA a
    USING (select BUSIDATE,
                  FUNDID,
                  SUBJECTID,
                  SUBJECTNAME,
                  FABAL,
                  STKFLAG,
                  MARKET,
                  STKAMOUNT,
                  UNITINVEST,
                  TOTALCOST,
                  PRICE,
                  STKASSET,
                  BONDINTR,
                  FUNDKIND,
                  STKID,
                  VALIDFLAG,
                  SYSSUBJECTID,
                  MONEYTYPE,
                  FASTKCODE,
                  REMARK,
                  DIRECTION,
                  FT_HEDGEFLAG
             from T_ODS_PMS_FADATA t) o
    on (a.BUSIDATE = o.BUSIDATE and a.FUNDID = o.FUNDID and a.SUBJECTID = o.SUBJECTID)
    WHEN MATCHED THEN
      UPDATE
         set a.SUBJECTNAME  = o.SUBJECTNAME,
             a.FABAL        = o.FABAL,
             a.STKFLAG      = o.STKFLAG,
             a.MARKET       = o.MARKET,
             a.STKAMOUNT    = o.STKAMOUNT,
             a.UNITINVEST   = o.UNITINVEST,
             a.TOTALCOST    = o.TOTALCOST,
             a.PRICE        = o.PRICE,
             a.STKASSET     = o.STKASSET,
             a.BONDINTR     = o.BONDINTR,
             a.FUNDKIND     = o.FUNDKIND,
             a.STKID        = o.STKID,
             a.VALIDFLAG    = o.VALIDFLAG,
             a.SYSSUBJECTID = o.SYSSUBJECTID,
             a.MONEYTYPE    = o.MONEYTYPE,
             a.FASTKCODE    = o.FASTKCODE,
             a.REMARK       = o.REMARK,
             a.DIRECTION    = o.DIRECTION,
             a.FT_HEDGEFLAG = o.FT_HEDGEFLAG
    WHEN NOT MATCHED THEN
      INSERT
        (a.BUSIDATE,
         a.FUNDID,
         a.SUBJECTID,
         a.SUBJECTNAME,
         a.FABAL,
         a.STKFLAG,
         a.MARKET,
         a.STKAMOUNT,
         a.UNITINVEST,
         a.TOTALCOST,
         a.PRICE,
         a.STKASSET,
         a.BONDINTR,
         a.FUNDKIND,
         a.STKID,
         a.VALIDFLAG,
         a.SYSSUBJECTID,
         a.MONEYTYPE,
         a.FASTKCODE,
         a.REMARK,
         a.DIRECTION,
         a.FT_HEDGEFLAG)
      values
        (o.BUSIDATE,
         o.FUNDID,
         o.SUBJECTID,
         o.SUBJECTNAME,
         o.FABAL,
         o.STKFLAG,
         o.MARKET,
         o.STKAMOUNT,
         o.UNITINVEST,
         o.TOTALCOST,
         o.PRICE,
         o.STKASSET,
         o.BONDINTR,
         o.FUNDKIND,
         o.STKID,
         o.VALIDFLAG,
         o.SYSSUBJECTID,
         o.MONEYTYPE,
         o.FASTKCODE,
         o.REMARK,
         o.DIRECTION,
         o.FT_HEDGEFLAG);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_fadata;

  /******************************************************************
    *功能模块： 中债估值表|采集数据清洗
    *功能描述：
    *创建人：yix
    *创建时间：2019年7月15日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_bondestimate(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    --更新
    MERGE INTO ODS_PMS_BONDESTIMATE a
    USING (select STKCODE,
                  MARKET,
                  RELIBITY,
                  STKNAME,
                  VALTDATE,
                  LISTGMKTCD,
                  LISTGMKT,
                  YRSTOMTRTY,
                  VALTDDRTYP,
                  ACRDINTRST,
                  VALTDCLNPR,
                  VALTDYLD,
                  VALTDMODFD,
                  VALTDCONEX,
                  VALTDPVBP,
                  VALTDSPRDD,
                  VALTDSPRDC,
                  MKTDRTYPRI,
                  MKTCLNPRIC,
                  MKTYLD,
                  MKTMODFDDR,
                  MKTCONXTY,
                  MKTPVBP,
                  MKTSPRDDRT,
                  MKTSPRDCON,
                  VALTDRATED,
                  VALTDRATEC,
                  MKTATEDRT,
                  MKTRATECON,
                  ENDDAYYALT,
                  ENDDAYACRD,
                  RMNGPRNCPL,
                  DCSYL,
                  lastdate
             from T_ODS_PMS_BONDESTIMATE t) o
    on (a.STKCODE = o.STKCODE and a.MARKET = o.MARKET and a.RELIBITY = o.RELIBITY and a.VALTDATE = o.VALTDATE
       and a.lastdate = o.lastdate)
    WHEN MATCHED THEN
      UPDATE
         set a.STKNAME    = o.STKNAME,
             a.LISTGMKTCD = o.LISTGMKTCD,
             a.LISTGMKT   = o.LISTGMKT,
             a.YRSTOMTRTY = o.YRSTOMTRTY,
             a.VALTDDRTYP = o.VALTDDRTYP,
             a.ACRDINTRST = o.ACRDINTRST,
             a.VALTDCLNPR = o.VALTDCLNPR,
             a.VALTDYLD   = o.VALTDYLD,
             a.VALTDMODFD = o.VALTDMODFD,
             a.VALTDCONEX = o.VALTDCONEX,
             a.VALTDPVBP  = o.VALTDPVBP,
             a.VALTDSPRDD = o.VALTDSPRDD,
             a.VALTDSPRDC = o.VALTDSPRDC,
             a.MKTDRTYPRI = o.MKTDRTYPRI,
             a.MKTCLNPRIC = o.MKTCLNPRIC,
             a.MKTYLD     = o.MKTYLD,
             a.MKTMODFDDR = o.MKTMODFDDR,
             a.MKTCONXTY  = o.MKTCONXTY,
             a.MKTPVBP    = o.MKTPVBP,
             a.MKTSPRDDRT = o.MKTSPRDDRT,
             a.MKTSPRDCON = o.MKTSPRDCON,
             a.VALTDRATED = o.VALTDRATED,
             a.VALTDRATEC = o.VALTDRATEC,
             a.MKTATEDRT  = o.MKTATEDRT,
             a.MKTRATECON = o.MKTRATECON,
             a.ENDDAYYALT = o.ENDDAYYALT,
             a.ENDDAYACRD = o.ENDDAYACRD,
             a.RMNGPRNCPL = o.RMNGPRNCPL,
             a.DCSYL      = o.DCSYL
    WHEN NOT MATCHED THEN
      INSERT
        (a.STKCODE,
         a.MARKET,
         a.RELIBITY,
         a.STKNAME,
         a.VALTDATE,
         a.LISTGMKTCD,
         a.LISTGMKT,
         a.YRSTOMTRTY,
         a.VALTDDRTYP,
         a.ACRDINTRST,
         a.VALTDCLNPR,
         a.VALTDYLD,
         a.VALTDMODFD,
         a.VALTDCONEX,
         a.VALTDPVBP,
         a.VALTDSPRDD,
         a.VALTDSPRDC,
         a.MKTDRTYPRI,
         a.MKTCLNPRIC,
         a.MKTYLD,
         a.MKTMODFDDR,
         a.MKTCONXTY,
         a.MKTPVBP,
         a.MKTSPRDDRT,
         a.MKTSPRDCON,
         a.VALTDRATED,
         a.VALTDRATEC,
         a.MKTATEDRT,
         a.MKTRATECON,
         a.ENDDAYYALT,
         a.ENDDAYACRD,
         a.RMNGPRNCPL,
         a.DCSYL,
         a.lastdate)
      values
        (o.STKCODE,
         o.MARKET,
         o.RELIBITY,
         o.STKNAME,
         o.VALTDATE,
         o.LISTGMKTCD,
         o.LISTGMKT,
         o.YRSTOMTRTY,
         o.VALTDDRTYP,
         o.ACRDINTRST,
         o.VALTDCLNPR,
         o.VALTDYLD,
         o.VALTDMODFD,
         o.VALTDCONEX,
         o.VALTDPVBP,
         o.VALTDSPRDD,
         o.VALTDSPRDC,
         o.MKTDRTYPRI,
         o.MKTCLNPRIC,
         o.MKTYLD,
         o.MKTMODFDDR,
         o.MKTCONXTY,
         o.MKTPVBP,
         o.MKTSPRDDRT,
         o.MKTSPRDCON,
         o.VALTDRATED,
         o.VALTDRATEC,
         o.MKTATEDRT,
         o.MKTRATECON,
         o.ENDDAYYALT,
         o.ENDDAYACRD,
         o.RMNGPRNCPL,
         o.DCSYL,
         o.lastdate);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    c_sql := 'delete from RDT_SR_BONDCBVALUATION t where t.ENDDATE in (select a.VALTDATE from T_ODS_PMS_BONDESTIMATE a ) ';
    execute immediate c_sql;
    insert into RDT_SR_BONDCBVALUATION
      (STKID,
       ENDDATE,
       MARKET,
       ACCRUINTEREST,
       VALUEFULLPRICE,
       VALUECLEANPRICE,
       VPYIELD,
       VPADURATION,
       VPCONVEXITY,
       VPPOINTVALUE,
       VPINTERESTDURATION,
       VPINTERESTCONVEXITY,
       VPSPREADDURATION,
       VPSPREADCONVEXITY,
       TRUEFULLPRICE,
       TRUECLEANPRICE,
       TRUEYIELD,
       TRUEREMAINMATURITY,
       TRUEADURATION,
       TRUECONVEXITY,
       TRUEPOINTVALUE,
       TRUEINTERESTDURATION,
       TRUEINTERESTCONVEXITY,
       TRUESPREADDURATION,
       TRUESPREADCONVEXITY,
       ABLIQCOEFFICIENT,
       POSITIONPERCENT,
       RELATIVELIQCOEFFICIENT,
       RELATIVELIQNUM,
       SETTFULLPRICE,
       SETTACCRUINTEREST,
       RESIDUALCAPITAL,
       POINTSPREADYIELD,
       ESTIMATEDALLOCATINGR,
       DATASOURCE,
       INSERTBY,
       INSERTTIME,
       UPDATEBY,
       UPDATETIME)
      select pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
             t.stkcode as STKID,
             t.VALTDATE ENDDATE, --估值日期
             to_number(pkg_pub.f_get_dictmap('rdt',
                                             '0',
                                             '***',
                                             '30',
                                             t.market)) MARKET, --市场
             t.ACRDINTRST, --应计利息
             t.VALTDDRTYP, --估值全价
             t.VALTDCLNPR, --估值净价
             t.VALTDYLD, --估值收益率
             t.VALTDMODFD, --估值修正久期
             t.VALTDCONEX, --估值凸性
             t.VALTDPVBP, --估值基点价值
             t.VALTDRATED VPINTERESTDURATION, --估值利率久期
             t.VALTDRATEC VPINTERESTCONVEXITY, --估值利率凸性
             t.VALTDSPRDD, --估值利差久期
             t.VALTDSPRDC, --估值利差凸性
             t.MKTDRTYPRI as TRUEFULLPRICE, --真实全价
             t.MKTCLNPRIC as TRUECLEANPRICE, --真实净价
             t.MKTYLD as TRUEYIELD, --真实收益率
             t.YRSTOMTRTY as TRUEREMAINMATURITY, --代偿期
             t.MKTMODFDDR as TRUEADURATION, --真实修正久期
             t.MKTCONXTY as TRUECONVEXITY, --真实凸性
             t.MKTPVBP as TRUEPOINTVALUE, --真实基点价值
             t.MKTATEDRT as TRUEINTERESTDURATION, --真实利率久期
             t.MKTRATECON as TRUEINTERESTCONVEXITY, --真实利率凸性
             t.MKTSPRDDRT as TRUESPREADDURATION, --真实利差久期
             t.MKTSPRDCON as TRUESPREADCONVEXITY, --真实利差凸性
             0 as ABLIQCOEFFICIENT, --绝对流动性系数
             0 as POSITIONPERCENT, --位置百分比
             0 as RELATIVELIQCOEFFICIENT, --相对流动性系数
             0 as RELATIVELIQNUM, --相对流动性取值
             t.ENDDAYYALT as SETTFULLPRICE, --日终估值全价
             t.ENDDAYACRD as SETTACCRUINTEREST, --日终应计利息
             t.RMNGPRNCPL as RESIDUALCAPITAL, --剩余本金
             0 as POINTSPREADYIELD, --点差收益率（%）
             0 as ESTIMATEDALLOCATINGR, --估算的行权后票面利率
             'tz' as ATASOURCE, --数据来源
             'admin' as INSERTBY, --插入人
             t.VALTDATE as INSERTTIME, --插入时间
             'admin' as UPDATEBY, --更新人
             t.VALTDATE as UPDATETIME --更新时间
        from t_ods_pms_bondestimate t
       where t.RELIBITY = '1'
         and rowid in(select max(rowid) from t_ods_pms_bondestimate o
          group by o.stkcode,o.market,o.valtdate);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_bondestimate;

  /******************************************************************
    *功能模块： 中证估值表|采集数据清洗
    *功能描述：
    *创建人：yix
    *创建时间：2019年7月15日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_bondvalue(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    --更新
    MERGE INTO ODS_PMS_BONDVALUE a
    USING (select VALUEDATE,
                  STKCODE,
                  MARKET,
                  CALCULATIONPRICE,
                  YIELDTOMATURITY,
                  MODIFIEDDURATION,
                  CONVEXITY,
                  CLEANPRICE,
                  ACCRUEDINTEREST,
                  RESERVE,
                  busidate
             from T_ODS_PMS_BONDVALUE t) o
    on (a.VALUEDATE = o.VALUEDATE and a.STKCODE = o.STKCODE and a.MARKET = o.MARKET
        and a.busidate = o.busidate)
    WHEN MATCHED THEN
      UPDATE
         set a.CALCULATIONPRICE = o.CALCULATIONPRICE,
             a.YIELDTOMATURITY  = o.YIELDTOMATURITY,
             a.MODIFIEDDURATION = o.MODIFIEDDURATION,
             a.CONVEXITY        = o.CONVEXITY,
             a.CLEANPRICE       = o.CLEANPRICE,
             a.ACCRUEDINTEREST  = o.ACCRUEDINTEREST,
             a.RESERVE          = o.RESERVE
    WHEN NOT MATCHED THEN
      INSERT
        (a.VALUEDATE,
         a.STKCODE,
         a.MARKET,
         a.CALCULATIONPRICE,
         a.YIELDTOMATURITY,
         a.MODIFIEDDURATION,
         a.CONVEXITY,
         a.CLEANPRICE,
         a.ACCRUEDINTEREST,
         a.RESERVE,
         a.busidate)
      values
        (o.VALUEDATE,
         o.STKCODE,
         o.MARKET,
         o.CALCULATIONPRICE,
         o.YIELDTOMATURITY,
         o.MODIFIEDDURATION,
         o.CONVEXITY,
         o.CLEANPRICE,
         o.ACCRUEDINTEREST,
         o.RESERVE,
         o.busidate);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    c_sql := 'delete from RDT_SR_BONDCSIVALUATION t where t.ENDDATE in (select a.VALUEDATE from T_ODS_PMS_BONDVALUE a ) ';
    execute immediate c_sql;

    insert into RDT_SR_BONDCSIVALUATION
      (STKID,
       ENDDATE,
       VALUEFULLPRICE,
       VPYIELD,
       VPADURATION,
       VPCONVEXITY,
       VALUECLEANPRICE,
       ACCRUINTEREST,
       DATASOURCE,
       INSERTBY,
       INSERTTIME,
       UPDATEBY,
       UPDATETIME)
      select pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
             t.stkcode as STKID,
             t.VALUEDATE as ENDDATE, --日期
             t.CALCULATIONPRICE as VALUEFULLPRICE, --计算价格
             t.YIELDTOMATURITY as VPYIELD, --计算收益率
             t.MODIFIEDDURATION as VPADURATION, --修正久期
             t.CONVEXITY as VPCONVEXITY, --凸性
             t.CLEANPRICE as VALUECLEANPRICE, --净价
             t.ACCRUEDINTEREST as ACCRUINTEREST, --应计利息
             '  ' as ATASOURCE, --数据来源
             'admin' as INSERTBY, --插入人
             t.VALUEDATE as INSERTTIME, --插入时间
             'admin' as UPDATEBY, --更新人
             t.VALUEDATE as UPDATETIME --更新时间
        from T_ODS_PMS_BONDVALUE t
        where rowid in(select max(rowid) from t_ods_pms_bondvalue o
         group by o.stkcode,o.market,o.valuedate);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_bondvalue;

  /******************************************************************
    *功能模块： 发行人信息表|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年7月19日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_issuer(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ODS_PMS_ISSUER';
    execute immediate c_sql;

    insert into ODS_PMS_ISSUER
      select * from T_ODS_PMS_ISSUER;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    --转储到RDT_SR_SEC_ISSUER
    --来源表:
    MERGE INTO rdt_sr_sec_issuer t1
    USING (select t2.issuerid,
                  t2.issuername,
                  t2.issuerasset,
                  t2.isstotalassers,
                  t2.issuerlocation,
                  t2.outappraise,
                  t2.fullname
             from (select issuerid,issuername,issuerasset,
                          isstotalassers,issuerlocation,
                          outappraise,fullname,
                          row_number() over(partition by issuerid order by busidate desc) rnt
                   from ods_pms_issuer)t2
                   where t2.rnt=1) t3
    on (t1.issuerid = t3.issuerid)
    WHEN MATCHED THEN
      UPDATE
         set t1.issuername     = t3.issuername,
             t1.issuernvaamt   = t3.issuerasset,
             t1.issueramt      = t3.isstotalassers,
             t1.governmentarea = t3.issuerlocation,
             t1.ratecode       = t3.outappraise
    WHEN NOT MATCHED THEN
      INSERT
        (t1.issuerid,
         t1.issuername,
         t1.issuernvaamt,
         t1.issueramt,
         t1.governmentarea,
         t1.ratecode,
         t1.datasource,
         t1.insertby,
         t1.inserttime,
         t1.updateby,
         t1.updatetime)
      values
        (t3.issuerid,
         t3.issuername,
         t3.issuerasset,
         t3.isstotalassers,
         t3.issuerlocation,
         t3.outappraise,
         'tz',
         '8888',
         SRF_FORMATSYSDATE,
         '8888',
         SRF_FORMATSYSDATE);

    --转储到RDT_SR_SEC_GRADE(主体评级)
    MERGE INTO rdt_sr_sec_grade t1
    USING (select t2.stkid,
                  p1.busidate gradeday,
                  '16' gradetype,
                  ' ' grader,
                  pkg_pub.f_get_dictmap('rdt','0','***','pjzh',p1.outappraise) mainappraise
             from ods_pms_stock t2,t_ods_pms_issuer p1
              where t2.issuerid = p1.issuerid) t3
    on (t1.stkid = t3.stkid and t1.gradetype = t3.gradetype and t1.gradeday = t3.gradeday
        and t1.grader = t3.grader)
    WHEN MATCHED THEN
      UPDATE
         set t1.gradecode  = t3.mainappraise,
             t1.updateby   = '8888',
             t1.updatetime = SRF_FORMATSYSDATE
    WHEN NOT MATCHED THEN
      INSERT
        (t1.stkid,
         t1.gradeday,
         t1.gradetype,
         t1.grader,
         t1.gradecode,
         t1.datasource,
         t1.insertby,
         t1.inserttime,
         t1.updateby,
         t1.updatetime)
      values
        (t3.stkid,
         t3.gradeday,
         t3.gradetype,
         t3.grader,
         t3.mainappraise,
         'tz',
         '8888',
         SRF_FORMATSYSDATE,
         '8888',
         SRF_FORMATSYSDATE);

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_issuer;

  /******************************************************************
    *功能模块： 证券分类表|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年7月19日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_stkclass(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ODS_PMS_STKCLASS';
    execute immediate c_sql;

    insert into ODS_PMS_STKCLASS
      select * from T_ODS_PMS_STKCLASS;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_stkclass;

  /******************************************************************
    *功能模块： 证券发行信息表|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年7月19日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_stkissueinfo(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ODS_PMS_STKISSUEINFO';
    execute immediate c_sql;

    insert into ODS_PMS_STKISSUEINFO
      select * from T_ODS_PMS_STKISSUEINFO;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_stkissueinfo;
  /******************************************************************
    *功能模块： 债券评级|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年7月19日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_bondappraise(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ODS_PMS_BONDAPPRAISE';
    execute immediate c_sql;

    insert into ODS_PMS_BONDAPPRAISE
      select * from T_ODS_PMS_BONDAPPRAISE;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_bondappraise;

  /******************************************************************
    *功能模块： 证券池股票明细|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年7月19日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_stkpooldetail(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ODS_PMS_STKPOOLDETAIL';
    execute immediate c_sql;

    insert into ODS_PMS_STKPOOLDETAIL
      select * from T_ODS_PMS_STKPOOLDETAIL;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_stkpooldetail;

  /******************************************************************
    *功能模块： 财汇接口权益信息表|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年7月19日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_finchina_right(errcode out int,
                                       errmsg  out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ODS_PMS_FINCHINA_RIGHT';
    execute immediate c_sql;

    insert into ODS_PMS_FINCHINA_RIGHT
      select * from T_ODS_PMS_FINCHINA_RIGHT;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_finchina_right;
  /******************************************************************
    *功能模块： 场外基金申赎参数|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年7月19日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_investfundinfo(errcode out int,
                                       errmsg  out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ODS_PMS_INVESTFUNDINFO';
    execute immediate c_sql;

    insert into ODS_PMS_INVESTFUNDINFO
      select * from T_ODS_PMS_INVESTFUNDINFO;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_investfundinfo;

  /******************************************************************
    *功能模块： 新股上市信息表|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年7月19日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_par_newstock_info(errcode out int,
                                          errmsg  out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    c_sql := 'truncate table ODS_PMS_PAR_NEWSTOCK_INFO';
    execute immediate c_sql;

    insert into ODS_PMS_PAR_NEWSTOCK_INFO
      select * from T_ODS_PMS_PAR_NEWSTOCK_INFO;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_par_newstock_info;

  /******************************************************************
    *功能模块： 期货合约信息|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年9月26日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_ft_stkinfo(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    update t_pms_ft_stkinfo t
       set t.stkid = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', trim(t.market)) ||
                     t.stkcode;

    update t_pms_ft_stkinfo t
       set t.market = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', trim(t.market));
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    c_sql := 'truncate table ods_pms_ft_stkinfo';
    execute immediate c_sql;

    insert into ods_pms_ft_stkinfo
      select * from t_pms_ft_stkinfo;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    --rdt_bsc_ftstkinfo
    --来源表：ods_pms_ft_stkinfo,ods_pms_STOCK
    merge into rdt_bsc_ftstkinfo t
    using (select  t2.stkid,
       t2.market,
       t2.productid,
       t2.stkcode,
       t4.stkname stkname,
       nvl(srf_trans_sectype3(t4.stktype),' ') stktype,
       '' curcode,
       t2.convertunit convertunit,
       '' delivmode,
       '' deliverydate,
       t2.expiredate lasttradingdate,
       '' lastdeliverydate,
       'tz' datasource,
       '8888' updateby,
       to_char(sysdate,'yyyyMMdd') updatetime,
       '' littlestchangeunit,
       0 contractinnercode
       from ods_pms_ft_stkinfo t2
       left join ODS_PMS_STOCK t4 on t2.stkid=t4.stkid) t3
       on (t.stkcode=t3.stkcode and t.market=t3.market and t.productid=t3.productid)
       when matched then
       update set
       t.stkname=t3.stkname,
       t.stktype=t3.stktype,
       t.curcode=t3.curcode,
       t.convertunit=t3.convertunit,
       t.lasttradingdate=t3.lasttradingdate,
       t.updatetime=t3.updatetime
       when not matched then
       insert(
       stkid,
       market,
       productid,
       stkcode,
       stkname,
       stktype,
       curcode,
       convertunit,
       delivmode,
       deliverydate,
       lasttradingdate,
       lastdeliverydate,
       datasource,
       updateby,
       updatetime,
       littlestchangeunit,
       contractinnercode
       )
       values(
       t3.stkid,
       t3.market,
       t3.productid,
       t3.stkcode,
       t3.stkname,
       t3.stktype,
       t3.curcode,
       t3.convertunit,
       t3.delivmode,
       t3.deliverydate,
       t3.lasttradingdate,
       t3.lastdeliverydate,
       t3.datasource,
       t3.updateby,
       t3.updatetime,
       t3.littlestchangeunit,
       t3.contractinnercode
       );

      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_ft_stkinfo;

  /******************************************************************
    *功能模块： 期货品种|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年9月26日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_ft_product(errcode out int,
                                          errmsg  out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

    update t_pms_ft_product t
       set t.market = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', trim(t.market));
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    c_sql := 'truncate table ods_pms_ft_product';
    execute immediate c_sql;

    insert into ods_pms_ft_product
      select * from t_pms_ft_product;
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    --rdt_bsc_ftproduct
    --来源表：ods_pms_ft_stkinfo,ods_pms_STOCK
    merge into rdt_bsc_ftproduct t
    using (
    select distinct t2.market,
    t2.productid,
    t2.productname,
    t4.stktype,
    t2.defaultpricetype,
    t2.upperlimitrate,
    t2.downlimitrate,
    t2.delivmode,
    t2.delivmonthflag,
    t2.volmultiple,
    t2.underlyingmultiple,
    t2.pricetick,
    t2.mindelivqty,
    t2.measurename,
    t2.quotationname,
    t2.maxlimitorderqty,
    t2.minlimitorderqty,
    t2.cancelflag,
    t2.marketpriceflag,
    t2.status,
    t2.pricechg,
    t2.closetdfeetype,
    t2.closetdamtrate,
    t2.closetdqtyrate,
    t2.tradetimekind
    from ods_pms_ft_product t2
    left join ods_pms_ft_stkinfo t3 on t2.productid=t3.productid
    left join ods_pms_stock t4 on t3.stkid=t4.stkid
    ) t5
    on (t.market=t5.market and t.productid=t5.productid)
    when matched then
    update set
    t.productname=t5.productname,
    t.stktype=t5.stktype,
    t.defaultpricetype=t5.defaultpricetype,
    t.upperlimitrate=t5.upperlimitrate,
    t.downlimitrate=t5.downlimitrate,
    t.delivmode=t5.delivmode,
    t.delivmonthflag=t5.delivmonthflag,
    t.volmultiple=t5.volmultiple,
    t.underlyingmultiple=t5.underlyingmultiple,
    t.pricetick=t5.pricetick,
    t.mindelivqty=t5.mindelivqty,
    t.measurename=t5.measurename,
    t.quotationname=t5.quotationname,
    t.maxlimitorderqty=t5.maxlimitorderqty,
    t.minlimitorderqty=t5.minlimitorderqty,
    t.cancelflag=t5.cancelflag,
    t.marketpriceflag=t5.marketpriceflag,
    t.status=t5.status,
    t.pricechg=t5.pricechg,
    t.closetdfeetype=t5.closetdfeetype,
    t.closetdamtrate=t5.closetdamtrate,
    t.closetdqtyrate=t5.closetdqtyrate,
    t.tradetimekind=t5.tradetimekind
    when not matched then
    insert (
    t.market,
    t.productid,
    t.productname,
    t.stktype,
    t.defaultpricetype,
    t.upperlimitrate,
    t.downlimitrate,
    t.delivmode,
    t.delivmonthflag,
    t.volmultiple,
    t.underlyingmultiple,
    t.pricetick,
    t.mindelivqty,
    t.measurename,
    t.quotationname,
    t.maxlimitorderqty,
    t.minlimitorderqty,
    t.cancelflag,
    t.marketpriceflag,
    t.status,
    t.pricechg,
    t.closetdfeetype,
    t.closetdamtrate,
    t.closetdqtyrate,
    t.tradetimekind
    )values
    (
    t5.market,
    t5.productid,
    t5.productname,
    t5.stktype,
    t5.defaultpricetype,
    t5.upperlimitrate,
    t5.downlimitrate,
    t5.delivmode,
    t5.delivmonthflag,
    t5.volmultiple,
    t5.underlyingmultiple,
    t5.pricetick,
    t5.mindelivqty,
    t5.measurename,
    t5.quotationname,
    t5.maxlimitorderqty,
    t5.minlimitorderqty,
    t5.cancelflag,
    t5.marketpriceflag,
    t5.status,
    t5.pricechg,
    t5.closetdfeetype,
    t5.closetdamtrate,
    t5.closetdqtyrate,
    t5.tradetimekind
    )
    ;
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_ft_product;

  /******************************************************************
    *功能模块： 行情库|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年10月09日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_stkprice(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

        update t_pms_stkprice t
       set t.stkid = pkg_pub.f_get_dictmap('rdt', '0', '***', '30', t.market) ||
                     t.stkcode;

        insert into ods_pms_stkprice
      select *
        from t_pms_stkprice t1
       where not exists (select 1
                from ods_pms_stkprice t2
               where t1.market = t2.market
                 and t1.busidate = t2.busidate
                 and t1.stkcode = t2.stkcode);
        	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_stkprice;
  /******************************************************************
    *功能模块： 期货保证金设置|采集数据清洗
    *功能描述：
    *创建人：huxin
    *创建时间：2019年10月09日
    *修改人：
    *修改时间：
    *修改原因:
  *****************************************************************/
  procedure etl_ods_pms_ft_margin(errcode out int, errmsg out varchar2) is
    c_sql varchar2(4000);
    V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; 
    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; 
    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; 
    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; 
    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; 
    begin
    --程序开始执行 
    V_LOG_POINT := '001'; 
    V_MSG       := '程序开始执行'; 
    DATA_COLLECTING_LOG(V_PROC_NAME, 
                        V_LOG_POINT, 
                        V_MSG, 
                        V_LOG_FLAG, 
                        V_LOADROW);
    errcode := 0;
    errmsg  := 'OK';

        insert into ods_pms_ft_margin
      select *
        from t_pms_ft_margin t1
       where not exists (select 1
                from ods_pms_ft_margin t2
               where t1.fundid = t2.fundid
                 and t1.stkid = t2.stkid
                 and t1.seat = t2.seat);
        	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    exception
        when others then
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 1;
      errmsg  := sqlcode || ' ' || sqlerrm;
  end etl_ods_pms_ft_margin;
end pkg_etl_data_pms;
/
