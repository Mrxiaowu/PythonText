create or replace package body pkg_etl_data_mis is
  
  --大额存单概况
  procedure cj_MIS_BONDBADEPOSITI(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ods_MIS_BONDBADEPOSITI t2
    USING t_MIS_BONDBADEPOSITI t
    ON (t.INNERCODE = t2.INNERCODE and t.UPDATETIME = t2.UPDATETIME)
    --UPDATE
    --SET T2.AMT=t.AMT,T2.BAMT=t.BAMT,T2.QTY=t.QTY,T2.PRICE=t.PRICE
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.INFOPUBLDATE,
         t2.DEPOSITRCODE,
         t2.DISCIPMECHANCODE,
         t2.MATURITYTYPE,
         t2.DEPOSITRMATURITY,
         t2.VALUEDATE,
         t2.MATURITYDATE,
         t2.CURRENCYUNIT,
         t2.ISSUEWAY,
         t2.ISSUEOBJECT,
         t2.ISSUECHANNEL,
         t2.PLANNEDISSUEVOL,
         t2.ISSUEBEGINDATE,
         t2.ISSUEBEGINTIME,
         t2.ISSUEENDDATE,
         t2.ISSUEENDTIME,
         t2.SUBDEPOSITVALUE,
         t2.MINCHANGEUNIT,
         t2.COMPOUNDMETHOD,
         t2.RATE,
         t2.INTPAYMENTMETHOD,
         t2.INTERESTFORMULA,
         t2.EARLYMATURITYDATE,
         t2.REDEMPTIONDATE,
         t2.ACTUALSUBAMOUNT,
         t2.REMARK,
         t2.UPDATETIME,
         t2.JSID)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.INFOPUBLDATE,
         t.DEPOSITRCODE,
         t.DISCIPMECHANCODE,
         t.MATURITYTYPE,
         t.DEPOSITRMATURITY,
         t.VALUEDATE,
         t.MATURITYDATE,
         t.CURRENCYUNIT,
         t.ISSUEWAY,
         t.ISSUEOBJECT,
         t.ISSUECHANNEL,
         t.PLANNEDISSUEVOL,
         t.ISSUEBEGINDATE,
         t.ISSUEBEGINTIME,
         t.ISSUEENDDATE,
         t.ISSUEENDTIME,
         t.SUBDEPOSITVALUE,
         t.MINCHANGEUNIT,
         t.COMPOUNDMETHOD,
         t.RATE,
         t.INTPAYMENTMETHOD,
         t.INTERESTFORMULA,
         t.EARLYMATURITYDATE,
         t.REDEMPTIONDATE,
         t.ACTUALSUBAMOUNT,
         t.REMARK,
         t.UPDATETIME,
         t.JSID);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_BONDBADEPOSITI;

  --债券信用评级
  procedure cj_MIS_BONDCREDITGRADE(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_BONDCREDITGRADE t2
    USING t_MIS_BONDCREDITGRADE t
    ON (t.MAINCODE = t2.MAINCODE and t.INFOPUBLDATE = t2.INFOPUBLDATE and t.CRASCODE = t2.CRASCODE and t.CRDATE = t2.CRDATE)
    --UPDATE
    --SET T2.AMT=t.AMT,T2.BAMT=t.BAMT,T2.QTY=t.QTY,T2.PRICE=t.PRICE
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.MAINCODE,
         t2.BONDISSUER,
         t2.BICODE,
         t2.INFOPUBLDATE,
         t2.INFOSOURCE,
         t2.CRAS,
         t2.CRASCODE,
         t2.CRDATE,
         t2.STCR,
         t2.STCRCODE,
         t2.STCRREMARK,
         t2.LTCR,
         t2.LTCRCODE,
         t2.LTCRREMARK,
         t2.CRANTICIPATE,
         t2.CRSTATUS,
         t2.UPDATETIME,
         t2.JSID,
         t2.TCRTYPE,
         t2.BONDISSUERCR,
         t2.BONDISSUERCRCODE,
         t2.CRSYSTEM,
         t2.GUARANTORNAME,
         t2.GUARANTORCODE,
         t2.GUARANTORCR,
         t2.GUARANTORCRCODE,
         t2.BDCRDATE,
         t2.BDCRAS,
         t2.BDCRASCODE,
         t2.BICRDATE,
         t2.BICRAS,
         t2.BICRASCODE,
         t2.BICRSYSTEM,
         t2.BICRANTICIPATE,
         t2.BICRSTATUS,
         t2.BGCRDATE,
         t2.BGCRAS,
         t2.BGCRASCODE,
         t2.BGCRSYSTEM,
         t2.BGCRANTICIPATE,
         t2.BGCRSTATUS,
         t2.CROUTLOOK,
         t2.BICROUTLOOK,
         t2.BGCRROUTLOOK,
         t2.RATEMETHOD)
      VALUES
        (t.ID,
         t.MAINCODE,
         t.BONDISSUER,
         t.BICODE,
         t.INFOPUBLDATE,
         t.INFOSOURCE,
         t.CRAS,
         t.CRASCODE,
         t.CRDATE,
         t.STCR,
         t.STCRCODE,
         t.STCRREMARK,
         t.LTCR,
         t.LTCRCODE,
         t.LTCRREMARK,
         t.CRANTICIPATE,
         t.CRSTATUS,
         t.UPDATETIME,
         t.JSID,
         t.TCRTYPE,
         t.BONDISSUERCR,
         t.BONDISSUERCRCODE,
         t.CRSYSTEM,
         t.GUARANTORNAME,
         t.GUARANTORCODE,
         t.GUARANTORCR,
         t.GUARANTORCRCODE,
         t.BDCRDATE,
         t.BDCRAS,
         t.BDCRASCODE,
         t.BICRDATE,
         t.BICRAS,
         t.BICRASCODE,
         t.BICRSYSTEM,
         t.BICRANTICIPATE,
         t.BICRSTATUS,
         t.BGCRDATE,
         t.BGCRAS,
         t.BGCRASCODE,
         t.BGCRSYSTEM,
         t.BGCRANTICIPATE,
         t.BGCRSTATUS,
         t.CROUTLOOK,
         t.BICROUTLOOK,
         t.BGCRROUTLOOK,
         t.RATEMETHOD);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_BONDCREDITGRADE;

  --债券要素表
  procedure cj_MIS_BONDBASICINFO(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_BONDBASICINFO t2
    USING t_MIS_BONDBASICINFO t
    ON (t.MAINCODE = t2.MAINCODE)
    WHEN MATCHED THEN
      UPDATE
         SET t2.innercode              = t.innercode,
             t2.bondfullname           = t.bondfullname,
             t2.bondform               = t.bondform,
             t2.bondnature             = t.bondnature,
             t2.optiontype             = t.optiontype,
             t2.ifstrips               = t.ifstrips,
             t2.issuer                 = t.issuer,
             t2.issuernature           = t.issuernature,
             t2.guarantor              = t.guarantor,
             t2.leadunderwriter        = t.leadunderwriter,
             t2.initialinfopubldate    = t.initialinfopubldate,
             t2.creditrating           = t.creditrating,
             t2.cras                   = t.cras,
             t2.actualissuesize        = t.actualissuesize,
             t2.maturity               = t.maturity,
             t2.parvalue               = t.parvalue,
             t2.issueprice             = t.issueprice,
             t2.currencyunit           = t.currencyunit,
             t2.issuerefytm            = t.issuerefytm,
             t2.compoundmethod         = t.compoundmethod,
             t2.intpaymentmethod       = t.intpaymentmethod,
             t2.pmremark               = t.pmremark,
             t2.couponrate             = t.couponrate,
             t2.minannualrate          = t.minannualrate,
             t2.frnrefrateper          = t.frnrefrateper,
             t2.frnrefrateselecrremark = t.frnrefrateselecrremark,
             t2.frnmargin              = t.frnmargin,
             t2.payinteresteffency     = t.payinteresteffency,
             t2.biddate                = t.biddate,
             t2.issuedate              = t.issuedate,
             t2.transferdate           = t.transferdate,
             t2.valuedate              = t.valuedate,
             t2.regdate                = t.regdate,
             t2.listeddate             = t.listeddate,
             t2.delistdate             = t.delistdate,
             t2.enddate                = t.enddate,
             t2.redemptiondate         = t.redemptiondate,
             t2.remark                 = t.remark,
             t2.creatorg               = t.creatorg,
             t2.abslevel               = t.abslevel,
             t2.abslevelratio          = t.abslevelratio,
             t2.interestcap            = t.interestcap,
             t2.updatetime             = t.updatetime,
             t2.jsid                   = t.jsid,
             t2.multiguarantor         = t.multiguarantor,
             t2.sponsor                = t.sponsor,
             t2.interestitem           = t.interestitem,
             t2.interestformula        = t.interestformula,
             t2.rateiffloat            = t.rateiffloat,
             t2.rateadjustmode         = t.rateadjustmode,
             t2.custodycode            = t.custodycode,
             t2.redemptionmethod       = t.redemptionmethod,
             t2.earlymaturitydate      = t.earlymaturitydate,
             t2.initialinvestornum     = t.initialinvestornum,
             t2.privateinvestornum     = t.privateinvestornum,
             t2.interestformulahs      = t.interestformulahs,
             t2.basicassettype         = t.basicassettype,
             t2.abslevely              = t.abslevely,
             t2.opmaturity             = t.opmaturity,
             t2.primalpricingdate      = t.primalpricingdate,
             t2.legalenddate           = t.legalenddate
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.MAINCODE,
         t2.INNERCODE,
         t2.BONDFULLNAME,
         t2.BONDFORM,
         t2.BONDNATURE,
         t2.OPTIONTYPE,
         t2.IFSTRIPS,
         t2.ISSUER,
         t2.ISSUERNATURE,
         t2.GUARANTOR,
         t2.LEADUNDERWRITER,
         t2.INITIALINFOPUBLDATE,
         t2.CREDITRATING,
         t2.CRAS,
         t2.ACTUALISSUESIZE,
         t2.MATURITY,
         t2.PARVALUE,
         t2.ISSUEPRICE,
         t2.CURRENCYUNIT,
         t2.ISSUEREFYTM,
         t2.COMPOUNDMETHOD,
         t2.INTPAYMENTMETHOD,
         t2.PMREMARK,
         t2.COUPONRATE,
         t2.MINANNUALRATE,
         t2.FRNREFRATEPER,
         t2.FRNREFRATESELECRREMARK,
         t2.FRNMARGIN,
         t2.PAYINTERESTEFFENCY,
         t2.BIDDATE,
         t2.ISSUEDATE,
         t2.TRANSFERDATE,
         t2.VALUEDATE,
         t2.REGDATE,
         t2.LISTEDDATE,
         t2.DELISTDATE,
         t2.ENDDATE,
         t2.REDEMPTIONDATE,
         t2.REMARK,
         t2.CREATORG,
         t2.ABSLEVEL,
         t2.ABSLEVELRATIO,
         t2.INTERESTCAP,
         t2.UPDATETIME,
         t2.JSID,
         t2.MULTIGUARANTOR,
         t2.SPONSOR,
         t2.INTERESTITEM,
         t2.INTERESTFORMULA,
         t2.RATEIFFLOAT,
         t2.RATEADJUSTMODE,
         t2.CUSTODYCODE,
         t2.REDEMPTIONMETHOD,
         t2.EARLYMATURITYDATE,
         t2.INITIALINVESTORNUM,
         t2.PRIVATEINVESTORNUM,
         t2.INTERESTFORMULAHS,
         t2.BASICASSETTYPE,
         t2.ABSLEVELY,
         t2.OPMATURITY,
         t2.PRIMALPRICINGDATE,
         t2.LEGALENDDATE)
      VALUES
        (t.ID,
         t.MAINCODE,
         t.INNERCODE,
         t.BONDFULLNAME,
         t.BONDFORM,
         t.BONDNATURE,
         t.OPTIONTYPE,
         t.IFSTRIPS,
         t.ISSUER,
         t.ISSUERNATURE,
         t.GUARANTOR,
         t.LEADUNDERWRITER,
         t.INITIALINFOPUBLDATE,
         t.CREDITRATING,
         t.CRAS,
         t.ACTUALISSUESIZE,
         t.MATURITY,
         t.PARVALUE,
         t.ISSUEPRICE,
         t.CURRENCYUNIT,
         t.ISSUEREFYTM,
         t.COMPOUNDMETHOD,
         t.INTPAYMENTMETHOD,
         t.PMREMARK,
         t.COUPONRATE,
         t.MINANNUALRATE,
         t.FRNREFRATEPER,
         t.FRNREFRATESELECRREMARK,
         t.FRNMARGIN,
         t.PAYINTERESTEFFENCY,
         t.BIDDATE,
         t.ISSUEDATE,
         t.TRANSFERDATE,
         t.VALUEDATE,
         t.REGDATE,
         t.LISTEDDATE,
         t.DELISTDATE,
         t.ENDDATE,
         t.REDEMPTIONDATE,
         t.REMARK,
         t.CREATORG,
         t.ABSLEVEL,
         t.ABSLEVELRATIO,
         t.INTERESTCAP,
         t.UPDATETIME,
         t.JSID,
         t.MULTIGUARANTOR,
         t.SPONSOR,
         t.INTERESTITEM,
         t.INTERESTFORMULA,
         t.RATEIFFLOAT,
         t.RATEADJUSTMODE,
         t.CUSTODYCODE,
         t.REDEMPTIONMETHOD,
         t.EARLYMATURITYDATE,
         t.INITIALINVESTORNUM,
         t.PRIVATEINVESTORNUM,
         t.INTERESTFORMULAHS,
         t.BASICASSETTYPE,
         t.ABSLEVELY,
         t.OPMATURITY,
         t.PRIMALPRICINGDATE,
         t.LEGALENDDATE);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_BONDBASICINFO;

  --中债估值信息
  procedure cj_MIS_BONDCBVALUATION(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_BONDCBVALUATION t2
    USING t_MIS_BONDCBVALUATION t
    ON (t.InnerCode = t2.InnerCode and t.EndDate = t2.EndDate and t.UPDATETIME = t2.UPDATETIME)
    /*WHEN MATCHED THEN
                                            UPDATE
                                            SET
                                            t2.id = t.id,
                                            t2.innercode = t.innercode,
                                            t2.enddate = t.enddate,
                                            t2.exchange = t.exchange,
                                            t2.accruinterest = t.accruinterest,
                                            t2.valuefullprice = t.valuefullprice,
                                            t2.valuecleanprice = t.valuecleanprice,
                                            t2.vpyield = t.vpyield,
                                            t2.vpaduration = t.vpaduration,
                                            t2.vpconvexity = t.vpconvexity,
                                            t2.vppointvalue = t.vppointvalue,
                                            t2.vpinterestduration = t.vpinterestduration,
                                            t2.vpinterestconvexity = t.vpinterestconvexity,
                                            t2.vpspreadduration = t.vpspreadduration,
                                            t2.vpspreadconvexity = t.vpspreadconvexity,
                                            t2.truefullprice = t.truefullprice,
                                            t2.truecleanprice = t.truecleanprice,
                                            t2.trueyield = t.trueyield,
                                            t2.trueremainmaturity = t.trueremainmaturity,
                                            t2.trueaduration = t.trueaduration,
                                            t2.trueconvexity = t.trueconvexity,
                                            t2.truepointvalue = t.truepointvalue,
                                            t2.trueinterestduration = t.trueinterestduration,
                                            t2.trueinterestconvexity = t.trueinterestconvexity,
                                            t2.truespreadduration = t.truespreadduration,
                                            t2.truespreadconvexity = t.truespreadconvexity,
                                            t2.abliqcoefficient = t.abliqcoefficient,
                                            t2.positionpercent = t.positionpercent,
                                            t2.relativeliqcoefficient = t.relativeliqcoefficient,
                                            t2.relativeliqnum = t.relativeliqnum,
                                            t2.updatetime = t.updatetime,
                                            t2.jsid = t.jsid,
                                            t2.settfullprice = t.settfullprice,
                                            t2.settaccruinterest = t.settaccruinterest,
                                            t2.residualcapital = t.residualcapital,
                                            t2.pointspreadyield = t.pointspreadyield,
                                            t2.estimatedallocatingr = t.estimatedallocatingr*/
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.ENDDATE,
         t2.EXCHANGE,
         t2.ACCRUINTEREST,
         t2.VALUEFULLPRICE,
         t2.VALUECLEANPRICE,
         t2.VPYIELD,
         t2.VPADURATION,
         t2.VPCONVEXITY,
         t2.VPPOINTVALUE,
         t2.VPINTERESTDURATION,
         t2.VPINTERESTCONVEXITY,
         t2.VPSPREADDURATION,
         t2.VPSPREADCONVEXITY,
         t2.TRUEFULLPRICE,
         t2.TRUECLEANPRICE,
         t2.TRUEYIELD,
         t2.TRUEREMAINMATURITY,
         t2.TRUEADURATION,
         t2.TRUECONVEXITY,
         t2.TRUEPOINTVALUE,
         t2.TRUEINTERESTDURATION,
         t2.TRUEINTERESTCONVEXITY,
         t2.TRUESPREADDURATION,
         t2.TRUESPREADCONVEXITY,
         t2.ABLIQCOEFFICIENT,
         t2.POSITIONPERCENT,
         t2.RELATIVELIQCOEFFICIENT,
         t2.RELATIVELIQNUM,
         t2.UPDATETIME,
         t2.JSID,
         t2.SETTFULLPRICE,
         t2.SETTACCRUINTEREST,
         t2.RESIDUALCAPITAL,
         t2.POINTSPREADYIELD,
         t2.ESTIMATEDALLOCATINGR)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.ENDDATE,
         t.EXCHANGE,
         t.ACCRUINTEREST,
         t.VALUEFULLPRICE,
         t.VALUECLEANPRICE,
         t.VPYIELD,
         t.VPADURATION,
         t.VPCONVEXITY,
         t.VPPOINTVALUE,
         t.VPINTERESTDURATION,
         t.VPINTERESTCONVEXITY,
         t.VPSPREADDURATION,
         t.VPSPREADCONVEXITY,
         t.TRUEFULLPRICE,
         t.TRUECLEANPRICE,
         t.TRUEYIELD,
         t.TRUEREMAINMATURITY,
         t.TRUEADURATION,
         t.TRUECONVEXITY,
         t.TRUEPOINTVALUE,
         t.TRUEINTERESTDURATION,
         t.TRUEINTERESTCONVEXITY,
         t.TRUESPREADDURATION,
         t.TRUESPREADCONVEXITY,
         t.ABLIQCOEFFICIENT,
         t.POSITIONPERCENT,
         t.RELATIVELIQCOEFFICIENT,
         t.RELATIVELIQNUM,
         t.UPDATETIME,
         t.JSID,
         t.SETTFULLPRICE,
         t.SETTACCRUINTEREST,
         t.RESIDUALCAPITAL,
         t.POINTSPREADYIELD,
         t.ESTIMATEDALLOCATINGR);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_BONDCBVALUATION;

  --中证债券估值
  procedure cj_MIS_BONDCSIVALUATION(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_BONDCSIVALUATION t2
    USING t_MIS_BONDCSIVALUATION t
    ON (t.InnerCode = t2.InnerCode and t.EndDate = t2.EndDate)
    WHEN MATCHED THEN
      UPDATE
         SET t2.valuefullprice  = t.valuefullprice,
             t2.vpyield         = t.vpyield,
             t2.vpaduration     = t.vpaduration,
             t2.vpconvexity     = t.vpconvexity,
             t2.valuecleanprice = t.valuecleanprice,
             t2.accruinterest   = t.accruinterest,
             t2.updatetime      = t.updatetime,
             t2.jsid            = t.jsid
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.ENDDATE,
         t2.VALUEFULLPRICE,
         t2.VPYIELD,
         t2.VPADURATION,
         t2.VPCONVEXITY,
         t2.VALUECLEANPRICE,
         t2.ACCRUINTEREST,
         t2.UPDATETIME,
         t2.JSID)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.ENDDATE,
         t.VALUEFULLPRICE,
         t.VPYIELD,
         t.VPADURATION,
         t.VPCONVEXITY,
         t.VALUECLEANPRICE,
         t.ACCRUINTEREST,
         t.UPDATETIME,
         t.JSID);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_BONDCSIVALUATION;

  --三板做市商关联
  procedure cj_MIS_NQMKTMAKERELA(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_NQMKTMAKERELA t2
    USING t_MIS_NQMKTMAKERELA t
    ON (t.InnerCode = t2.InnerCode and t.MktMakeCode = t2.MktMakeCode and t.InDate = t2.InDate)
    WHEN MATCHED THEN
      UPDATE
         SET t2.infopubldate = t.infopubldate,
             t2.companycode  = t.companycode,
             t2.mktmakename  = t.mktmakename,
             t2.mktmakecode  = t.mktmakecode,
             t2.ifeffective  = t.ifeffective,
             t2.inserttime   = t.inserttime,
             t2.updatetime   = t.updatetime,
             t2.jsid         = t.jsid,
             t2.outdate      = t.outdate,
             t2.outreason    = t.outreason,
             t2.remark       = t.remark
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INFOPUBLDATE,
         t2.COMPANYCODE,
         t2.MKTMAKENAME,
         t2.MKTMAKECODE,
         t2.IFEFFECTIVE,
         t2.INSERTTIME,
         t2.UPDATETIME,
         t2.JSID,
         t2.INNERCODE,
         t2.INDATE,
         t2.OUTDATE,
         t2.OUTREASON,
         t2.REMARK)
      VALUES
        (t.ID,
         t.INFOPUBLDATE,
         t.COMPANYCODE,
         t.MKTMAKENAME,
         t.MKTMAKECODE,
         t.IFEFFECTIVE,
         t.INSERTTIME,
         t.UPDATETIME,
         t.JSID,
         t.INNERCODE,
         t.INDATE,
         t.OUTDATE,
         t.OUTREASON,
         t.REMARK);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_NQMKTMAKERELA;

  --三板挂牌公司名单
  procedure cj_MIS_NQCOMLIST(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_NQCOMLIST t2
    USING t_MIS_NQCOMLIST t
    ON (t.SecuCode = t2.SecuCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.secuabbr      = t.secuabbr,
             t2.innercode     = t.innercode,
             t2.transtype     = t.transtype,
             t2.industryname  = t.industryname,
             t2.industrycode  = t.industrycode,
             t2.ifeffective   = t.ifeffective,
             t2.inserttime    = t.inserttime,
             t2.updatetime    = t.updatetime,
             t2.jsid          = t.jsid,
             t2.prioritylevel = t.prioritylevel,
             t2.brokercode    = t.brokercode,
             t2.areacode      = t.areacode
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.SECUCODE,
         t2.SECUABBR,
         t2.INNERCODE,
         t2.TRANSTYPE,
         t2.INDUSTRYNAME,
         t2.INDUSTRYCODE,
         t2.IFEFFECTIVE,
         t2.INSERTTIME,
         t2.UPDATETIME,
         t2.JSID,
         t2.PRIORITYLEVEL,
         t2.BROKERCODE,
         t2.AREACODE)
      VALUES
        (t.ID,
         t.SECUCODE,
         t.SECUABBR,
         t.INNERCODE,
         t.TRANSTYPE,
         t.INDUSTRYNAME,
         t.INDUSTRYCODE,
         t.IFEFFECTIVE,
         t.INSERTTIME,
         t.UPDATETIME,
         t.JSID,
         t.PRIORITYLEVEL,
         t.BROKERCODE,
         t.AREACODE);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_NQCOMLIST;

  --债券代码对照表
  procedure cj_MIS_BOND_CODE(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_BOND_CODE t2
    USING t_MIS_BOND_CODE t
    ON (t.MainCode = t2.MainCode and t.InnerCode = t2.InnerCode and t.CompanyCode = t2.CompanyCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.secucode           = t.secucode,
             t2.secuabbr           = t.secuabbr,
             t2.chispelling        = t.chispelling,
             t2.chiname            = t.chiname,
             t2.chinameabbr        = t.chinameabbr,
             t2.engname            = t.engname,
             t2.engnameabbr        = t.engnameabbr,
             t2.currency           = t.currency,
             t2.secumarket         = t.secumarket,
             t2.listeddate         = t.listeddate,
             t2.listedsector       = t.listedsector,
             t2.listedstate        = t.listedstate,
             t2.delistdate         = t.delistdate,
             t2.issuer             = t.issuer,
             t2.issuernature       = t.issuernature,
             t2.bondnature         = t.bondnature,
             t2.updatetime         = t.updatetime,
             t2.jsid               = t.jsid,
             t2.interestenddate    = t.interestenddate,
             t2.bondtypelevel1     = t.bondtypelevel1,
             t2.bondtypelevel1desc = t.bondtypelevel1desc,
             t2.bondtypelevel2     = t.bondtypelevel2,
             t2.bondtypelevel2desc = t.bondtypelevel2desc,
             t2.issueyear          = t.issueyear,
             t2.issuephase         = t.issuephase
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.MAINCODE,
         t2.INNERCODE,
         t2.COMPANYCODE,
         t2.SECUCODE,
         t2.SECUABBR,
         t2.CHISPELLING,
         t2.CHINAME,
         t2.CHINAMEABBR,
         t2.ENGNAME,
         t2.ENGNAMEABBR,
         t2.CURRENCY,
         t2.SECUMARKET,
         t2.LISTEDDATE,
         t2.LISTEDSECTOR,
         t2.LISTEDSTATE,
         t2.DELISTDATE,
         t2.ISSUER,
         t2.ISSUERNATURE,
         t2.BONDNATURE,
         t2.UPDATETIME,
         t2.JSID,
         t2.INTERESTENDDATE,
         t2.BONDTYPELEVEL1,
         t2.BONDTYPELEVEL1DESC,
         t2.BONDTYPELEVEL2,
         t2.BONDTYPELEVEL2DESC,
         t2.ISSUEYEAR,
         t2.ISSUEPHASE)
      VALUES
        (t.ID,
         t.MAINCODE,
         t.INNERCODE,
         t.COMPANYCODE,
         t.SECUCODE,
         t.SECUABBR,
         t.CHISPELLING,
         t.CHINAME,
         t.CHINAMEABBR,
         t.ENGNAME,
         t.ENGNAMEABBR,
         t.CURRENCY,
         t.SECUMARKET,
         t.LISTEDDATE,
         t.LISTEDSECTOR,
         t.LISTEDSTATE,
         t.DELISTDATE,
         t.ISSUER,
         t.ISSUERNATURE,
         t.BONDNATURE,
         t.UPDATETIME,
         t.JSID,
         t.INTERESTENDDATE,
         t.BONDTYPELEVEL1,
         t.BONDTYPELEVEL1DESC,
         t.BONDTYPELEVEL2,
         t.BONDTYPELEVEL2DESC,
         t.ISSUEYEAR,
         t.ISSUEPHASE);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_BOND_CODE;

  --证券主表
  procedure cj_MIS_SECUMAIN(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_SECUMAIN t2
    USING t_MIS_SECUMAIN t
    ON (t.InnerCode = t2.InnerCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.companycode  = t.companycode,
             t2.secucode     = t.secucode,
             t2.chiname      = t.chiname,
             t2.chinameabbr  = t.chinameabbr,
             t2.engname      = t.engname,
             t2.engnameabbr  = t.engnameabbr,
             t2.secuabbr     = t.secuabbr,
             t2.chispelling  = t.chispelling,
             t2.secumarket   = t.secumarket,
             t2.secucategory = t.secucategory,
             t2.listeddate   = t.listeddate,
             t2.listedsector = t.listedsector,
             t2.listedstate  = t.listedstate,
             t2.xgrq         = t.xgrq,
             t2.jsid         = t.jsid,
             t2.isin         = t.isin
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.COMPANYCODE,
         t2.SECUCODE,
         t2.CHINAME,
         t2.CHINAMEABBR,
         t2.ENGNAME,
         t2.ENGNAMEABBR,
         t2.SECUABBR,
         t2.CHISPELLING,
         t2.SECUMARKET,
         t2.SECUCATEGORY,
         t2.LISTEDDATE,
         t2.LISTEDSECTOR,
         t2.LISTEDSTATE,
         t2.XGRQ,
         t2.JSID,
         t2.ISIN)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.COMPANYCODE,
         t.SECUCODE,
         t.CHINAME,
         t.CHINAMEABBR,
         t.ENGNAME,
         t.ENGNAMEABBR,
         t.SECUABBR,
         t.CHISPELLING,
         t.SECUMARKET,
         t.SECUCATEGORY,
         t.LISTEDDATE,
         t.LISTEDSECTOR,
         t.LISTEDSTATE,
         t.XGRQ,
         t.JSID,
         t.ISIN);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_SECUMAIN;

  --公募基金概况
  procedure cj_MIS_MF_FUNDARCHIVES(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_MF_FUNDARCHIVES t2
    USING t_MIS_MF_FUNDARCHIVES t
    ON (t.InnerCode = t2.InnerCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.establishmentdate              = t.establishmentdate,
             t2.listeddate                     = t.listeddate,
             t2.duration                       = t.duration,
             t2.startdate                      = t.startdate,
             t2.expiredate                     = t.expiredate,
             t2.manager                        = t.manager,
             t2.investadvisorcode              = t.investadvisorcode,
             t2.trusteecode                    = t.trusteecode,
             t2.warrantor                      = t.warrantor,
             t2.type                           = t.type,
             t2.investmenttype                 = t.investmenttype,
             t2.investstyle                    = t.investstyle,
             t2.foundedsize                    = t.foundedsize,
             t2.investorientation              = t.investorientation,
             t2.investtarget                   = t.investtarget,
             t2.performancebenchmark           = t.performancebenchmark,
             t2.profitdistributionrule         = t.profitdistributionrule,
             t2.investfield                    = t.investfield,
             t2.briefintro                     = t.briefintro,
             t2.xgrq                           = t.xgrq,
             t2.jsid                           = t.jsid,
             t2.applyingcodefront              = t.applyingcodefront,
             t2.applyingcodeback               = t.applyingcodeback,
             t2.guaranteedperiod               = t.guaranteedperiod,
             t2.riskreturncharacter            = t.riskreturncharacter,
             t2.lowestsumsubscribing           = t.lowestsumsubscribing,
             t2.lowestsumredemption            = t.lowestsumredemption,
             t2.lsfrdescription                = t.lsfrdescription,
             t2.lowestsumforholding            = t.lowestsumforholding,
             t2.lsfhdescription                = t.lsfhdescription,
             t2.fundnature                     = t.fundnature,
             t2.fundtypecode                   = t.fundtypecode,
             t2.fundtype                       = t.fundtype,
             t2.carryoverdate                  = t.carryoverdate,
             t2.carryoverdateremark            = t.carryoverdateremark,
             t2.carryovertype                  = t.carryovertype,
             t2.reginstcode                    = t.reginstcode,
             t2.securitycode                   = t.securitycode,
             t2.deliverydays                   = t.deliverydays,
             t2.riskreturncode                 = t.riskreturncode,
             t2.floattype                      = t.floattype,
             t2.custodymarket                  = t.custodymarket,
             t2.operationperiod                = t.operationperiod,
             t2.operationpdunitcode            = t.operationpdunitcode,
             t2.operationpdunitname            = t.operationpdunitname,
             t2.ifinitiatingfund               = t.ifinitiatingfund,
             t2.classificationfundtype         = t.classificationfundtype,
             t2.agrbenchmkrateofsharea         = t.agrbenchmkrateofsharea,
             t2.agrbenchmkrateofshareanotes    = t.agrbenchmkrateofshareanotes,
             t2.regularshareconversionnotes    = t.regularshareconversionnotes,
             t2.nonregularshareconversionnotes = t.nonregularshareconversionnotes,
             t2.exapplyingmarket               = t.exapplyingmarket,
             t2.exapplyingcode                 = t.exapplyingcode,
             t2.exapplyingabbr                 = t.exapplyingabbr,
             t2.shareproperties                = t.shareproperties,
             t2.stclearingdate                 = t.stclearingdate,
             t2.enclearingdate                 = t.enclearingdate,
             t2.lowestsumsubll                 = t.lowestsumsubll,
             t2.lowestsumpurll                 = t.lowestsumpurll,
             t2.maincode                       = t.maincode,
             t2.exprofitdistri                 = t.exprofitdistri,
             t2.otcprofitdistri                = t.otcprofitdistri,
             t2.iffof                          = t.iffof
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.ESTABLISHMENTDATE,
         t2.LISTEDDATE,
         t2.DURATION,
         t2.STARTDATE,
         t2.EXPIREDATE,
         t2.MANAGER,
         t2.INVESTADVISORCODE,
         t2.TRUSTEECODE,
         t2.WARRANTOR,
         t2.TYPE,
         t2.INVESTMENTTYPE,
         t2.INVESTSTYLE,
         t2.FOUNDEDSIZE,
         t2.INVESTORIENTATION,
         t2.INVESTTARGET,
         t2.PERFORMANCEBENCHMARK,
         t2.PROFITDISTRIBUTIONRULE,
         t2.INVESTFIELD,
         t2.BRIEFINTRO,
         t2.XGRQ,
         t2.JSID,
         t2.APPLYINGCODEFRONT,
         t2.APPLYINGCODEBACK,
         t2.GUARANTEEDPERIOD,
         t2.RISKRETURNCHARACTER,
         t2.LOWESTSUMSUBSCRIBING,
         t2.LOWESTSUMREDEMPTION,
         t2.LSFRDESCRIPTION,
         t2.LOWESTSUMFORHOLDING,
         t2.LSFHDESCRIPTION,
         t2.FUNDNATURE,
         t2.FUNDTYPECODE,
         t2.FUNDTYPE,
         t2.CARRYOVERDATE,
         t2.CARRYOVERDATEREMARK,
         t2.CARRYOVERTYPE,
         t2.REGINSTCODE,
         t2.SECURITYCODE,
         t2.DELIVERYDAYS,
         t2.RISKRETURNCODE,
         t2.FLOATTYPE,
         t2.CUSTODYMARKET,
         t2.OPERATIONPERIOD,
         t2.OPERATIONPDUNITCODE,
         t2.OPERATIONPDUNITNAME,
         t2.IFINITIATINGFUND,
         t2.CLASSIFICATIONFUNDTYPE,
         t2.AGRBENCHMKRATEOFSHAREA,
         t2.AGRBENCHMKRATEOFSHAREANOTES,
         t2.REGULARSHARECONVERSIONNOTES,
         t2.NONREGULARSHARECONVERSIONNOTES,
         t2.EXAPPLYINGMARKET,
         t2.EXAPPLYINGCODE,
         t2.EXAPPLYINGABBR,
         t2.SHAREPROPERTIES,
         t2.STCLEARINGDATE,
         t2.ENCLEARINGDATE,
         t2.LOWESTSUMSUBLL,
         t2.LOWESTSUMPURLL,
         t2.MAINCODE,
         t2.EXPROFITDISTRI,
         t2.OTCPROFITDISTRI,
         t2.IFFOF)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.ESTABLISHMENTDATE,
         t.LISTEDDATE,
         t.DURATION,
         t.STARTDATE,
         t.EXPIREDATE,
         t.MANAGER,
         t.INVESTADVISORCODE,
         t.TRUSTEECODE,
         t.WARRANTOR,
         t.TYPE,
         t.INVESTMENTTYPE,
         t.INVESTSTYLE,
         t.FOUNDEDSIZE,
         t.INVESTORIENTATION,
         t.INVESTTARGET,
         t.PERFORMANCEBENCHMARK,
         t.PROFITDISTRIBUTIONRULE,
         t.INVESTFIELD,
         t.BRIEFINTRO,
         t.XGRQ,
         t.JSID,
         t.APPLYINGCODEFRONT,
         t.APPLYINGCODEBACK,
         t.GUARANTEEDPERIOD,
         t.RISKRETURNCHARACTER,
         t.LOWESTSUMSUBSCRIBING,
         t.LOWESTSUMREDEMPTION,
         t.LSFRDESCRIPTION,
         t.LOWESTSUMFORHOLDING,
         t.LSFHDESCRIPTION,
         t.FUNDNATURE,
         t.FUNDTYPECODE,
         t.FUNDTYPE,
         t.CARRYOVERDATE,
         t.CARRYOVERDATEREMARK,
         t.CARRYOVERTYPE,
         t.REGINSTCODE,
         t.SECURITYCODE,
         t.DELIVERYDAYS,
         t.RISKRETURNCODE,
         t.FLOATTYPE,
         t.CUSTODYMARKET,
         t.OPERATIONPERIOD,
         t.OPERATIONPDUNITCODE,
         t.OPERATIONPDUNITNAME,
         t.IFINITIATINGFUND,
         t.CLASSIFICATIONFUNDTYPE,
         t.AGRBENCHMKRATEOFSHAREA,
         t.AGRBENCHMKRATEOFSHAREANOTES,
         t.REGULARSHARECONVERSIONNOTES,
         t.NONREGULARSHARECONVERSIONNOTES,
         t.EXAPPLYINGMARKET,
         t.EXAPPLYINGCODE,
         t.EXAPPLYINGABBR,
         t.SHAREPROPERTIES,
         t.STCLEARINGDATE,
         t.ENCLEARINGDATE,
         t.LOWESTSUMSUBLL,
         t.LOWESTSUMPURLL,
         t.MAINCODE,
         t.EXPROFITDISTRI,
         t.OTCPROFITDISTRI,
         t.IFFOF);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_MF_FUNDARCHIVES;

  --机构基本资料
  procedure cj_MIS_LC_INSTIARCHIVE(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_LC_INSTIARCHIVE t2
    USING t_MIS_LC_INSTIARCHIVE t
    ON (t.CompanyCode = t2.CompanyCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.parentcompany     = t.parentcompany,
             t2.listedcode        = t.listedcode,
             t2.investadvisorname = t.investadvisorname,
             t2.trusteename       = t.trusteename,
             t2.chiname           = t.chiname,
             t2.abbrchiname       = t.abbrchiname,
             t2.namechispelling   = t.namechispelling,
             t2.engname           = t.engname,
             t2.abbrengname       = t.abbrengname,
             t2.regcapital        = t.regcapital,
             t2.currencyunit      = t.currencyunit,
             t2.establishmentdate = t.establishmentdate,
             t2.economicnature    = t.economicnature,
             t2.companynature     = t.companynature,
             t2.companytype       = t.companytype,
             t2.regaddr           = t.regaddr,
             t2.regzip            = t.regzip,
             t2.regcity           = t.regcity,
             t2.officeaddr        = t.officeaddr,
             t2.contactaddr       = t.contactaddr,
             t2.contactzip        = t.contactzip,
             t2.contactcity       = t.contactcity,
             t2.email             = t.email,
             t2.website           = t.website,
             t2.legalpersonrepr   = t.legalpersonrepr,
             t2.generalmanager    = t.generalmanager,
             t2.othermanager      = t.othermanager,
             t2.contactman        = t.contactman,
             t2.tel               = t.tel,
             t2.fax               = t.fax,
             t2.briefintrotext    = t.briefintrotext,
             t2.businessmajor     = t.businessmajor,
             t2.industry          = t.industry,
             t2.startdate         = t.startdate,
             t2.closedate         = t.closedate,
             t2.closereason       = t.closereason,
             t2.ifexisted         = t.ifexisted,
             t2.xgrq              = t.xgrq,
             t2.jsid              = t.jsid,
             t2.organizationcode  = t.organizationcode,
             t2.companycval       = t.companycval,
             t2.creditcode        = t.creditcode,
             t2.regarea           = t.regarea
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.COMPANYCODE,
         t2.PARENTCOMPANY,
         t2.LISTEDCODE,
         t2.INVESTADVISORNAME,
         t2.TRUSTEENAME,
         t2.CHINAME,
         t2.ABBRCHINAME,
         t2.NAMECHISPELLING,
         t2.ENGNAME,
         t2.ABBRENGNAME,
         t2.REGCAPITAL,
         t2.CURRENCYUNIT,
         t2.ESTABLISHMENTDATE,
         t2.ECONOMICNATURE,
         t2.COMPANYNATURE,
         t2.COMPANYTYPE,
         t2.REGADDR,
         t2.REGZIP,
         t2.REGCITY,
         t2.OFFICEADDR,
         t2.CONTACTADDR,
         t2.CONTACTZIP,
         t2.CONTACTCITY,
         t2.EMAIL,
         t2.WEBSITE,
         t2.LEGALPERSONREPR,
         t2.GENERALMANAGER,
         t2.OTHERMANAGER,
         t2.CONTACTMAN,
         t2.TEL,
         t2.FAX,
         t2.BRIEFINTROTEXT,
         t2.BUSINESSMAJOR,
         t2.INDUSTRY,
         t2.STARTDATE,
         t2.CLOSEDATE,
         t2.CLOSEREASON,
         t2.IFEXISTED,
         t2.XGRQ,
         t2.JSID,
         t2.ORGANIZATIONCODE,
         t2.COMPANYCVAL,
         t2.CREDITCODE,
         t2.REGAREA)
      VALUES
        (t.ID,
         t.COMPANYCODE,
         t.PARENTCOMPANY,
         t.LISTEDCODE,
         t.INVESTADVISORNAME,
         t.TRUSTEENAME,
         t.CHINAME,
         t.ABBRCHINAME,
         t.NAMECHISPELLING,
         t.ENGNAME,
         t.ABBRENGNAME,
         t.REGCAPITAL,
         t.CURRENCYUNIT,
         t.ESTABLISHMENTDATE,
         t.ECONOMICNATURE,
         t.COMPANYNATURE,
         t.COMPANYTYPE,
         t.REGADDR,
         t.REGZIP,
         t.REGCITY,
         t.OFFICEADDR,
         t.CONTACTADDR,
         t.CONTACTZIP,
         t.CONTACTCITY,
         t.EMAIL,
         t.WEBSITE,
         t.LEGALPERSONREPR,
         t.GENERALMANAGER,
         t.OTHERMANAGER,
         t.CONTACTMAN,
         t.TEL,
         t.FAX,
         t.BRIEFINTROTEXT,
         t.BUSINESSMAJOR,
         t.INDUSTRY,
         t.STARTDATE,
         t.CLOSEDATE,
         t.CLOSEREASON,
         t.IFEXISTED,
         t.XGRQ,
         t.JSID,
         t.ORGANIZATIONCODE,
         t.COMPANYCVAL,
         t.CREDITCODE,
         t.REGAREA);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_LC_INSTIARCHIVE;

  --三板证券主表
  procedure cj_MIS_NQ_SecuMain(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_NQ_SecuMain t2
    USING t_MIS_NQ_SecuMain t
    ON (t.InnerCode = t2.InnerCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.companycode  = t.companycode,
             t2.secucode     = t.secucode,
             t2.chiname      = t.chiname,
             t2.chinameabbr  = t.chinameabbr,
             t2.engname      = t.engname,
             t2.engnameabbr  = t.engnameabbr,
             t2.secuabbr     = t.secuabbr,
             t2.chispelling  = t.chispelling,
             t2.secumarket   = t.secumarket,
             t2.secucategory = t.secucategory,
             t2.listeddate   = t.listeddate,
             t2.listedsector = t.listedsector,
             t2.listedstate  = t.listedstate,
             t2.isin         = t.isin,
             t2.inserttime   = t.inserttime,
             t2.updatetime   = t.updatetime,
             t2.jsid         = t.jsid
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.COMPANYCODE,
         t2.SECUCODE,
         t2.CHINAME,
         t2.CHINAMEABBR,
         t2.ENGNAME,
         t2.ENGNAMEABBR,
         t2.SECUABBR,
         t2.CHISPELLING,
         t2.SECUMARKET,
         t2.SECUCATEGORY,
         t2.LISTEDDATE,
         t2.LISTEDSECTOR,
         t2.LISTEDSTATE,
         t2.ISIN,
         t2.INSERTTIME,
         t2.UPDATETIME,
         t2.JSID)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.COMPANYCODE,
         t.SECUCODE,
         t.CHINAME,
         t.CHINAMEABBR,
         t.ENGNAME,
         t.ENGNAMEABBR,
         t.SECUABBR,
         t.CHISPELLING,
         t.SECUMARKET,
         t.SECUCATEGORY,
         t.LISTEDDATE,
         t.LISTEDSECTOR,
         t.LISTEDSTATE,
         t.ISIN,
         t.INSERTTIME,
         t.UPDATETIME,
         t.JSID);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_NQ_SecuMain;

  --A股发行与上市
  procedure cj_MIS_LC_ASHAREIPO(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_LC_ASHAREIPO t2
    USING t_MIS_LC_ASHAREIPO t
    ON (t.InnerCode = t2.InnerCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.initialinfopubldate      = t.initialinfopubldate,
             t2.intentletterpubldate     = t.intentletterpubldate,
             t2.intentlettersigndate     = t.intentlettersigndate,
             t2.prospectuspubldate       = t.prospectuspubldate,
             t2.prospectussigndate       = t.prospectussigndate,
             t2.listannouncementdate     = t.listannouncementdate,
             t2.raisingmethod            = t.raisingmethod,
             t2.stocktype                = t.stocktype,
             t2.issuepriceceiling        = t.issuepriceceiling,
             t2.issuepricefloor          = t.issuepricefloor,
             t2.issuevolceiling          = t.issuevolceiling,
             t2.issuevolfloor            = t.issuevolfloor,
             t2.overallotmentoption      = t.overallotmentoption,
             t2.parvalue                 = t.parvalue,
             t2.issueprice               = t.issueprice,
             t2.statesharesissueprice    = t.statesharesissueprice,
             t2.weightedperatio          = t.weightedperatio,
             t2.dilutedperatio           = t.dilutedperatio,
             t2.issuevol                 = t.issuevol,
             t2.statesharesissued        = t.statesharesissued,
             t2.totalissuemv             = t.totalissuemv,
             t2.issuecost                = t.issuecost,
             t2.underwritingfee          = t.underwritingfee,
             t2.cpafee                   = t.cpafee,
             t2.assetappraisalfee        = t.assetappraisalfee,
             t2.landevaluationfee        = t.landevaluationfee,
             t2.attorneyfee              = t.attorneyfee,
             t2.totalagentfee            = t.totalagentfee,
             t2.onlineissuefee           = t.onlineissuefee,
             t2.scripfee                 = t.scripfee,
             t2.sponsorfee               = t.sponsorfee,
             t2.otherfee                 = t.otherfee,
             t2.issuecostpershare        = t.issuecostpershare,
             t2.ipoproceeds              = t.ipoproceeds,
             t2.iponetproceeds           = t.iponetproceeds,
             t2.statesharesproceeds      = t.statesharesproceeds,
             t2.statesharesnetproceeds   = t.statesharesnetproceeds,
             t2.moneytoaccount           = t.moneytoaccount,
             t2.datetoaccount            = t.datetoaccount,
             t2.pricingmodel             = t.pricingmodel,
             t2.rationmodel              = t.rationmodel,
             t2.issuemethod              = t.issuemethod,
             t2.issueobject              = t.issueobject,
             t2.issuestartdate           = t.issuestartdate,
             t2.issueenddate             = t.issueenddate,
             t2.onlinestartdate          = t.onlinestartdate,
             t2.onlineenddate            = t.onlineenddate,
             t2.bookingstartdatelp       = t.bookingstartdatelp,
             t2.bookingenddatelp         = t.bookingenddatelp,
             t2.paystartdatelp           = t.paystartdatelp,
             t2.payenddatelp             = t.payenddatelp,
             t2.applystartdatef          = t.applystartdatef,
             t2.applyenddatef            = t.applyenddatef,
             t2.paystartdatef            = t.paystartdatef,
             t2.payenddatef              = t.payenddatef,
             t2.secmarketplacingdate     = t.secmarketplacingdate,
             t2.paystartdatesm           = t.paystartdatesm,
             t2.payenddatesm             = t.payenddatesm,
             t2.underwritingsigndate     = t.underwritingsigndate,
             t2.underwritingstartdate    = t.underwritingstartdate,
             t2.underwritingenddate      = t.underwritingenddate,
             t2.preparedlistexchange     = t.preparedlistexchange,
             t2.listdate                 = t.listdate,
             t2.staffshareslistdate      = t.staffshareslistdate,
             t2.staffshareslistterm      = t.staffshareslistterm,
             t2.outstandingshares        = t.outstandingshares,
             t2.numover1000shares        = t.numover1000shares,
             t2.firstopenprice           = t.firstopenprice,
             t2.firstcloseprice          = t.firstcloseprice,
             t2.firsthighprice           = t.firsthighprice,
             t2.firstlowprice            = t.firstlowprice,
             t2.firstturnover            = t.firstturnover,
             t2.applycodeonline          = t.applycodeonline,
             t2.sharesonline             = t.sharesonline,
             t2.applyunitonline          = t.applyunitonline,
             t2.applymaxonline           = t.applymaxonline,
             t2.validapplyvolonline      = t.validapplyvolonline,
             t2.validapplynumonline      = t.validapplynumonline,
             t2.oversubstimesonline      = t.oversubstimesonline,
             t2.numallotedonline         = t.numallotedonline,
             t2.freezedmoneyonline       = t.freezedmoneyonline,
             t2.lotrateonline            = t.lotrateonline,
             t2.applycodesm              = t.applycodesm,
             t2.applymaxsm               = t.applymaxsm,
             t2.placingsharessm          = t.placingsharessm,
             t2.applyunitsm              = t.applyunitsm,
             t2.validapplyvolsm          = t.validapplyvolsm,
             t2.validapplynumsm          = t.validapplynumsm,
             t2.oversubstimessm          = t.oversubstimessm,
             t2.numallotedsm             = t.numallotedsm,
             t2.lotratesm                = t.lotratesm,
             t2.placingshareslp          = t.placingshareslp,
             t2.applyunitlp              = t.applyunitlp,
             t2.applymaxlp               = t.applymaxlp,
             t2.applyminlp               = t.applyminlp,
             t2.validapplyvollp          = t.validapplyvollp,
             t2.validapplynumlp          = t.validapplynumlp,
             t2.oversubstimeslp          = t.oversubstimeslp,
             t2.applymoneylp             = t.applymoneylp,
             t2.lotratelp                = t.lotratelp,
             t2.fundprefallotment        = t.fundprefallotment,
             t2.singlefundprefallotment  = t.singlefundprefallotment,
             t2.funprefallotmentshares   = t.funprefallotmentshares,
             t2.funprefallotmentholdterm = t.funprefallotmentholdterm,
             t2.staqnetallotment         = t.staqnetallotment,
             t2.staqnetsubscription      = t.staqnetsubscription,
             t2.staffallotment           = t.staffallotment,
             t2.earningforecastyear      = t.earningforecastyear,
             t2.mainincomeforecast       = t.mainincomeforecast,
             t2.netprofitforecast        = t.netprofitforecast,
             t2.dilutedepsforecast       = t.dilutedepsforecast,
             t2.dividendpolicy           = t.dividendpolicy,
             t2.estimatedfirstdividate   = t.estimatedfirstdividate,
             t2.underwritingmode         = t.underwritingmode,
             t2.underwriterboughtvol     = t.underwriterboughtvol,
             t2.xgrq                     = t.xgrq,
             t2.jsid                     = t.jsid,
             t2.peratiobeforeissue       = t.peratiobeforeissue,
             t2.peratioafterissue        = t.peratioafterissue,
             t2.biddernumberlp           = t.biddernumberlp,
             t2.placingnumberlp          = t.placingnumberlp,
             t2.applyvollp               = t.applyvollp,
             t2.tailoredplavollp         = t.tailoredplavollp,
             t2.onlineissueplan          = t.onlineissueplan,
             t2.offlineapplyplan         = t.offlineapplyplan,
             t2.strategyapplyplan        = t.strategyapplyplan,
             t2.refundmentdate_offline   = t.refundmentdate_offline,
             t2.refundmentdate_online    = t.refundmentdate_online,
             t2.issuenameabbr_online     = t.issuenameabbr_online,
             t2.applyfloor_online        = t.applyfloor_online,
             t2.loaccuapplyceiling       = t.loaccuapplyceiling,
             t2.offlinelockperiod        = t.offlinelockperiod,
             t2.oversubsum               = t.oversubsum,
             t2.exgsoptionpubldate       = t.exgsoptionpubldate,
             t2.exgsoptionenddate        = t.exgsoptionenddate,
             t2.slbuysum                 = t.slbuysum,
             t2.exgsoptionoversubsum     = t.exgsoptionoversubsum,
             t2.napsbeforeissue          = t.napsbeforeissue,
             t2.napsafterissue           = t.napsafterissue,
             t2.issueresultpubldate      = t.issueresultpubldate,
             t2.normallegalpersonshareld = t.normallegalpersonshareld,
             t2.strategicinvestorshareld = t.strategicinvestorshareld,
             t2.leadunderwriter          = t.leadunderwriter,
             t2.coleadunderwriter        = t.coleadunderwriter,
             t2.listingsponsor           = t.listingsponsor,
             t2.distributor              = t.distributor,
             t2.internationalcoordinator = t.internationalcoordinator,
             t2.normallegalpersonshare   = t.normallegalpersonshare,
             t2.strategicinvestorshare   = t.strategicinvestorshare,
             t2.otherplacingshare        = t.otherplacingshare,
             t2.placingshareproportion   = t.placingshareproportion,
             t2.totalshareslistdate      = t.totalshareslistdate,
             t2.totalsharesbeforeissue   = t.totalsharesbeforeissue,
             t2.firstavgprice            = t.firstavgprice,
             t2.firstturnovervolume      = t.firstturnovervolume,
             t2.firstturnovervalue       = t.firstturnovervalue,
             t2.firstchangepct           = t.firstchangepct,
             t2.secucode                 = t.secucode,
             t2.plannedproceeds          = t.plannedproceeds,
             t2.pricenumberpi            = t.pricenumberpi,
             t2.minprogressivepricepi    = t.minprogressivepricepi,
             t2.minapplyingpricepi       = t.minapplyingpricepi,
             t2.pricenumberbb            = t.pricenumberbb,
             t2.minprogressivepricebb    = t.minprogressivepricebb,
             t2.maxapplyingratiobb       = t.maxapplyingratiobb,
             t2.securityabbr             = t.securityabbr,
             t2.issueprocesscode         = t.issueprocesscode,
             t2.timesbb                  = t.timesbb,
             t2.originalvolceiling       = t.originalvolceiling,
             t2.originalvol              = t.originalvol,
             t2.olbefputback             = t.olbefputback,
             t2.offlbefputback           = t.offlbefputback,
             t2.paydateonline            = t.paydateonline,
             t2.paydateoffline           = t.paydateoffline,
             t2.issuepubldate            = t.issuepubldate,
             t2.resultpulbdate           = t.resultpulbdate,
             t2.subsshareol              = t.subsshareol,
             t2.subsmoneyol              = t.subsmoneyol,
             t2.abandonsubssol           = t.abandonsubssol,
             t2.abandonsubsmol           = t.abandonsubsmol,
             t2.underwritsol             = t.underwritsol,
             t2.underwritmol             = t.underwritmol,
             t2.underwritpol             = t.underwritpol,
             t2.subssharefl              = t.subssharefl,
             t2.subsmoneyfl              = t.subsmoneyfl,
             t2.abandonsubssfl           = t.abandonsubssfl,
             t2.abandonsubsmfl           = t.abandonsubsmfl,
             t2.underwritsfl             = t.underwritsfl,
             t2.underwritmfl             = t.underwritmfl,
             t2.underwritpfl             = t.underwritpfl,
             t2.markvaluefloor           = t.markvaluefloor,
             t2.markvaluestatmt          = t.markvaluestatmt,
             t2.lotnumonline             = t.lotnumonline,
             t2.estiissueprice           = t.estiissueprice,
             t2.estiapmaxonline          = t.estiapmaxonline,
             t2.alotratelp               = t.alotratelp,
             t2.blotratelp               = t.blotratelp,
             t2.clotratelp               = t.clotratelp,
             t2.estiperatio              = t.estiperatio
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.INITIALINFOPUBLDATE,
         t2.INTENTLETTERPUBLDATE,
         t2.INTENTLETTERSIGNDATE,
         t2.PROSPECTUSPUBLDATE,
         t2.PROSPECTUSSIGNDATE,
         t2.LISTANNOUNCEMENTDATE,
         t2.RAISINGMETHOD,
         t2.STOCKTYPE,
         t2.ISSUEPRICECEILING,
         t2.ISSUEPRICEFLOOR,
         t2.ISSUEVOLCEILING,
         t2.ISSUEVOLFLOOR,
         t2.OVERALLOTMENTOPTION,
         t2.PARVALUE,
         t2.ISSUEPRICE,
         t2.STATESHARESISSUEPRICE,
         t2.WEIGHTEDPERATIO,
         t2.DILUTEDPERATIO,
         t2.ISSUEVOL,
         t2.STATESHARESISSUED,
         t2.TOTALISSUEMV,
         t2.ISSUECOST,
         t2.UNDERWRITINGFEE,
         t2.CPAFEE,
         t2.ASSETAPPRAISALFEE,
         t2.LANDEVALUATIONFEE,
         t2.ATTORNEYFEE,
         t2.TOTALAGENTFEE,
         t2.ONLINEISSUEFEE,
         t2.SCRIPFEE,
         t2.SPONSORFEE,
         t2.OTHERFEE,
         t2.ISSUECOSTPERSHARE,
         t2.IPOPROCEEDS,
         t2.IPONETPROCEEDS,
         t2.STATESHARESPROCEEDS,
         t2.STATESHARESNETPROCEEDS,
         t2.MONEYTOACCOUNT,
         t2.DATETOACCOUNT,
         t2.PRICINGMODEL,
         t2.RATIONMODEL,
         t2.ISSUEMETHOD,
         t2.ISSUEOBJECT,
         t2.ISSUESTARTDATE,
         t2.ISSUEENDDATE,
         t2.ONLINESTARTDATE,
         t2.ONLINEENDDATE,
         t2.BOOKINGSTARTDATELP,
         t2.BOOKINGENDDATELP,
         t2.PAYSTARTDATELP,
         t2.PAYENDDATELP,
         t2.APPLYSTARTDATEF,
         t2.APPLYENDDATEF,
         t2.PAYSTARTDATEF,
         t2.PAYENDDATEF,
         t2.SECMARKETPLACINGDATE,
         t2.PAYSTARTDATESM,
         t2.PAYENDDATESM,
         t2.UNDERWRITINGSIGNDATE,
         t2.UNDERWRITINGSTARTDATE,
         t2.UNDERWRITINGENDDATE,
         t2.PREPAREDLISTEXCHANGE,
         t2.LISTDATE,
         t2.STAFFSHARESLISTDATE,
         t2.STAFFSHARESLISTTERM,
         t2.OUTSTANDINGSHARES,
         t2.NUMOVER1000SHARES,
         t2.FIRSTOPENPRICE,
         t2.FIRSTCLOSEPRICE,
         t2.FIRSTHIGHPRICE,
         t2.FIRSTLOWPRICE,
         t2.FIRSTTURNOVER,
         t2.APPLYCODEONLINE,
         t2.SHARESONLINE,
         t2.APPLYUNITONLINE,
         t2.APPLYMAXONLINE,
         t2.VALIDAPPLYVOLONLINE,
         t2.VALIDAPPLYNUMONLINE,
         t2.OVERSUBSTIMESONLINE,
         t2.NUMALLOTEDONLINE,
         t2.FREEZEDMONEYONLINE,
         t2.LOTRATEONLINE,
         t2.APPLYCODESM,
         t2.APPLYMAXSM,
         t2.PLACINGSHARESSM,
         t2.APPLYUNITSM,
         t2.VALIDAPPLYVOLSM,
         t2.VALIDAPPLYNUMSM,
         t2.OVERSUBSTIMESSM,
         t2.NUMALLOTEDSM,
         t2.LOTRATESM,
         t2.PLACINGSHARESLP,
         t2.APPLYUNITLP,
         t2.APPLYMAXLP,
         t2.APPLYMINLP,
         t2.VALIDAPPLYVOLLP,
         t2.VALIDAPPLYNUMLP,
         t2.OVERSUBSTIMESLP,
         t2.APPLYMONEYLP,
         t2.LOTRATELP,
         t2.FUNDPREFALLOTMENT,
         t2.SINGLEFUNDPREFALLOTMENT,
         t2.FUNPREFALLOTMENTSHARES,
         t2.FUNPREFALLOTMENTHOLDTERM,
         t2.STAQNETALLOTMENT,
         t2.STAQNETSUBSCRIPTION,
         t2.STAFFALLOTMENT,
         t2.EARNINGFORECASTYEAR,
         t2.MAININCOMEFORECAST,
         t2.NETPROFITFORECAST,
         t2.DILUTEDEPSFORECAST,
         t2.DIVIDENDPOLICY,
         t2.ESTIMATEDFIRSTDIVIDATE,
         t2.UNDERWRITINGMODE,
         t2.UNDERWRITERBOUGHTVOL,
         t2.XGRQ,
         t2.JSID,
         t2.PERATIOBEFOREISSUE,
         t2.PERATIOAFTERISSUE,
         t2.BIDDERNUMBERLP,
         t2.PLACINGNUMBERLP,
         t2.APPLYVOLLP,
         t2.TAILOREDPLAVOLLP,
         t2.ONLINEISSUEPLAN,
         t2.OFFLINEAPPLYPLAN,
         t2.STRATEGYAPPLYPLAN,
         t2.REFUNDMENTDATE_OFFLINE,
         t2.REFUNDMENTDATE_ONLINE,
         t2.ISSUENAMEABBR_ONLINE,
         t2.APPLYFLOOR_ONLINE,
         t2.LOACCUAPPLYCEILING,
         t2.OFFLINELOCKPERIOD,
         t2.OVERSUBSUM,
         t2.EXGSOPTIONPUBLDATE,
         t2.EXGSOPTIONENDDATE,
         t2.SLBUYSUM,
         t2.EXGSOPTIONOVERSUBSUM,
         t2.NAPSBEFOREISSUE,
         t2.NAPSAFTERISSUE,
         t2.ISSUERESULTPUBLDATE,
         t2.NORMALLEGALPERSONSHARELD,
         t2.STRATEGICINVESTORSHARELD,
         t2.LEADUNDERWRITER,
         t2.COLEADUNDERWRITER,
         t2.LISTINGSPONSOR,
         t2.DISTRIBUTOR,
         t2.INTERNATIONALCOORDINATOR,
         t2.NORMALLEGALPERSONSHARE,
         t2.STRATEGICINVESTORSHARE,
         t2.OTHERPLACINGSHARE,
         t2.PLACINGSHAREPROPORTION,
         t2.TOTALSHARESLISTDATE,
         t2.TOTALSHARESBEFOREISSUE,
         t2.FIRSTAVGPRICE,
         t2.FIRSTTURNOVERVOLUME,
         t2.FIRSTTURNOVERVALUE,
         t2.FIRSTCHANGEPCT,
         t2.SECUCODE,
         t2.PLANNEDPROCEEDS,
         t2.PRICENUMBERPI,
         t2.MINPROGRESSIVEPRICEPI,
         t2.MINAPPLYINGPRICEPI,
         t2.PRICENUMBERBB,
         t2.MINPROGRESSIVEPRICEBB,
         t2.MAXAPPLYINGRATIOBB,
         t2.SECURITYABBR,
         t2.ISSUEPROCESSCODE,
         t2.TIMESBB,
         t2.ORIGINALVOLCEILING,
         t2.ORIGINALVOL,
         t2.OLBEFPUTBACK,
         t2.OFFLBEFPUTBACK,
         t2.PAYDATEONLINE,
         t2.PAYDATEOFFLINE,
         t2.ISSUEPUBLDATE,
         t2.RESULTPULBDATE,
         t2.SUBSSHAREOL,
         t2.SUBSMONEYOL,
         t2.ABANDONSUBSSOL,
         t2.ABANDONSUBSMOL,
         t2.UNDERWRITSOL,
         t2.UNDERWRITMOL,
         t2.UNDERWRITPOL,
         t2.SUBSSHAREFL,
         t2.SUBSMONEYFL,
         t2.ABANDONSUBSSFL,
         t2.ABANDONSUBSMFL,
         t2.UNDERWRITSFL,
         t2.UNDERWRITMFL,
         t2.UNDERWRITPFL,
         t2.MARKVALUEFLOOR,
         t2.MARKVALUESTATMT,
         t2.LOTNUMONLINE,
         t2.ESTIISSUEPRICE,
         t2.ESTIAPMAXONLINE,
         t2.ALOTRATELP,
         t2.BLOTRATELP,
         t2.CLOTRATELP,
         t2.ESTIPERATIO)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.INITIALINFOPUBLDATE,
         t.INTENTLETTERPUBLDATE,
         t.INTENTLETTERSIGNDATE,
         t.PROSPECTUSPUBLDATE,
         t.PROSPECTUSSIGNDATE,
         t.LISTANNOUNCEMENTDATE,
         t.RAISINGMETHOD,
         t.STOCKTYPE,
         t.ISSUEPRICECEILING,
         t.ISSUEPRICEFLOOR,
         t.ISSUEVOLCEILING,
         t.ISSUEVOLFLOOR,
         t.OVERALLOTMENTOPTION,
         t.PARVALUE,
         t.ISSUEPRICE,
         t.STATESHARESISSUEPRICE,
         t.WEIGHTEDPERATIO,
         t.DILUTEDPERATIO,
         t.ISSUEVOL,
         t.STATESHARESISSUED,
         t.TOTALISSUEMV,
         t.ISSUECOST,
         t.UNDERWRITINGFEE,
         t.CPAFEE,
         t.ASSETAPPRAISALFEE,
         t.LANDEVALUATIONFEE,
         t.ATTORNEYFEE,
         t.TOTALAGENTFEE,
         t.ONLINEISSUEFEE,
         t.SCRIPFEE,
         t.SPONSORFEE,
         t.OTHERFEE,
         t.ISSUECOSTPERSHARE,
         t.IPOPROCEEDS,
         t.IPONETPROCEEDS,
         t.STATESHARESPROCEEDS,
         t.STATESHARESNETPROCEEDS,
         t.MONEYTOACCOUNT,
         t.DATETOACCOUNT,
         t.PRICINGMODEL,
         t.RATIONMODEL,
         t.ISSUEMETHOD,
         t.ISSUEOBJECT,
         t.ISSUESTARTDATE,
         t.ISSUEENDDATE,
         t.ONLINESTARTDATE,
         t.ONLINEENDDATE,
         t.BOOKINGSTARTDATELP,
         t.BOOKINGENDDATELP,
         t.PAYSTARTDATELP,
         t.PAYENDDATELP,
         t.APPLYSTARTDATEF,
         t.APPLYENDDATEF,
         t.PAYSTARTDATEF,
         t.PAYENDDATEF,
         t.SECMARKETPLACINGDATE,
         t.PAYSTARTDATESM,
         t.PAYENDDATESM,
         t.UNDERWRITINGSIGNDATE,
         t.UNDERWRITINGSTARTDATE,
         t.UNDERWRITINGENDDATE,
         t.PREPAREDLISTEXCHANGE,
         t.LISTDATE,
         t.STAFFSHARESLISTDATE,
         t.STAFFSHARESLISTTERM,
         t.OUTSTANDINGSHARES,
         t.NUMOVER1000SHARES,
         t.FIRSTOPENPRICE,
         t.FIRSTCLOSEPRICE,
         t.FIRSTHIGHPRICE,
         t.FIRSTLOWPRICE,
         t.FIRSTTURNOVER,
         t.APPLYCODEONLINE,
         t.SHARESONLINE,
         t.APPLYUNITONLINE,
         t.APPLYMAXONLINE,
         t.VALIDAPPLYVOLONLINE,
         t.VALIDAPPLYNUMONLINE,
         t.OVERSUBSTIMESONLINE,
         t.NUMALLOTEDONLINE,
         t.FREEZEDMONEYONLINE,
         t.LOTRATEONLINE,
         t.APPLYCODESM,
         t.APPLYMAXSM,
         t.PLACINGSHARESSM,
         t.APPLYUNITSM,
         t.VALIDAPPLYVOLSM,
         t.VALIDAPPLYNUMSM,
         t.OVERSUBSTIMESSM,
         t.NUMALLOTEDSM,
         t.LOTRATESM,
         t.PLACINGSHARESLP,
         t.APPLYUNITLP,
         t.APPLYMAXLP,
         t.APPLYMINLP,
         t.VALIDAPPLYVOLLP,
         t.VALIDAPPLYNUMLP,
         t.OVERSUBSTIMESLP,
         t.APPLYMONEYLP,
         t.LOTRATELP,
         t.FUNDPREFALLOTMENT,
         t.SINGLEFUNDPREFALLOTMENT,
         t.FUNPREFALLOTMENTSHARES,
         t.FUNPREFALLOTMENTHOLDTERM,
         t.STAQNETALLOTMENT,
         t.STAQNETSUBSCRIPTION,
         t.STAFFALLOTMENT,
         t.EARNINGFORECASTYEAR,
         t.MAININCOMEFORECAST,
         t.NETPROFITFORECAST,
         t.DILUTEDEPSFORECAST,
         t.DIVIDENDPOLICY,
         t.ESTIMATEDFIRSTDIVIDATE,
         t.UNDERWRITINGMODE,
         t.UNDERWRITERBOUGHTVOL,
         t.XGRQ,
         t.JSID,
         t.PERATIOBEFOREISSUE,
         t.PERATIOAFTERISSUE,
         t.BIDDERNUMBERLP,
         t.PLACINGNUMBERLP,
         t.APPLYVOLLP,
         t.TAILOREDPLAVOLLP,
         t.ONLINEISSUEPLAN,
         t.OFFLINEAPPLYPLAN,
         t.STRATEGYAPPLYPLAN,
         t.REFUNDMENTDATE_OFFLINE,
         t.REFUNDMENTDATE_ONLINE,
         t.ISSUENAMEABBR_ONLINE,
         t.APPLYFLOOR_ONLINE,
         t.LOACCUAPPLYCEILING,
         t.OFFLINELOCKPERIOD,
         t.OVERSUBSUM,
         t.EXGSOPTIONPUBLDATE,
         t.EXGSOPTIONENDDATE,
         t.SLBUYSUM,
         t.EXGSOPTIONOVERSUBSUM,
         t.NAPSBEFOREISSUE,
         t.NAPSAFTERISSUE,
         t.ISSUERESULTPUBLDATE,
         t.NORMALLEGALPERSONSHARELD,
         t.STRATEGICINVESTORSHARELD,
         t.LEADUNDERWRITER,
         t.COLEADUNDERWRITER,
         t.LISTINGSPONSOR,
         t.DISTRIBUTOR,
         t.INTERNATIONALCOORDINATOR,
         t.NORMALLEGALPERSONSHARE,
         t.STRATEGICINVESTORSHARE,
         t.OTHERPLACINGSHARE,
         t.PLACINGSHAREPROPORTION,
         t.TOTALSHARESLISTDATE,
         t.TOTALSHARESBEFOREISSUE,
         t.FIRSTAVGPRICE,
         t.FIRSTTURNOVERVOLUME,
         t.FIRSTTURNOVERVALUE,
         t.FIRSTCHANGEPCT,
         t.SECUCODE,
         t.PLANNEDPROCEEDS,
         t.PRICENUMBERPI,
         t.MINPROGRESSIVEPRICEPI,
         t.MINAPPLYINGPRICEPI,
         t.PRICENUMBERBB,
         t.MINPROGRESSIVEPRICEBB,
         t.MAXAPPLYINGRATIOBB,
         t.SECURITYABBR,
         t.ISSUEPROCESSCODE,
         t.TIMESBB,
         t.ORIGINALVOLCEILING,
         t.ORIGINALVOL,
         t.OLBEFPUTBACK,
         t.OFFLBEFPUTBACK,
         t.PAYDATEONLINE,
         t.PAYDATEOFFLINE,
         t.ISSUEPUBLDATE,
         t.RESULTPULBDATE,
         t.SUBSSHAREOL,
         t.SUBSMONEYOL,
         t.ABANDONSUBSSOL,
         t.ABANDONSUBSMOL,
         t.UNDERWRITSOL,
         t.UNDERWRITMOL,
         t.UNDERWRITPOL,
         t.SUBSSHAREFL,
         t.SUBSMONEYFL,
         t.ABANDONSUBSSFL,
         t.ABANDONSUBSMFL,
         t.UNDERWRITSFL,
         t.UNDERWRITMFL,
         t.UNDERWRITPFL,
         t.MARKVALUEFLOOR,
         t.MARKVALUESTATMT,
         t.LOTNUMONLINE,
         t.ESTIISSUEPRICE,
         t.ESTIAPMAXONLINE,
         t.ALOTRATELP,
         t.BLOTRATELP,
         t.CLOTRATELP,
         t.ESTIPERATIO);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_LC_ASHAREIPO;

  --债券发行与上市
  procedure cj_MIS_BOND_ISSUE(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_BOND_ISSUE t2
    USING t_MIS_BOND_ISSUE t
    ON (t.InnerCode = t2.InnerCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.maincode               = t.maincode,
             t2.bondfullname           = t.bondfullname,
             t2.bondnature             = t.bondnature,
             t2.bondform               = t.bondform,
             t2.currencyunit           = t.currencyunit,
             t2.crossexchange          = t.crossexchange,
             t2.initialinfopubldate    = t.initialinfopubldate,
             t2.issuer                 = t.issuer,
             t2.issuernature           = t.issuernature,
             t2.guarantor              = t.guarantor,
             t2.guarantmethod          = t.guarantmethod,
             t2.cras                   = t.cras,
             t2.creditrating           = t.creditrating,
             t2.leadunderwriter        = t.leadunderwriter,
             t2.underwritingmethod     = t.underwritingmethod,
             t2.publicannouncementdate = t.publicannouncementdate,
             t2.issueobject            = t.issueobject,
             t2.issuemethod            = t.issuemethod,
             t2.issueprice             = t.issueprice,
             t2.substriptionunit       = t.substriptionunit,
             t2.issuecommissionrate    = t.issuecommissionrate,
             t2.issuestardate          = t.issuestardate,
             t2.issueenddate           = t.issueenddate,
             t2.planissuesize          = t.planissuesize,
             t2.actualissuesize        = t.actualissuesize,
             t2.expuannouncementday    = t.expuannouncementday,
             t2.listeddate             = t.listeddate,
             t2.exchange               = t.exchange,
             t2.tradeunit              = t.tradeunit,
             t2.parvalueper            = t.parvalueper,
             t2.delistdate             = t.delistdate,
             t2.updatetime             = t.updatetime,
             t2.jsid                   = t.jsid,
             t2.applycodeonline        = t.applycodeonline,
             t2.applyunitonline        = t.applyunitonline,
             t2.applymaxonline         = t.applymaxonline,
             t2.applyminonline         = t.applyminonline,
             t2.applyminoffline        = t.applyminoffline
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.MAINCODE,
         t2.INNERCODE,
         t2.BONDFULLNAME,
         t2.BONDNATURE,
         t2.BONDFORM,
         t2.CURRENCYUNIT,
         t2.CROSSEXCHANGE,
         t2.INITIALINFOPUBLDATE,
         t2.ISSUER,
         t2.ISSUERNATURE,
         t2.GUARANTOR,
         t2.GUARANTMETHOD,
         t2.CRAS,
         t2.CREDITRATING,
         t2.LEADUNDERWRITER,
         t2.UNDERWRITINGMETHOD,
         t2.PUBLICANNOUNCEMENTDATE,
         t2.ISSUEOBJECT,
         t2.ISSUEMETHOD,
         t2.ISSUEPRICE,
         t2.SUBSTRIPTIONUNIT,
         t2.ISSUECOMMISSIONRATE,
         t2.ISSUESTARDATE,
         t2.ISSUEENDDATE,
         t2.PLANISSUESIZE,
         t2.ACTUALISSUESIZE,
         t2.EXPUANNOUNCEMENTDAY,
         t2.LISTEDDATE,
         t2.EXCHANGE,
         t2.TRADEUNIT,
         t2.PARVALUEPER,
         t2.DELISTDATE,
         t2.UPDATETIME,
         t2.JSID,
         t2.APPLYCODEONLINE,
         t2.APPLYUNITONLINE,
         t2.APPLYMAXONLINE,
         t2.APPLYMINONLINE,
         t2.APPLYMINOFFLINE)
      VALUES
        (t.ID,
         t.MAINCODE,
         t.INNERCODE,
         t.BONDFULLNAME,
         t.BONDNATURE,
         t.BONDFORM,
         t.CURRENCYUNIT,
         t.CROSSEXCHANGE,
         t.INITIALINFOPUBLDATE,
         t.ISSUER,
         t.ISSUERNATURE,
         t.GUARANTOR,
         t.GUARANTMETHOD,
         t.CRAS,
         t.CREDITRATING,
         t.LEADUNDERWRITER,
         t.UNDERWRITINGMETHOD,
         t.PUBLICANNOUNCEMENTDATE,
         t.ISSUEOBJECT,
         t.ISSUEMETHOD,
         t.ISSUEPRICE,
         t.SUBSTRIPTIONUNIT,
         t.ISSUECOMMISSIONRATE,
         t.ISSUESTARDATE,
         t.ISSUEENDDATE,
         t.PLANISSUESIZE,
         t.ACTUALISSUESIZE,
         t.EXPUANNOUNCEMENTDAY,
         t.LISTEDDATE,
         t.EXCHANGE,
         t.TRADEUNIT,
         t.PARVALUEPER,
         t.DELISTDATE,
         t.UPDATETIME,
         t.JSID,
         t.APPLYCODEONLINE,
         t.APPLYUNITONLINE,
         t.APPLYMAXONLINE,
         t.APPLYMINONLINE,
         t.APPLYMINOFFLINE);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_BOND_ISSUE;

  --公司行业划分表
  procedure cj_MIS_LC_EXGINDUSTRY(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_LC_EXGINDUSTRY t2
    USING t_MIS_LC_EXGINDUSTRY t
    ON (t.CompanyCode = t2.CompanyCode and t.InfoPublDate = t2.InfoPublDate and t.Standard = t2.Standard and t.Industry = t2.Industry and t.IfPerformed = t2.IfPerformed)
    WHEN MATCHED THEN
      UPDATE
         SET t2.infosource         = t.infosource,
             t2.canceldate         = t.canceldate,
             t2.xgrq               = t.xgrq,
             t2.jsid               = t.jsid,
             t2.firstindustrycode  = t.firstindustrycode,
             t2.firstindustryname  = t.firstindustryname,
             t2.secondindustrycode = t.secondindustrycode,
             t2.secondindustryname = t.secondindustryname,
             t2.thirdindustrycode  = t.thirdindustrycode,
             t2.thirdindustryname  = t.thirdindustryname,
             t2.fourthindustrycode = t.fourthindustrycode,
             t2.fourthindustryname = t.fourthindustryname
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.COMPANYCODE,
         t2.INFOPUBLDATE,
         t2.INFOSOURCE,
         t2.STANDARD,
         t2.INDUSTRY,
         t2.IFPERFORMED,
         t2.CANCELDATE,
         t2.XGRQ,
         t2.JSID,
         t2.FIRSTINDUSTRYCODE,
         t2.FIRSTINDUSTRYNAME,
         t2.SECONDINDUSTRYCODE,
         t2.SECONDINDUSTRYNAME,
         t2.THIRDINDUSTRYCODE,
         t2.THIRDINDUSTRYNAME,
         t2.FOURTHINDUSTRYCODE,
         t2.FOURTHINDUSTRYNAME)
      VALUES
        (t.ID,
         t.COMPANYCODE,
         t.INFOPUBLDATE,
         t.INFOSOURCE,
         t.STANDARD,
         t.INDUSTRY,
         t.IFPERFORMED,
         t.CANCELDATE,
         t.XGRQ,
         t.JSID,
         t.FIRSTINDUSTRYCODE,
         t.FIRSTINDUSTRYNAME,
         t.SECONDINDUSTRYCODE,
         t.SECONDINDUSTRYNAME,
         t.THIRDINDUSTRYCODE,
         t.THIRDINDUSTRYNAME,
         t.FOURTHINDUSTRYCODE,
         t.FOURTHINDUSTRYNAME);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_LC_EXGINDUSTRY;

  --行业类别表
  procedure cj_MIS_CT_INDUSTRYTYPE(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_CT_INDUSTRYTYPE t2
    USING t_MIS_CT_INDUSTRYTYPE t
    ON (t.Standard = t2.Standard and t.IndustryNum = t2.IndustryNum)
    WHEN MATCHED THEN
      UPDATE
         SET t2.classification     = t.classification,
             t2.industrycode       = t.industrycode,
             t2.industryname       = t.industryname,
             t2.sectorcode         = t.sectorcode,
             t2.updatetime         = t.updatetime,
             t2.jsid               = t.jsid,
             t2.firstindustrycode  = t.firstindustrycode,
             t2.firstindustryname  = t.firstindustryname,
             t2.secondindustrycode = t.secondindustrycode,
             t2.secondindustryname = t.secondindustryname,
             t2.thirdindustrycode  = t.thirdindustrycode,
             t2.thirdindustryname  = t.thirdindustryname,
             t2.fourthindustrycode = t.fourthindustrycode,
             t2.fourthindustryname = t.fourthindustryname,
             t2.industrynamee      = t.industrynamee
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.STANDARD,
         t2.INDUSTRYNUM,
         t2.CLASSIFICATION,
         t2.INDUSTRYCODE,
         t2.INDUSTRYNAME,
         t2.SECTORCODE,
         t2.UPDATETIME,
         t2.JSID,
         t2.FIRSTINDUSTRYCODE,
         t2.FIRSTINDUSTRYNAME,
         t2.SECONDINDUSTRYCODE,
         t2.SECONDINDUSTRYNAME,
         t2.THIRDINDUSTRYCODE,
         t2.THIRDINDUSTRYNAME,
         t2.FOURTHINDUSTRYCODE,
         t2.FOURTHINDUSTRYNAME,
         t2.INDUSTRYNAMEE)
      VALUES
        (t.ID,
         t.STANDARD,
         t.INDUSTRYNUM,
         t.CLASSIFICATION,
         t.INDUSTRYCODE,
         t.INDUSTRYNAME,
         t.SECTORCODE,
         t.UPDATETIME,
         t.JSID,
         t.FIRSTINDUSTRYCODE,
         t.FIRSTINDUSTRYNAME,
         t.SECONDINDUSTRYCODE,
         t.SECONDINDUSTRYNAME,
         t.THIRDINDUSTRYCODE,
         t.THIRDINDUSTRYNAME,
         t.FOURTHINDUSTRYCODE,
         t.FOURTHINDUSTRYNAME,
         t.INDUSTRYNAMEE);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_CT_INDUSTRYTYPE;

  --日行情表
  procedure cj_MIS_QT_DAILYQUOTE(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_QT_DAILYQUOTE t2
    USING t_MIS_QT_DAILYQUOTE t
    ON (t.InnerCode = t2.InnerCode and t.TradingDay = t2.TradingDay)
    WHEN MATCHED THEN
      UPDATE
         SET t2.prevcloseprice = t.prevcloseprice,
             t2.openprice      = t.openprice,
             t2.highprice      = t.highprice,
             t2.lowprice       = t.lowprice,
             t2.closeprice     = t.closeprice,
             t2.turnovervolume = t.turnovervolume,
             t2.turnovervalue  = t.turnovervalue,
             t2.turnoverdeals  = t.turnoverdeals,
             t2.xgrq           = t.xgrq,
             t2.jsid           = t.jsid
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.TRADINGDAY,
         t2.PREVCLOSEPRICE,
         t2.OPENPRICE,
         t2.HIGHPRICE,
         t2.LOWPRICE,
         t2.CLOSEPRICE,
         t2.TURNOVERVOLUME,
         t2.TURNOVERVALUE,
         t2.TURNOVERDEALS,
         t2.XGRQ,
         t2.JSID)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.TRADINGDAY,
         t.PREVCLOSEPRICE,
         t.OPENPRICE,
         t.HIGHPRICE,
         t.LOWPRICE,
         t.CLOSEPRICE,
         t.TURNOVERVOLUME,
         t.TURNOVERVALUE,
         t.TURNOVERDEALS,
         t.XGRQ,
         t.JSID);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_QT_DAILYQUOTE;

  --指数行情
  procedure cj_MIS_QT_INDEXQUOTE(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_QT_INDEXQUOTE t2
    USING t_MIS_QT_INDEXQUOTE t
    ON (t.InnerCode = t2.InnerCode and t.TradingDay = t2.TradingDay)
    WHEN MATCHED THEN
      UPDATE
         SET t2.prevcloseprice = t.prevcloseprice,
             t2.openprice      = t.openprice,
             t2.highprice      = t.highprice,
             t2.lowprice       = t.lowprice,
             t2.closeprice     = t.closeprice,
             t2.turnovervolume = t.turnovervolume,
             t2.turnovervalue  = t.turnovervalue,
             t2.turnoverdeals  = t.turnoverdeals,
             t2.changepct      = t.changepct,
             t2.xgrq           = t.xgrq,
             t2.jsid           = t.jsid,
             t2.negotiablemv   = t.negotiablemv
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.TRADINGDAY,
         t2.PREVCLOSEPRICE,
         t2.OPENPRICE,
         t2.HIGHPRICE,
         t2.LOWPRICE,
         t2.CLOSEPRICE,
         t2.TURNOVERVOLUME,
         t2.TURNOVERVALUE,
         t2.TURNOVERDEALS,
         t2.CHANGEPCT,
         t2.XGRQ,
         t2.JSID,
         t2.NEGOTIABLEMV)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.TRADINGDAY,
         t.PREVCLOSEPRICE,
         t.OPENPRICE,
         t.HIGHPRICE,
         t.LOWPRICE,
         t.CLOSEPRICE,
         t.TURNOVERVOLUME,
         t.TURNOVERVALUE,
         t.TURNOVERDEALS,
         t.CHANGEPCT,
         t.XGRQ,
         t.JSID,
         t.NEGOTIABLEMV);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_QT_INDEXQUOTE;

  --A股配股
  procedure cj_MIS_LC_ASHAREPLACEMENT(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_LC_ASHAREPLACEMENT t2
    USING t_MIS_LC_ASHAREPLACEMENT t
    ON (t.InnerCode = t2.InnerCode and t.InitialInfoPublDate = t2.InitialInfoPublDate)
    WHEN MATCHED THEN
      UPDATE
         SET t2.playear                  = t.playear,
             t2.stocktype                = t.stocktype,
             t2.advancedate              = t.advancedate,
             t2.smdecipubldate           = t.smdecipubldate,
             t2.pricingmodel             = t.pricingmodel,
             t2.pricingdescription       = t.pricingdescription,
             t2.advancevalidstartdate    = t.advancevalidstartdate,
             t2.advancevalidenddate      = t.advancevalidenddate,
             t2.plaratioplanned          = t.plaratioplanned,
             t2.plapriceceiling          = t.plapriceceiling,
             t2.plapricefloor            = t.plapricefloor,
             t2.decipubldate             = t.decipubldate,
             t2.plaprospectuspubldate    = t.plaprospectuspubldate,
             t2.plaabbrname              = t.plaabbrname,
             t2.placode                  = t.placode,
             t2.baseshares               = t.baseshares,
             t2.plannedplavol            = t.plannedplavol,
             t2.actualplaratio           = t.actualplaratio,
             t2.actualplavol             = t.actualplavol,
             t2.plaobject                = t.plaobject,
             t2.parvalue                 = t.parvalue,
             t2.plaprice                 = t.plaprice,
             t2.transferplaratio         = t.transferplaratio,
             t2.transferplavol           = t.transferplavol,
             t2.transferfeepershare      = t.transferfeepershare,
             t2.oddlotstreatment         = t.oddlotstreatment,
             t2.plaproceeds              = t.plaproceeds,
             t2.placost                  = t.placost,
             t2.underwritingfee          = t.underwritingfee,
             t2.cpafee                   = t.cpafee,
             t2.assetappraisalfee        = t.assetappraisalfee,
             t2.landevaluationfee        = t.landevaluationfee,
             t2.attorneyfee              = t.attorneyfee,
             t2.totalagentfee            = t.totalagentfee,
             t2.onlineissuefee           = t.onlineissuefee,
             t2.scripfee                 = t.scripfee,
             t2.sponsorfee               = t.sponsorfee,
             t2.otherfee                 = t.otherfee,
             t2.planetproceeds           = t.planetproceeds,
             t2.rightregdate             = t.rightregdate,
             t2.exrightdate              = t.exrightdate,
             t2.paystartdate             = t.paystartdate,
             t2.payenddate               = t.payenddate,
             t2.datetoaccount            = t.datetoaccount,
             t2.moneytoaccount           = t.moneytoaccount,
             t2.plalistdate              = t.plalistdate,
             t2.largeshsubsstatement     = t.largeshsubsstatement,
             t2.schemechange             = t.schemechange,
             t2.changestatement          = t.changestatement,
             t2.underwritingmode         = t.underwritingmode,
             t2.underwriterboughtvol     = t.underwriterboughtvol,
             t2.publicshsubscriptionesti = t.publicshsubscriptionesti,
             t2.publicshsubscriptionactu = t.publicshsubscriptionactu,
             t2.xgrq                     = t.xgrq,
             t2.jsid                     = t.jsid,
             t2.plannedplaproceeds       = t.plannedplaproceeds
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.INITIALINFOPUBLDATE,
         t2.PLAYEAR,
         t2.STOCKTYPE,
         t2.ADVANCEDATE,
         t2.SMDECIPUBLDATE,
         t2.PRICINGMODEL,
         t2.PRICINGDESCRIPTION,
         t2.ADVANCEVALIDSTARTDATE,
         t2.ADVANCEVALIDENDDATE,
         t2.PLARATIOPLANNED,
         t2.PLAPRICECEILING,
         t2.PLAPRICEFLOOR,
         t2.DECIPUBLDATE,
         t2.PLAPROSPECTUSPUBLDATE,
         t2.PLAABBRNAME,
         t2.PLACODE,
         t2.BASESHARES,
         t2.PLANNEDPLAVOL,
         t2.ACTUALPLARATIO,
         t2.ACTUALPLAVOL,
         t2.PLAOBJECT,
         t2.PARVALUE,
         t2.PLAPRICE,
         t2.TRANSFERPLARATIO,
         t2.TRANSFERPLAVOL,
         t2.TRANSFERFEEPERSHARE,
         t2.ODDLOTSTREATMENT,
         t2.PLAPROCEEDS,
         t2.PLACOST,
         t2.UNDERWRITINGFEE,
         t2.CPAFEE,
         t2.ASSETAPPRAISALFEE,
         t2.LANDEVALUATIONFEE,
         t2.ATTORNEYFEE,
         t2.TOTALAGENTFEE,
         t2.ONLINEISSUEFEE,
         t2.SCRIPFEE,
         t2.SPONSORFEE,
         t2.OTHERFEE,
         t2.PLANETPROCEEDS,
         t2.RIGHTREGDATE,
         t2.EXRIGHTDATE,
         t2.PAYSTARTDATE,
         t2.PAYENDDATE,
         t2.DATETOACCOUNT,
         t2.MONEYTOACCOUNT,
         t2.PLALISTDATE,
         t2.LARGESHSUBSSTATEMENT,
         t2.SCHEMECHANGE,
         t2.CHANGESTATEMENT,
         t2.UNDERWRITINGMODE,
         t2.UNDERWRITERBOUGHTVOL,
         t2.PUBLICSHSUBSCRIPTIONESTI,
         t2.PUBLICSHSUBSCRIPTIONACTU,
         t2.XGRQ,
         t2.JSID,
         t2.PLANNEDPLAPROCEEDS)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.INITIALINFOPUBLDATE,
         t.PLAYEAR,
         t.STOCKTYPE,
         t.ADVANCEDATE,
         t.SMDECIPUBLDATE,
         t.PRICINGMODEL,
         t.PRICINGDESCRIPTION,
         t.ADVANCEVALIDSTARTDATE,
         t.ADVANCEVALIDENDDATE,
         t.PLARATIOPLANNED,
         t.PLAPRICECEILING,
         t.PLAPRICEFLOOR,
         t.DECIPUBLDATE,
         t.PLAPROSPECTUSPUBLDATE,
         t.PLAABBRNAME,
         t.PLACODE,
         t.BASESHARES,
         t.PLANNEDPLAVOL,
         t.ACTUALPLARATIO,
         t.ACTUALPLAVOL,
         t.PLAOBJECT,
         t.PARVALUE,
         t.PLAPRICE,
         t.TRANSFERPLARATIO,
         t.TRANSFERPLAVOL,
         t.TRANSFERFEEPERSHARE,
         t.ODDLOTSTREATMENT,
         t.PLAPROCEEDS,
         t.PLACOST,
         t.UNDERWRITINGFEE,
         t.CPAFEE,
         t.ASSETAPPRAISALFEE,
         t.LANDEVALUATIONFEE,
         t.ATTORNEYFEE,
         t.TOTALAGENTFEE,
         t.ONLINEISSUEFEE,
         t.SCRIPFEE,
         t.SPONSORFEE,
         t.OTHERFEE,
         t.PLANETPROCEEDS,
         t.RIGHTREGDATE,
         t.EXRIGHTDATE,
         t.PAYSTARTDATE,
         t.PAYENDDATE,
         t.DATETOACCOUNT,
         t.MONEYTOACCOUNT,
         t.PLALISTDATE,
         t.LARGESHSUBSSTATEMENT,
         t.SCHEMECHANGE,
         t.CHANGESTATEMENT,
         t.UNDERWRITINGMODE,
         t.UNDERWRITERBOUGHTVOL,
         t.PUBLICSHSUBSCRIPTIONESTI,
         t.PUBLICSHSUBSCRIPTIONACTU,
         t.XGRQ,
         t.JSID,
         t.PLANNEDPLAPROCEEDS);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_LC_ASHAREPLACEMENT;

  --公募基金发行与上市
  procedure cj_MIS_MF_ISSUEANDLISTING(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_MF_ISSUEANDLISTING t2
    USING t_MIS_MF_ISSUEANDLISTING t
    ON (t.InnerCode = t2.InnerCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.initialinfopubldate       = t.initialinfopubldate,
             t2.prospectusissueddate      = t.prospectusissueddate,
             t2.listannouncementissuedate = t.listannouncementissuedate,
             t2.fundraisingmethod         = t.fundraisingmethod,
             t2.fundtype                  = t.fundtype,
             t2.issueobject               = t.issueobject,
             t2.issuestartdate            = t.issuestartdate,
             t2.issueenddate              = t.issueenddate,
             t2.parvalue                  = t.parvalue,
             t2.unitissueprice            = t.unitissueprice,
             t2.unitissuefee              = t.unitissuefee,
             t2.shareissued               = t.shareissued,
             t2.initiatorsubscribevolume  = t.initiatorsubscribevolume,
             t2.initiatorholdfloatshares  = t.initiatorholdfloatshares,
             t2.initiatorholdterm         = t.initiatorholdterm,
             t2.miniinitiatorholdingratio = t.miniinitiatorholdingratio,
             t2.finstitutionquota         = t.finstitutionquota,
             t2.publicoffershares         = t.publicoffershares,
             t2.generallegalpersonquota   = t.generallegalpersonquota,
             t2.abbrnameforapplying       = t.abbrnameforapplying,
             t2.applyingcode              = t.applyingcode,
             t2.applyingunit              = t.applyingunit,
             t2.minimumapplying           = t.minimumapplying,
             t2.maximumapplying           = t.maximumapplying,
             t2.applyingtimes             = t.applyingtimes,
             t2.validlapplyingaccounts    = t.validlapplyingaccounts,
             t2.validapplyingvol          = t.validapplyingvol,
             t2.overapplyingmultiples     = t.overapplyingmultiples,
             t2.freezefunds               = t.freezefunds,
             t2.hitratio                  = t.hitratio,
             t2.listeddate                = t.listeddate,
             t2.listedplace               = t.listedplace,
             t2.outstandingshares         = t.outstandingshares,
             t2.firstdayopenprice         = t.firstdayopenprice,
             t2.firstdaycolseprice        = t.firstdaycolseprice,
             t2.firstdayturnoverratio     = t.firstdayturnoverratio,
             t2.establishmentdate         = t.establishmentdate,
             t2.applyopeningdate          = t.applyopeningdate,
             t2.redeemopeningdate         = t.redeemopeningdate,
             t2.xgrq                      = t.xgrq,
             t2.jsid                      = t.jsid,
             t2.currencycode              = t.currencycode,
             t2.currencystyle             = t.currencystyle,
             t2.issuestate                = t.issuestate,
             t2.issuecanceldate           = t.issuecanceldate
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.INITIALINFOPUBLDATE,
         t2.PROSPECTUSISSUEDDATE,
         t2.LISTANNOUNCEMENTISSUEDATE,
         t2.FUNDRAISINGMETHOD,
         t2.FUNDTYPE,
         t2.ISSUEOBJECT,
         t2.ISSUESTARTDATE,
         t2.ISSUEENDDATE,
         t2.PARVALUE,
         t2.UNITISSUEPRICE,
         t2.UNITISSUEFEE,
         t2.SHAREISSUED,
         t2.INITIATORSUBSCRIBEVOLUME,
         t2.INITIATORHOLDFLOATSHARES,
         t2.INITIATORHOLDTERM,
         t2.MINIINITIATORHOLDINGRATIO,
         t2.FINSTITUTIONQUOTA,
         t2.PUBLICOFFERSHARES,
         t2.GENERALLEGALPERSONQUOTA,
         t2.ABBRNAMEFORAPPLYING,
         t2.APPLYINGCODE,
         t2.APPLYINGUNIT,
         t2.MINIMUMAPPLYING,
         t2.MAXIMUMAPPLYING,
         t2.APPLYINGTIMES,
         t2.VALIDLAPPLYINGACCOUNTS,
         t2.VALIDAPPLYINGVOL,
         t2.OVERAPPLYINGMULTIPLES,
         t2.FREEZEFUNDS,
         t2.HITRATIO,
         t2.LISTEDDATE,
         t2.LISTEDPLACE,
         t2.OUTSTANDINGSHARES,
         t2.FIRSTDAYOPENPRICE,
         t2.FIRSTDAYCOLSEPRICE,
         t2.FIRSTDAYTURNOVERRATIO,
         t2.ESTABLISHMENTDATE,
         t2.APPLYOPENINGDATE,
         t2.REDEEMOPENINGDATE,
         t2.XGRQ,
         t2.JSID,
         t2.CURRENCYCODE,
         t2.CURRENCYSTYLE,
         t2.ISSUESTATE,
         t2.ISSUECANCELDATE)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.INITIALINFOPUBLDATE,
         t.PROSPECTUSISSUEDDATE,
         t.LISTANNOUNCEMENTISSUEDATE,
         t.FUNDRAISINGMETHOD,
         t.FUNDTYPE,
         t.ISSUEOBJECT,
         t.ISSUESTARTDATE,
         t.ISSUEENDDATE,
         t.PARVALUE,
         t.UNITISSUEPRICE,
         t.UNITISSUEFEE,
         t.SHAREISSUED,
         t.INITIATORSUBSCRIBEVOLUME,
         t.INITIATORHOLDFLOATSHARES,
         t.INITIATORHOLDTERM,
         t.MINIINITIATORHOLDINGRATIO,
         t.FINSTITUTIONQUOTA,
         t.PUBLICOFFERSHARES,
         t.GENERALLEGALPERSONQUOTA,
         t.ABBRNAMEFORAPPLYING,
         t.APPLYINGCODE,
         t.APPLYINGUNIT,
         t.MINIMUMAPPLYING,
         t.MAXIMUMAPPLYING,
         t.APPLYINGTIMES,
         t.VALIDLAPPLYINGACCOUNTS,
         t.VALIDAPPLYINGVOL,
         t.OVERAPPLYINGMULTIPLES,
         t.FREEZEFUNDS,
         t.HITRATIO,
         t.LISTEDDATE,
         t.LISTEDPLACE,
         t.OUTSTANDINGSHARES,
         t.FIRSTDAYOPENPRICE,
         t.FIRSTDAYCOLSEPRICE,
         t.FIRSTDAYTURNOVERRATIO,
         t.ESTABLISHMENTDATE,
         t.APPLYOPENINGDATE,
         t.REDEEMOPENINGDATE,
         t.XGRQ,
         t.JSID,
         t.CURRENCYCODE,
         t.CURRENCYSTYLE,
         t.ISSUESTATE,
         t.ISSUECANCELDATE);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_MF_ISSUEANDLISTING;

  --可转债基本信息
  procedure cj_MIS_BOND_CONBDBASICINFO(errcode out int,
                                       errmsg  out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_BOND_CONBDBASICINFO t2
    USING t_MIS_BOND_CONBDBASICINFO t
    ON (t.InnerCode = t2.InnerCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.secucode              = t.secucode,
             t2.secuabbr              = t.secuabbr,
             t2.bondform              = t.bondform,
             t2.cbembeddedoption      = t.cbembeddedoption,
             t2.initialinfopubldate   = t.initialinfopubldate,
             t2.guarantor             = t.guarantor,
             t2.cras                  = t.cras,
             t2.creditrating          = t.creditrating,
             t2.leadunderwriter       = t.leadunderwriter,
             t2.maturity              = t.maturity,
             t2.startdate             = t.startdate,
             t2.enddate               = t.enddate,
             t2.parvalue              = t.parvalue,
             t2.issuevolume           = t.issuevolume,
             t2.actualissuesize       = t.actualissuesize,
             t2.compoundmethod        = t.compoundmethod,
             t2.intpaymentmethod      = t.intpaymentmethod,
             t2.couponfreq            = t.couponfreq,
             t2.couponrate            = t.couponrate,
             t2.incrscouponmargin     = t.incrscouponmargin,
             t2.frnmargin             = t.frnmargin,
             t2.frnrefratetext        = t.frnrefratetext,
             t2.minannualrate         = t.minannualrate,
             t2.intpaymentdescription = t.intpaymentdescription,
             t2.listeddate            = t.listeddate,
             t2.listedenddate         = t.listedenddate,
             t2.tradeenddate          = t.tradeenddate,
             t2.lasttradingdate       = t.lasttradingdate,
             t2.delistdate            = t.delistdate,
             t2.convstkabbrname       = t.convstkabbrname,
             t2.convcode              = t.convcode,
             t2.convtermstartdate     = t.convtermstartdate,
             t2.convtermenddate       = t.convtermenddate,
             t2.stopconvdate          = t.stopconvdate,
             t2.callprotection        = t.callprotection,
             t2.updatetime            = t.updatetime,
             t2.jsid                  = t.jsid,
             t2.multiguarantor        = t.multiguarantor,
             t2.sponsor               = t.sponsor,
             t2.interestitem          = t.interestitem,
             t2.interestformula       = t.interestformula,
             t2.rateiffloat           = t.rateiffloat,
             t2.rateadjustmode        = t.rateadjustmode,
             t2.redeemofinterest      = t.redeemofinterest,
             t2.callpriceend          = t.callpriceend,
             t2.primalguarratio       = t.primalguarratio
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.SECUCODE,
         t2.SECUABBR,
         t2.BONDFORM,
         t2.CBEMBEDDEDOPTION,
         t2.INITIALINFOPUBLDATE,
         t2.GUARANTOR,
         t2.CRAS,
         t2.CREDITRATING,
         t2.LEADUNDERWRITER,
         t2.MATURITY,
         t2.STARTDATE,
         t2.ENDDATE,
         t2.PARVALUE,
         t2.ISSUEVOLUME,
         t2.ACTUALISSUESIZE,
         t2.COMPOUNDMETHOD,
         t2.INTPAYMENTMETHOD,
         t2.COUPONFREQ,
         t2.COUPONRATE,
         t2.INCRSCOUPONMARGIN,
         t2.FRNMARGIN,
         t2.FRNREFRATETEXT,
         t2.MINANNUALRATE,
         t2.INTPAYMENTDESCRIPTION,
         t2.LISTEDDATE,
         t2.LISTEDENDDATE,
         t2.TRADEENDDATE,
         t2.LASTTRADINGDATE,
         t2.DELISTDATE,
         t2.CONVSTKABBRNAME,
         t2.CONVCODE,
         t2.CONVTERMSTARTDATE,
         t2.CONVTERMENDDATE,
         t2.STOPCONVDATE,
         t2.CALLPROTECTION,
         t2.UPDATETIME,
         t2.JSID,
         t2.MULTIGUARANTOR,
         t2.SPONSOR,
         t2.INTERESTITEM,
         t2.INTERESTFORMULA,
         t2.RATEIFFLOAT,
         t2.RATEADJUSTMODE,
         t2.REDEEMOFINTEREST,
         t2.CALLPRICEEND,
         t2.PRIMALGUARRATIO)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.SECUCODE,
         t.SECUABBR,
         t.BONDFORM,
         t.CBEMBEDDEDOPTION,
         t.INITIALINFOPUBLDATE,
         t.GUARANTOR,
         t.CRAS,
         t.CREDITRATING,
         t.LEADUNDERWRITER,
         t.MATURITY,
         t.STARTDATE,
         t.ENDDATE,
         t.PARVALUE,
         t.ISSUEVOLUME,
         t.ACTUALISSUESIZE,
         t.COMPOUNDMETHOD,
         t.INTPAYMENTMETHOD,
         t.COUPONFREQ,
         t.COUPONRATE,
         t.INCRSCOUPONMARGIN,
         t.FRNMARGIN,
         t.FRNREFRATETEXT,
         t.MINANNUALRATE,
         t.INTPAYMENTDESCRIPTION,
         t.LISTEDDATE,
         t.LISTEDENDDATE,
         t.TRADEENDDATE,
         t.LASTTRADINGDATE,
         t.DELISTDATE,
         t.CONVSTKABBRNAME,
         t.CONVCODE,
         t.CONVTERMSTARTDATE,
         t.CONVTERMENDDATE,
         t.STOPCONVDATE,
         t.CALLPROTECTION,
         t.UPDATETIME,
         t.JSID,
         t.MULTIGUARANTOR,
         t.SPONSOR,
         t.INTERESTITEM,
         t.INTERESTFORMULA,
         t.RATEIFFLOAT,
         t.RATEADJUSTMODE,
         t.REDEEMOFINTEREST,
         t.CALLPRICEEND,
         t.PRIMALGUARRATIO);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_BOND_CONBDBASICINFO;

  --可转债发行信息
  procedure cj_MIS_BOND_CONBDISSUE(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_BOND_CONBDISSUE t2
    USING t_MIS_BOND_CONBDISSUE t
    ON (t.InnerCode = t2.InnerCode and t.issuer is not null and t.updatetime is not null)
    WHEN MATCHED THEN
      UPDATE
         SET t2.issuer                      = t.issuer,
             t2.bondnature                  = t.bondnature,
             t2.prospectusissuedate         = t.prospectusissuedate,
             t2.issuemethod                 = t.issuemethod,
             t2.issueobject                 = t.issueobject,
             t2.parvalue                    = t.parvalue,
             t2.issueprice                  = t.issueprice,
             t2.issuestartdate              = t.issuestartdate,
             t2.issueenddate                = t.issueenddate,
             t2.projectchangingdes          = t.projectchangingdes,
             t2.projectchangingtype         = t.projectchangingtype,
             t2.guarantor                   = t.guarantor,
             t2.leadunderwriter             = t.leadunderwriter,
             t2.cras                        = t.cras,
             t2.creditrating                = t.creditrating,
             t2.underwritingmethod          = t.underwritingmethod,
             t2.underwritingstartdate       = t.underwritingstartdate,
             t2.underwritingenddate         = t.underwritingenddate,
             t2.prefaltcodeh                = t.prefaltcodeh,
             t2.prefaltnameh                = t.prefaltnameh,
             t2.prefaltregdateh             = t.prefaltregdateh,
             t2.prefaltpaystartdateh        = t.prefaltpaystartdateh,
             t2.prefaltpayenddateh          = t.prefaltpayenddateh,
             t2.prefaltproportionh          = t.prefaltproportionh,
             t2.prefaltunith                = t.prefaltunith,
             t2.prefaltsizeh                = t.prefaltsizeh,
             t2.prefaltvalidapplyvolh       = t.prefaltvalidapplyvolh,
             t2.prefaltvalidapplynumh       = t.prefaltvalidapplynumh,
             t2.prefaltoversubstimeh        = t.prefaltoversubstimeh,
             t2.prefaltratioh               = t.prefaltratioh,
             t2.altsizef                    = t.altsizef,
             t2.altpaystartdatef            = t.altpaystartdatef,
             t2.altpayenddatef              = t.altpayenddatef,
             t2.altsizei                    = t.altsizei,
             t2.altpaystartdatei            = t.altpaystartdatei,
             t2.altpayenddatei              = t.altpayenddatei,
             t2.offlinepuboffvalidapplyvoli = t.offlinepuboffvalidapplyvoli,
             t2.offlinepuboffvalidapplynumi = t.offlinepuboffvalidapplynumi,
             t2.offlinepuboffoversubstimei  = t.offlinepuboffoversubstimei,
             t2.offlinepuboffratioi         = t.offlinepuboffratioi,
             t2.onlinepuboffcode            = t.onlinepuboffcode,
             t2.onlinepuboffname            = t.onlinepuboffname,
             t2.onlinepuboffdate            = t.onlinepuboffdate,
             t2.onlinepuboffsize            = t.onlinepuboffsize,
             t2.onlinepuboffunit            = t.onlinepuboffunit,
             t2.onlinepuboffapplycap        = t.onlinepuboffapplycap,
             t2.onlinepuboffvalidapplyvol   = t.onlinepuboffvalidapplyvol,
             t2.onlinepuboffvalidapplynum   = t.onlinepuboffvalidapplynum,
             t2.onlinepuboffoversubstime    = t.onlinepuboffoversubstime,
             t2.onlinepuboffratio           = t.onlinepuboffratio,
             t2.actualissuesize             = t.actualissuesize,
             t2.issuevol                    = t.issuevol,
             t2.proceeds                    = t.proceeds,
             t2.issuecost                   = t.issuecost,
             t2.netproceeds                 = t.netproceeds,
             t2.underwriterboughtvolume     = t.underwriterboughtvolume,
             t2.listannouncementdate        = t.listannouncementdate,
             t2.exchangeday                 = t.exchangeday,
             t2.profitforecastyear          = t.profitforecastyear,
             t2.revenueforecast             = t.revenueforecast,
             t2.netprofitforecast           = t.netprofitforecast,
             t2.diluteepsforecast           = t.diluteepsforecast,
             t2.updatetime                  = t.updatetime,
             t2.jsid                        = t.jsid,
             t2.onlinepuboffapplymin        = t.onlinepuboffapplymin,
             t2.offlineapplyunit            = t.offlineapplyunit,
             t2.offlineapplymax             = t.offlineapplymax,
             t2.offlineapplymin             = t.offlineapplymin,
             t2.depositratio                = t.depositratio,
             t2.depositnum                  = t.depositnum,
             t2.depositenddate              = t.depositenddate,
             t2.planissuesize               = t.planissuesize
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.ISSUER,
         t2.BONDNATURE,
         t2.PROSPECTUSISSUEDATE,
         t2.ISSUEMETHOD,
         t2.ISSUEOBJECT,
         t2.PARVALUE,
         t2.ISSUEPRICE,
         t2.ISSUESTARTDATE,
         t2.ISSUEENDDATE,
         t2.PROJECTCHANGINGDES,
         t2.PROJECTCHANGINGTYPE,
         t2.GUARANTOR,
         t2.LEADUNDERWRITER,
         t2.CRAS,
         t2.CREDITRATING,
         t2.UNDERWRITINGMETHOD,
         t2.UNDERWRITINGSTARTDATE,
         t2.UNDERWRITINGENDDATE,
         t2.PREFALTCODEH,
         t2.PREFALTNAMEH,
         t2.PREFALTREGDATEH,
         t2.PREFALTPAYSTARTDATEH,
         t2.PREFALTPAYENDDATEH,
         t2.PREFALTPROPORTIONH,
         t2.PREFALTUNITH,
         t2.PREFALTSIZEH,
         t2.PREFALTVALIDAPPLYVOLH,
         t2.PREFALTVALIDAPPLYNUMH,
         t2.PREFALTOVERSUBSTIMEH,
         t2.PREFALTRATIOH,
         t2.ALTSIZEF,
         t2.ALTPAYSTARTDATEF,
         t2.ALTPAYENDDATEF,
         t2.ALTSIZEI,
         t2.ALTPAYSTARTDATEI,
         t2.ALTPAYENDDATEI,
         t2.OFFLINEPUBOFFVALIDAPPLYVOLI,
         t2.OFFLINEPUBOFFVALIDAPPLYNUMI,
         t2.OFFLINEPUBOFFOVERSUBSTIMEI,
         t2.OFFLINEPUBOFFRATIOI,
         t2.ONLINEPUBOFFCODE,
         t2.ONLINEPUBOFFNAME,
         t2.ONLINEPUBOFFDATE,
         t2.ONLINEPUBOFFSIZE,
         t2.ONLINEPUBOFFUNIT,
         t2.ONLINEPUBOFFAPPLYCAP,
         t2.ONLINEPUBOFFVALIDAPPLYVOL,
         t2.ONLINEPUBOFFVALIDAPPLYNUM,
         t2.ONLINEPUBOFFOVERSUBSTIME,
         t2.ONLINEPUBOFFRATIO,
         t2.ACTUALISSUESIZE,
         t2.ISSUEVOL,
         t2.PROCEEDS,
         t2.ISSUECOST,
         t2.NETPROCEEDS,
         t2.UNDERWRITERBOUGHTVOLUME,
         t2.LISTANNOUNCEMENTDATE,
         t2.EXCHANGEDAY,
         t2.PROFITFORECASTYEAR,
         t2.REVENUEFORECAST,
         t2.NETPROFITFORECAST,
         t2.DILUTEEPSFORECAST,
         t2.UPDATETIME,
         t2.JSID,
         t2.ONLINEPUBOFFAPPLYMIN,
         t2.OFFLINEAPPLYUNIT,
         t2.OFFLINEAPPLYMAX,
         t2.OFFLINEAPPLYMIN,
         t2.DEPOSITRATIO,
         t2.DEPOSITNUM,
         t2.DEPOSITENDDATE,
         t2.PLANISSUESIZE)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.ISSUER,
         t.BONDNATURE,
         t.PROSPECTUSISSUEDATE,
         t.ISSUEMETHOD,
         t.ISSUEOBJECT,
         t.PARVALUE,
         t.ISSUEPRICE,
         t.ISSUESTARTDATE,
         t.ISSUEENDDATE,
         t.PROJECTCHANGINGDES,
         t.PROJECTCHANGINGTYPE,
         t.GUARANTOR,
         t.LEADUNDERWRITER,
         t.CRAS,
         t.CREDITRATING,
         t.UNDERWRITINGMETHOD,
         t.UNDERWRITINGSTARTDATE,
         t.UNDERWRITINGENDDATE,
         t.PREFALTCODEH,
         t.PREFALTNAMEH,
         t.PREFALTREGDATEH,
         t.PREFALTPAYSTARTDATEH,
         t.PREFALTPAYENDDATEH,
         t.PREFALTPROPORTIONH,
         t.PREFALTUNITH,
         t.PREFALTSIZEH,
         t.PREFALTVALIDAPPLYVOLH,
         t.PREFALTVALIDAPPLYNUMH,
         t.PREFALTOVERSUBSTIMEH,
         t.PREFALTRATIOH,
         t.ALTSIZEF,
         t.ALTPAYSTARTDATEF,
         t.ALTPAYENDDATEF,
         t.ALTSIZEI,
         t.ALTPAYSTARTDATEI,
         t.ALTPAYENDDATEI,
         t.OFFLINEPUBOFFVALIDAPPLYVOLI,
         t.OFFLINEPUBOFFVALIDAPPLYNUMI,
         t.OFFLINEPUBOFFOVERSUBSTIMEI,
         t.OFFLINEPUBOFFRATIOI,
         t.ONLINEPUBOFFCODE,
         t.ONLINEPUBOFFNAME,
         t.ONLINEPUBOFFDATE,
         t.ONLINEPUBOFFSIZE,
         t.ONLINEPUBOFFUNIT,
         t.ONLINEPUBOFFAPPLYCAP,
         t.ONLINEPUBOFFVALIDAPPLYVOL,
         t.ONLINEPUBOFFVALIDAPPLYNUM,
         t.ONLINEPUBOFFOVERSUBSTIME,
         t.ONLINEPUBOFFRATIO,
         t.ACTUALISSUESIZE,
         t.ISSUEVOL,
         t.PROCEEDS,
         t.ISSUECOST,
         t.NETPROCEEDS,
         t.UNDERWRITERBOUGHTVOLUME,
         t.LISTANNOUNCEMENTDATE,
         t.EXCHANGEDAY,
         t.PROFITFORECASTYEAR,
         t.REVENUEFORECAST,
         t.NETPROFITFORECAST,
         t.DILUTEEPSFORECAST,
         t.UPDATETIME,
         t.JSID,
         t.ONLINEPUBOFFAPPLYMIN,
         t.OFFLINEAPPLYUNIT,
         t.OFFLINEAPPLYMAX,
         t.OFFLINEAPPLYMIN,
         t.DEPOSITRATIO,
         t.DEPOSITNUM,
         t.DEPOSITENDDATE,
         t.PLANISSUESIZE);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_BOND_CONBDISSUE;

  --券商专项资产收益计划表
  procedure cj_MIS_BOND_ABSBASICINFO(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_BOND_ABSBASICINFO t2
    USING t_MIS_BOND_ABSBASICINFO t
    ON (t.InnerCode = t2.InnerCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.bondfullname        = t.bondfullname,
             t2.initialpubdate      = t.initialpubdate,
             t2.csrcapprovaldate    = t.csrcapprovaldate,
             t2.originalbeneficiary = t.originalbeneficiary,
             t2.manager             = t.manager,
             t2.custody             = t.custody,
             t2.cras                = t.cras,
             t2.guarantor           = t.guarantor,
             t2.guarantmethod       = t.guarantmethod,
             t2.basicasset          = t.basicasset,
             t2.creditrating        = t.creditrating,
             t2.crremark            = t.crremark,
             t2.parvalue            = t.parvalue,
             t2.actualissuesize     = t.actualissuesize,
             t2.expanbeyield        = t.expanbeyield,
             t2.exanenyield         = t.exanenyield,
             t2.maturitystdate      = t.maturitystdate,
             t2.expectmaturity      = t.expectmaturity,
             t2.maturityunit        = t.maturityunit,
             t2.paymentmethodremark = t.paymentmethodremark,
             t2.paymentmethod       = t.paymentmethod,
             t2.issuepubdate        = t.issuepubdate,
             t2.popularizestdate    = t.popularizestdate,
             t2.popularizeeddate    = t.popularizeeddate,
             t2.issueobject         = t.issueobject,
             t2.issureprice         = t.issureprice,
             t2.leastbuysum         = t.leastbuysum,
             t2.subscriptionunit    = t.subscriptionunit,
             t2.inceptiondate       = t.inceptiondate,
             t2.regorg              = t.regorg,
             t2.transferstdate      = t.transferstdate,
             t2.transfereddate      = t.transfereddate,
             t2.leasttransum        = t.leasttransum,
             t2.transferunit        = t.transferunit,
             t2.exchange            = t.exchange,
             t2.updatetime          = t.updatetime,
             t2.jsid                = t.jsid,
             t2.maturityenddate     = t.maturityenddate,
             t2.projecttype         = t.projecttype,
             t2.redemptionregdate   = t.redemptionregdate,
             t2.redemptiondate      = t.redemptiondate,
             t2.prioritylevel       = t.prioritylevel,
             t2.earlymdate          = t.earlymdate,
             t2.basicassettype      = t.basicassettype,
             t2.prioritylevely      = t.prioritylevely,
             t2.opmaturity          = t.opmaturity,
             t2.rateadjustmode      = t.rateadjustmode,
             t2.frnmargin           = t.frnmargin,
             t2.frnrefrateper       = t.frnrefrateper,
             t2.frnrefratetext      = t.frnrefratetext,
             t2.compoundmethod      = t.compoundmethod,
             t2.interestformula     = t.interestformula,
             t2.interestitem        = t.interestitem,
             t2.currencyunit        = t.currencyunit,
             t2.bidenddate          = t.bidenddate,
             t2.conenddateoffline   = t.conenddateoffline
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INNERCODE,
         t2.BONDFULLNAME,
         t2.INITIALPUBDATE,
         t2.CSRCAPPROVALDATE,
         t2.ORIGINALBENEFICIARY,
         t2.MANAGER,
         t2.CUSTODY,
         t2.CRAS,
         t2.GUARANTOR,
         t2.GUARANTMETHOD,
         t2.BASICASSET,
         t2.CREDITRATING,
         t2.CRREMARK,
         t2.PARVALUE,
         t2.ACTUALISSUESIZE,
         t2.EXPANBEYIELD,
         t2.EXANENYIELD,
         t2.MATURITYSTDATE,
         t2.EXPECTMATURITY,
         t2.MATURITYUNIT,
         t2.PAYMENTMETHODREMARK,
         t2.PAYMENTMETHOD,
         t2.ISSUEPUBDATE,
         t2.POPULARIZESTDATE,
         t2.POPULARIZEEDDATE,
         t2.ISSUEOBJECT,
         t2.ISSUREPRICE,
         t2.LEASTBUYSUM,
         t2.SUBSCRIPTIONUNIT,
         t2.INCEPTIONDATE,
         t2.REGORG,
         t2.TRANSFERSTDATE,
         t2.TRANSFEREDDATE,
         t2.LEASTTRANSUM,
         t2.TRANSFERUNIT,
         t2.EXCHANGE,
         t2.UPDATETIME,
         t2.JSID,
         t2.MATURITYENDDATE,
         t2.PROJECTTYPE,
         t2.REDEMPTIONREGDATE,
         t2.REDEMPTIONDATE,
         t2.PRIORITYLEVEL,
         t2.EARLYMDATE,
         t2.BASICASSETTYPE,
         t2.PRIORITYLEVELY,
         t2.OPMATURITY,
         t2.RATEADJUSTMODE,
         t2.FRNMARGIN,
         t2.FRNREFRATEPER,
         t2.FRNREFRATETEXT,
         t2.COMPOUNDMETHOD,
         t2.INTERESTFORMULA,
         t2.INTERESTITEM,
         t2.CURRENCYUNIT,
         t2.BIDENDDATE,
         t2.CONENDDATEOFFLINE)
      VALUES
        (t.ID,
         t.INNERCODE,
         t.BONDFULLNAME,
         t.INITIALPUBDATE,
         t.CSRCAPPROVALDATE,
         t.ORIGINALBENEFICIARY,
         t.MANAGER,
         t.CUSTODY,
         t.CRAS,
         t.GUARANTOR,
         t.GUARANTMETHOD,
         t.BASICASSET,
         t.CREDITRATING,
         t.CRREMARK,
         t.PARVALUE,
         t.ACTUALISSUESIZE,
         t.EXPANBEYIELD,
         t.EXANENYIELD,
         t.MATURITYSTDATE,
         t.EXPECTMATURITY,
         t.MATURITYUNIT,
         t.PAYMENTMETHODREMARK,
         t.PAYMENTMETHOD,
         t.ISSUEPUBDATE,
         t.POPULARIZESTDATE,
         t.POPULARIZEEDDATE,
         t.ISSUEOBJECT,
         t.ISSUREPRICE,
         t.LEASTBUYSUM,
         t.SUBSCRIPTIONUNIT,
         t.INCEPTIONDATE,
         t.REGORG,
         t.TRANSFERSTDATE,
         t.TRANSFEREDDATE,
         t.LEASTTRANSUM,
         t.TRANSFERUNIT,
         t.EXCHANGE,
         t.UPDATETIME,
         t.JSID,
         t.MATURITYENDDATE,
         t.PROJECTTYPE,
         t.REDEMPTIONREGDATE,
         t.REDEMPTIONDATE,
         t.PRIORITYLEVEL,
         t.EARLYMDATE,
         t.BASICASSETTYPE,
         t.PRIORITYLEVELY,
         t.OPMATURITY,
         t.RATEADJUSTMODE,
         t.FRNMARGIN,
         t.FRNREFRATEPER,
         t.FRNREFRATETEXT,
         t.COMPOUNDMETHOD,
         t.INTERESTFORMULA,
         t.INTERESTITEM,
         t.CURRENCYUNIT,
         t.BIDENDDATE,
         t.CONENDDATEOFFLINE);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_BOND_ABSBASICINFO;

  --公募基金托管人概况
  procedure cj_MIS_MF_TRUSTEEOUTLINE(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_MF_TRUSTEEOUTLINE t2
    USING t_MIS_MF_TRUSTEEOUTLINE t
    ON (t.TrusteeCode = t2.TrusteeCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.trusteename        = t.trusteename,
             t2.legalrepr          = t.legalrepr,
             t2.trusteefunctionary = t.trusteefunctionary,
             t2.linkman            = t.linkman,
             t2.establishmentdate  = t.establishmentdate,
             t2.organizationform   = t.organizationform,
             t2.regcapital         = t.regcapital,
             t2.regaddr            = t.regaddr,
             t2.officeaddr         = t.officeaddr,
             t2.zipcode            = t.zipcode,
             t2.website            = t.website,
             t2.email              = t.email,
             t2.contactaddr        = t.contactaddr,
             t2.tel                = t.tel,
             t2.fax                = t.fax,
             t2.background         = t.background,
             t2.xgrq               = t.xgrq,
             t2.jsid               = t.jsid
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.TRUSTEECODE,
         t2.TRUSTEENAME,
         t2.LEGALREPR,
         t2.TRUSTEEFUNCTIONARY,
         t2.LINKMAN,
         t2.ESTABLISHMENTDATE,
         t2.ORGANIZATIONFORM,
         t2.REGCAPITAL,
         t2.REGADDR,
         t2.OFFICEADDR,
         t2.ZIPCODE,
         t2.WEBSITE,
         t2.EMAIL,
         t2.CONTACTADDR,
         t2.TEL,
         t2.FAX,
         t2.BACKGROUND,
         t2.XGRQ,
         t2.JSID)
      VALUES
        (t.ID,
         t.TRUSTEECODE,
         t.TRUSTEENAME,
         t.LEGALREPR,
         t.TRUSTEEFUNCTIONARY,
         t.LINKMAN,
         t.ESTABLISHMENTDATE,
         t.ORGANIZATIONFORM,
         t.REGCAPITAL,
         t.REGADDR,
         t.OFFICEADDR,
         t.ZIPCODE,
         t.WEBSITE,
         t.EMAIL,
         t.CONTACTADDR,
         t.TEL,
         t.FAX,
         t.BACKGROUND,
         t.XGRQ,
         t.JSID);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_MF_TRUSTEEOUTLINE;

  --指数基本情况
  procedure cj_MIS_LC_INDEXBASICINFO(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_LC_INDEXBASICINFO t2
    USING t_MIS_LC_INDEXBASICINFO t
    ON (t.IndexCode = t2.IndexCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.indextype         = t.indextype,
             t2.componenttype     = t.componenttype,
             t2.industrystandard  = t.industrystandard,
             t2.industrytype      = t.industrytype,
             t2.puborgcode        = t.puborgcode,
             t2.puborgname        = t.puborgname,
             t2.creatindexorgcode = t.creatindexorgcode,
             t2.creatindexorgname = t.creatindexorgname,
             t2.pubdate           = t.pubdate,
             t2.basedate          = t.basedate,
             t2.basepoint         = t.basepoint,
             t2.wamethod          = t.wamethod,
             t2.componentsum      = t.componentsum,
             t2.componentadperiod = t.componentadperiod,
             t2.indexremark       = t.indexremark,
             t2.enddate           = t.enddate,
             t2.xgrq              = t.xgrq,
             t2.jsid              = t.jsid,
             t2.indexpricetype    = t.indexpricetype,
             t2.indexdesigntype   = t.indexdesigntype,
             t2.relationship      = t.relationship,
             t2.relamainindexcode = t.relamainindexcode,
             t2.secumarket        = t.secumarket,
             t2.currencycode      = t.currencycode,
             t2.indexabstract     = t.indexabstract
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INDEXCODE,
         t2.INDEXTYPE,
         t2.COMPONENTTYPE,
         t2.INDUSTRYSTANDARD,
         t2.INDUSTRYTYPE,
         t2.PUBORGCODE,
         t2.PUBORGNAME,
         t2.CREATINDEXORGCODE,
         t2.CREATINDEXORGNAME,
         t2.PUBDATE,
         t2.BASEDATE,
         t2.BASEPOINT,
         t2.WAMETHOD,
         t2.COMPONENTSUM,
         t2.COMPONENTADPERIOD,
         t2.INDEXREMARK,
         t2.ENDDATE,
         t2.XGRQ,
         t2.JSID,
         t2.INDEXPRICETYPE,
         t2.INDEXDESIGNTYPE,
         t2.RELATIONSHIP,
         t2.RELAMAININDEXCODE,
         t2.SECUMARKET,
         t2.CURRENCYCODE,
         t2.INDEXABSTRACT)
      VALUES
        (t.ID,
         t.INDEXCODE,
         t.INDEXTYPE,
         t.COMPONENTTYPE,
         t.INDUSTRYSTANDARD,
         t.INDUSTRYTYPE,
         t.PUBORGCODE,
         t.PUBORGNAME,
         t.CREATINDEXORGCODE,
         t.CREATINDEXORGNAME,
         t.PUBDATE,
         t.BASEDATE,
         t.BASEPOINT,
         t.WAMETHOD,
         t.COMPONENTSUM,
         t.COMPONENTADPERIOD,
         t.INDEXREMARK,
         t.ENDDATE,
         t.XGRQ,
         t.JSID,
         t.INDEXPRICETYPE,
         t.INDEXDESIGNTYPE,
         t.RELATIONSHIP,
         t.RELAMAININDEXCODE,
         t.SECUMARKET,
         t.CURRENCYCODE,
         t.INDEXABSTRACT);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_LC_INDEXBASICINFO;

  --指数与行业对应
  procedure cj_MIS_LC_CORRINDEXINDUSTRY(errcode out int,
                                        errmsg  out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_LC_CORRINDEXINDUSTRY t2
    USING t_MIS_LC_CORRINDEXINDUSTRY t
    ON (t.IndexCode = t2.IndexCode and t.IndustryStandard = t2.IndustryStandard and t.IndustryCode = t2.IndustryCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.enddate    = t.enddate,
             t2.indexstate = t.indexstate,
             t2.updatetime = t.updatetime,
             t2.jsid       = t.jsid
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INDEXCODE,
         t2.INDUSTRYSTANDARD,
         t2.INDUSTRYCODE,
         t2.ENDDATE,
         t2.INDEXSTATE,
         t2.UPDATETIME,
         t2.JSID)
      VALUES
        (t.ID,
         t.INDEXCODE,
         t.INDUSTRYSTANDARD,
         t.INDUSTRYCODE,
         t.ENDDATE,
         t.INDEXSTATE,
         t.UPDATETIME,
         t.JSID);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_LC_CORRINDEXINDUSTRY;

  --发行人现金流量表_金融类
  procedure cj_MIS_BONDFCASHFLOWSTATEMENT(errcode out int,
                                          errmsg  out varchar2) is
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

    /*    --临时表写入接口表中
        MERGE INTO ODS_MIS_BONDFCASHFLOWSTATEMENT t2
    USING t_MIS_BONDFCASHFLOWSTATEMENT t
    ON (t.IndexCode=t2.IndexCode  and t.IndustryStandard=t2.IndustryStandard
    and t.IndustryCode=t2.IndustryCode )
    WHEN MATCHED THEN
    UPDATE
    SET
    t2.enddate = t.enddate,
    t2.indexstate = t.indexstate,
    t2.updatetime = t.updatetime,
    t2.jsid = t.jsid
    WHEN NOT MATCHED THEN
    INSERT
      (t2.ID,t2.INDEXCODE,t2.INDUSTRYSTANDARD,t2.INDUSTRYCODE,t2.ENDDATE,t2.INDEXSTATE,t2.UPDATETIME,t2.JSID)
    VALUES
      (t.ID,t.INDEXCODE,t.INDUSTRYSTANDARD,t.INDUSTRYCODE,t.ENDDATE,t.INDEXSTATE,t.UPDATETIME,t.JSID);*/

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_BONDFCASHFLOWSTATEMENT;

  --系统常量表
  procedure cj_MIS_CT_SYSTEMCONST(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_CT_SYSTEMCONST t2
    USING t_MIS_CT_SYSTEMCONST t
    ON (t.LB = t2.LB and t.DM = t2.DM)
    WHEN MATCHED THEN
      UPDATE
         SET t2.lbmc   = t.lbmc,
             t2.ms     = t.ms,
             t2.xgrq   = t.xgrq,
             t2.jsid   = t.jsid,
             t2.fvalue = t.fvalue,
             t2.ivalue = t.ivalue,
             t2.dvalue = t.dvalue,
             t2.cvalue = t.cvalue
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.LB,
         t2.LBMC,
         t2.MS,
         t2.DM,
         t2.XGRQ,
         t2.JSID,
         t2.FVALUE,
         t2.IVALUE,
         t2.DVALUE,
         t2.CVALUE)
      VALUES
        (t.ID,
         t.LB,
         t.LBMC,
         t.MS,
         t.DM,
         t.XGRQ,
         t.JSID,
         t.FVALUE,
         t.IVALUE,
         t.DVALUE,
         t.CVALUE);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_CT_SYSTEMCONST;

  --三板转让方式
  procedure cj_MIS_NQ_TransType(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_NQ_TransType t2
    USING t_MIS_NQ_TransType t
    ON (t.StartDate = t2.StartDate and t.InnerCode = t2.InnerCode)
    WHEN MATCHED THEN
      UPDATE
         SET t2.infopubldate = t.infopubldate,
             t2.transtype    = t.transtype,
             t2.inserttime   = t.inserttime,
             t2.updatetime   = t.updatetime,
             t2.jsid         = t.jsid
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INFOPUBLDATE,
         t2.STARTDATE,
         t2.INNERCODE,
         t2.TRANSTYPE,
         t2.INSERTTIME,
         t2.UPDATETIME,
         t2.JSID)
      VALUES
        (t.ID,
         t.INFOPUBLDATE,
         t.STARTDATE,
         t.INNERCODE,
         t.TRANSTYPE,
         t.INSERTTIME,
         t.UPDATETIME,
         t.JSID);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_NQ_TransType;

  --中债指数样本券及权重
  procedure cj_MIS_SA_BDIComponentsWeight(errcode out int,
                                          errmsg  out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_SA_BDIComponentsWeight t2
    USING t_MIS_SA_BDIComponentsWeight t
    ON (t.IndexCode = t2.IndexCode and t.EndDate = t2.EndDate and t.InnerCode = t2.InnerCode and t.SecuMarket = t2.SecuMarket)
    WHEN MATCHED THEN
      UPDATE
         SET t2.indexname         = t.indexname,
             t2.maturitydesc      = t.maturitydesc,
             t2.secucode          = t.secucode,
             t2.secuabbr          = t.secuabbr,
             t2.marketdesc        = t.marketdesc,
             t2.fullprice         = t.fullprice,
             t2.cleanprice        = t.cleanprice,
             t2.accruinterest     = t.accruinterest,
             t2.changepct         = t.changepct,
             t2.remainmaturity    = t.remainmaturity,
             t2.avgytm            = t.avgytm,
             t2.pointspread       = t.pointspread,
             t2.modifiedduration  = t.modifiedduration,
             t2.interestduration  = t.interestduration,
             t2.spreadduration    = t.spreadduration,
             t2.trueconvexity     = t.trueconvexity,
             t2.interestconvexity = t.interestconvexity,
             t2.spreadconvexity   = t.spreadconvexity,
             t2.basicpointvalue   = t.basicpointvalue,
             t2.weightedratio     = t.weightedratio,
             t2.updatetime        = t.updatetime,
             t2.jsid              = t.jsid
    WHEN NOT MATCHED THEN
      INSERT
        (t2.ID,
         t2.INDEXCODE,
         t2.INDEXNAME,
         t2.MATURITYDESC,
         t2.ENDDATE,
         t2.INNERCODE,
         t2.SECUCODE,
         t2.SECUABBR,
         t2.SECUMARKET,
         t2.MARKETDESC,
         t2.FULLPRICE,
         t2.CLEANPRICE,
         t2.ACCRUINTEREST,
         t2.CHANGEPCT,
         t2.REMAINMATURITY,
         t2.AVGYTM,
         t2.POINTSPREAD,
         t2.MODIFIEDDURATION,
         t2.INTERESTDURATION,
         t2.SPREADDURATION,
         t2.TRUECONVEXITY,
         t2.INTERESTCONVEXITY,
         t2.SPREADCONVEXITY,
         t2.BASICPOINTVALUE,
         t2.WEIGHTEDRATIO,
         t2.UPDATETIME,
         t2.JSID)
      VALUES
        (t.ID,
         t.INDEXCODE,
         t.INDEXNAME,
         t.MATURITYDESC,
         t.ENDDATE,
         t.INNERCODE,
         t.SECUCODE,
         t.SECUABBR,
         t.SECUMARKET,
         t.MARKETDESC,
         t.FULLPRICE,
         t.CLEANPRICE,
         t.ACCRUINTEREST,
         t.CHANGEPCT,
         t.REMAINMATURITY,
         t.AVGYTM,
         t.POINTSPREAD,
         t.MODIFIEDDURATION,
         t.INTERESTDURATION,
         t.SPREADDURATION,
         t.TRUECONVEXITY,
         t.INTERESTCONVEXITY,
         t.SPREADCONVEXITY,
         t.BASICPOINTVALUE,
         t.WEIGHTEDRATIO,
         t.UPDATETIME,
         t.JSID);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_SA_BDIComponentsWeight;

  --汇率信息
  procedure cj_MIS_FX_OFFICIALFX(errcode out int, errmsg out varchar2) IS
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

    MERGE INTO ods_mis_fx_officialfx t2
    USING t_mis_fx_officialfx t
    ON (t.enddate = t2.enddate and t.reportperiod = t2.reportperiod and t.domecurrency = t2.domecurrency and t.forecurrency = t2.forecurrency)
    WHEN MATCHED THEN
      UPDATE
         SET t2.id           = t.id,
             t2.INFOPUBIDATE = t.infopubidate,
             t2.enddate      = t.enddate,
             t2.infosource   = t.infosource,
             t2.reportperiod = t.reportperiod,
             t2.domecurrency = t.domecurrency,
             t2.forecurrency = t.forecurrency,
             t2.midexrate    = t.midexrate,
             t2.updatetime   = t.updatetime,
             t2.jsid         = t.jsid
    WHEN NOT MATCHED THEN
      INSERT
        (t2.id,
         t2.INFOPUBIDATE,
         t2.enddate,
         t2.infosource,
         t2.reportperiod,
         t2.domecurrency,
         t2.forecurrency,
         t2.midexrate,
         t2.updatetime,
         t2.jsid)
      VALUES
        (t.id,
         t.INFOPUBIDATE,
         t.enddate,
         t.infosource,
         t.reportperiod,
         t.domecurrency,
         t.forecurrency,
         t.midexrate,
         t.updatetime,
         t.jsid);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_MIS_FX_OFFICIALFX;

  --债券发行招标
  procedure cj_mis_bondbid(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ods_mis_bondbid t2
    USING t_mis_bondbid t
    ON (t.InnerCode = t2.InnerCode and t.IssueType = t2.IssueType and t.BidType = t2.BidType and t.BidCategory = t2.BidCategory)
    /*WHEN MATCHED THEN
                                                    UPDATE SET*/
    WHEN NOT MATCHED THEN
      INSERT
        (t2.id,
         t2.innercode,
         t2.issuedate,
         t2.issuetype,
         t2.bidtype,
         t2.bidcategory,
         t2.documentnumber,
         t2.issuesizeplanned,
         t2.paidamount,
         t2.actualissusize,
         t2.additionalsize,
         t2.generalbasicunderwrite,
         t2.basicunderwrite,
         t2.minunderwrite,
         t2.bidamount,
         t2.companynumpresent,
         t2.bidcompanynum,
         t2.bidnumber,
         t2.validbidnumber,
         t2.invalidbidnumber,
         t2.validamount,
         t2.highbidprice,
         t2.lowbidprice,
         t2.companynumwinbid,
         t2.numwinbid,
         t2.amountwinbid,
         t2.distributeamountwinbid,
         t2.selfsupportamountwinbid,
         t2.highpricewinbid,
         t2.lowpricewinbid,
         t2.marginamountbid,
         t2.marginamountwinbid,
         t2.interestratespread,
         t2.issueprice,
         t2.pricewinbid,
         t2.referrenceyield,
         t2.couponrate,
         t2.interestratewinbid,
         t2.firstcouponrate,
         t2.companynumexcessbid,
         t2.amountexcessbid,
         t2.companynumlackbid,
         t2.amountlackbid,
         t2.updatetime,
         t2.jsid)
      VALUES
        (t.id,
         t.innercode,
         t.issuedate,
         t.issuetype,
         t.bidtype,
         t.bidcategory,
         t.documentnumber,
         t.issuesizeplanned,
         t.paidamount,
         t.actualissusize,
         t.additionalsize,
         t.generalbasicunderwrite,
         t.basicunderwrite,
         t.minunderwrite,
         t.bidamount,
         t.companynumpresent,
         t.bidcompanynum,
         t.bidnumber,
         t.validbidnumber,
         t.invalidbidnumber,
         t.validamount,
         t.highbidprice,
         t.lowbidprice,
         t.companynumwinbid,
         t.numwinbid,
         t.amountwinbid,
         t.distributeamountwinbid,
         t.selfsupportamountwinbid,
         t.highpricewinbid,
         t.lowpricewinbid,
         t.marginamountbid,
         t.marginamountwinbid,
         t.interestratespread,
         t.issueprice,
         t.pricewinbid,
         t.referrenceyield,
         t.couponrate,
         t.interestratewinbid,
         t.firstcouponrate,
         t.companynumexcessbid,
         t.amountexcessbid,
         t.companynumlackbid,
         t.amountlackbid,
         t.updatetime,
         t.jsid);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_mis_bondbid;

  --债券利率变动
  procedure cj_mis_bondinterestrate(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ods_mis_bondinterestrate t2
    USING t_mis_bondinterestrate t
    ON (t.InnerCode = t2.InnerCode and t.ValueDatePer = t2.ValueDatePer)
    WHEN NOT MATCHED THEN
      INSERT
        (t2.id,
         t2.innercode,
         t2.compoundmethod,
         t2.intpaymentmethod,
         t2.payinteresteffency,
         t2.valuedate,
         t2.valuedateper,
         t2.valuedatenextper,
         t2.interestthisyear,
         t2.frnrefrateper,
         t2.frnrefratetext,
         t2.frnrefratedate,
         t2.frnrefextramargin,
         t2.effectiverate,
         t2.periodbeginning,
         t2.periodend,
         t2.segmentrate,
         t2.updatetime,
         t2.jsid)
      VALUES
        (t.id,
         t.innercode,
         t.compoundmethod,
         t.intpaymentmethod,
         t.payinteresteffency,
         t.valuedate,
         t.valuedateper,
         t.valuedatenextper,
         t.interestthisyear,
         t.frnrefrateper,
         t.frnrefratetext,
         t.frnrefratedate,
         t.frnrefextramargin,
         t.effectiverate,
         t.periodbeginning,
         t.periodend,
         t.segmentrate,
         t.updatetime,
         t.jsid);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_mis_bondinterestrate;

  --最新财务指标
  procedure cj_mis_lc_newestfinaindex(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ods_mis_lc_newestfinaindex t2
    USING t_mis_lc_newestfinaindex t
    ON (t.COMPANYCODE = t2.COMPANYCODE and t.ADJUSTDATE = t2.ADJUSTDATE)
    WHEN NOT MATCHED THEN
      INSERT
        (t2.CompanyCode,
         t2.AdjustDate,
         t2.AdjustReason,
         t2.EndDate,
         t2.ShareCapitalBeforeAdjust,
         t2.SHareCapitalIncr,
         t2.ShareCapitalAftereAdjust,
         t2.CapitalReserveBeforeAdjus,
         t2.CapitalReserveIncr,
         t2.CapitalReserveAfterAdjust,
         t2.RetainedProfitBeforeAdjust,
         t2.RetainedProfitIncr,
         t2.RetainedProfitAfterAdjust,
         t2.SurplusReserveBeforeAdjust,
         t2.SurplusReserveIncr,
         t2.SurplusReserveAfterAdjust,
         t2.NetAssetBeforeAdjust,
         t2.NetAssetIncr,
         t2.NetAssetAfterAdjust,
         t2.EPS,
         t2.EPSTTM,
         t2.NAPS,
         t2.RetainedProfitPS,
         t2.CapitalReservePS,
         t2.NetOperateCashFlowPS,
         t2.XGRQ)
      VALUES
        (t.CompanyCode,
         t.AdjustDate,
         t.AdjustReason,
         t.EndDate,
         t.ShareCapitalBeforeAdjust,
         t.SHareCapitalIncr,
         t.ShareCapitalAftereAdjust,
         t.CapitalReserveBeforeAdjus,
         t.CapitalReserveIncr,
         t.CapitalReserveAfterAdjust,
         t.RetainedProfitBeforeAdjust,
         t.RetainedProfitIncr,
         t.RetainedProfitAfterAdjust,
         t.SurplusReserveBeforeAdjust,
         t.SurplusReserveIncr,
         t.SurplusReserveAfterAdjust,
         t.NetAssetBeforeAdjust,
         t.NetAssetIncr,
         t.NetAssetAfterAdjust,
         t.EPS,
         t.EPSTTM,
         t.NAPS,
         t.RetainedProfitPS,
         t.CapitalReservePS,
         t.NetOperateCashFlowPS,
         t.XGRQ);
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_mis_lc_newestfinaindex;

  --期货合约
  procedure cj_mis_fut_contractmain(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_FUT_CONTRACTMAIN t2
    USING T_MIS_FUT_CONTRACTMAIN t
    ON (t.contractinnercode = t2.contractinnercode)
    WHEN NOT MATCHED THEN
      INSERT
        (t2.contractinnercode,
         t2.contractcode,
         t2.contractabbr,
         t2.contractname,
         t2.exchangecode,
         t2.contracttype,
         t2.optioncode,
         t2.ifreal,
         t2.cmvalue,
         t2.contractmultiplier,
         t2.priceunit,
         t2.littlestchangeunit,
         t2.changepctlimit,
         t2.currencycode,
         t2.deliveryyear,
         t2.deliverymonth,
         t2.effectivedate,
         t2.lasttradingdate,
         t2.deliverydate,
         t2.lastdeliverydate,
         t2.deliverymethod,
         t2.deliverygrades,
         t2.minmarginratio,
         t2.tradingcommission,
         t2.deliverycommission,
         t2.settpricecode,
         t2.settpricedesc,
         t2.delisettpricecode,
         t2.delisettpricedesc,
         t2.listbasisprice,
         t2.changepctlistprice,
         t2.contractstate,
         t2.changereason,
         t2.contractmonth,
         t2.exchangedate,
         t2.lasttradingdatedesc,
         t2.lasttradingtimedesc,
         t2.deliverydatedesc,
         t2.lastdeliverydatedesc,
         t2.contractintroduction,
         t2.updatetime,
         t2.jsid)
      VALUES
        (t.contractinnercode,
         t.contractcode,
         t.contractabbr,
         t.contractname,
         t.exchangecode,
         t.contracttype,
         t.optioncode,
         t.ifreal,
         t.cmvalue,
         t.contractmultiplier,
         t.priceunit,
         t.littlestchangeunit,
         t.changepctlimit,
         t.currencycode,
         t.deliveryyear,
         t.deliverymonth,
         t.effectivedate,
         t.lasttradingdate,
         t.deliverydate,
         t.lastdeliverydate,
         t.deliverymethod,
         t.deliverygrades,
         t.minmarginratio,
         t.tradingcommission,
         t.deliverycommission,
         t.settpricecode,
         t.settpricedesc,
         t.delisettpricecode,
         t.delisettpricedesc,
         t.listbasisprice,
         t.changepctlistprice,
         t.contractstate,
         t.changereason,
         t.contractmonth,
         t.exchangedate,
         t.lasttradingdatedesc,
         t.lasttradingtimedesc,
         t.deliverydatedesc,
         t.lastdeliverydatedesc,
         t.contractintroduction,
         t.updatetime,
         t.jsid);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_mis_fut_contractmain;

  --期货品种
  procedure cj_mis_fut_futurescontract(errcode out int,
                                       errmsg  out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ODS_MIS_FUT_FUTURESCONTRACT t2
    USING T_MIS_FUT_FUTURESCONTRACT t
    ON (t.contractname = t2.contractname and t.contractoption = t2.contractoption and t.exchange = t2.exchange)
    WHEN NOT MATCHED THEN
      INSERT
        (t2.contractinnercode,
         t2.contractname,
         t2.contractoption,
         t2.exchange,
         t2.tradingcode,
         t2.contracttype,
         t2.contractmulti,
         t2.priceunit,
         t2.littlestchangeunit,
         t2.changepctlim,
         t2.contractmonth,
         t2.callauctiontime,
         t2.exchangedate,
         t2.nighttradingtime,
         t2.lasttradingdate,
         t2.lasttradingtime,
         t2.deliverydate,
         t2.lastdeliverydate,
         t2.deliverablegrades,
         t2.maintainratio,
         t2.fee,
         t2.settlementprice,
         t2.finalsettlementprice,
         t2.deliverymethod,
         t2.deliveryunit,
         t2.deliveryunitvalue,
         t2.contractintroduction,
         t2.contractstate,
         t2.updatetime,
         t2.jsid)
      VALUES
        (t.contractinnercode,
         t.contractname,
         t.contractoption,
         t.exchange,
         t.tradingcode,
         t.contracttype,
         t.contractmulti,
         t.priceunit,
         t.littlestchangeunit,
         t.changepctlim,
         t.contractmonth,
         t.callauctiontime,
         t.exchangedate,
         t.nighttradingtime,
         t.lasttradingdate,
         t.lasttradingtime,
         t.deliverydate,
         t.lastdeliverydate,
         t.deliverablegrades,
         t.maintainratio,
         t.fee,
         t.settlementprice,
         t.finalsettlementprice,
         t.deliverymethod,
         t.deliveryunit,
         t.deliveryunitvalue,
         t.contractintroduction,
         t.contractstate,
         t.updatetime,
         t.jsid);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_mis_fut_futurescontract;

  --期货品种
  procedure cj_mis_yhbs_jgflxx(errcode out int, errmsg out varchar2) is
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

    --临时表写入接口表中
    MERGE INTO ods_mis_yhbs_jgflxx t2
    USING t_mis_yhbs_jgflxx t
    ON (t.id = t2.id)
    WHEN NOT MATCHED THEN
      INSERT
        (t2.id,
         t2.companycode,
         t2.reportcompcode,
         t2.reportcompname,
         t2.enddate,
         t2.originalcurrency,
         t2.convertedcurrency,
         t2.institype1,
         t2.institype2,
         t2.companycval,
         t2.instiarea,
         t2.enterprisescale,
         t2.industrytype1,
         t2.industrytype2,
         t2.industrytype3,
         t2.industrytype4,
         t2.operatingrvenue,
         t2.totalasset,
         t2.iflisted,
         t2.inserttime,
         t2.updatetime,
         t2.jsid)
      VALUES
        (t.id,
         t.companycode,
         t.reportcompcode,
         t.reportcompname,
         t.enddate,
         t.originalcurrency,
         t.convertedcurrency,
         t.institype1,
         t.institype2,
         t.companycval,
         t.instiarea,
         t.enterprisescale,
         t.industrytype1,
         t.industrytype2,
         t.industrytype3,
         t.industrytype4,
         t.operatingrvenue,
         t.totalasset,
         t.iflisted,
         t.inserttime,
         t.updatetime,
         t.jsid);

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_mis_yhbs_jgflxx;

   --股本变动信息
  procedure cj_mis_lc_newestsharestru(errcode out int, errmsg out varchar2)is
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

    --临时表写入接口表中
    MERGE INTO ods_mis_lc_newestsharestru t1
    USING t_mis_lc_newestsharestru t2
    ON (t1.COMPANYCODE = t2.COMPANYCODE and t1.ENDDATE = t2.ENDDATE)
    when matched then
       update
       set
         t1.totalshares = t2.totalshares
    WHEN NOT MATCHED THEN
      INSERT
        (t1.ID	,
        t1.CompanyCode	,
        t1.InfoSource	,
        t1.EndDate	,
        t1.InfoPublDate	,
        t1.PerValue	,
        t1.NonListedShares	,
        t1.PromoterShares	,
        t1.StateShares	,
        t1.SLegalPersonShares	,
        t1.DLegalPersonShares	,
        t1.FLegalPersonShares	,
        t1.OtherPromoterShares	,
        t1.RaisedLPShares	,
        t1.RaisedSLPShares	,
        t1.NaturalPersonHoldLPShares	,
        t1.StaffShares	,
        t1.RightsIssueTransferred	,
        t1.PreferredAndOtherShares	,
        t1.PreferredShares	,
        t1.FloatShare	,
        t1.AFloats	,
        t1.AFloatListed	,
        t1.ManagementShares	,
        t1.StategicInvestorShares	,
        t1.CommonLPShares	,
        t1.MutualFundShares	,
        t1.AdditionalIssueUnlisted	,
        t1.RightsIssueUnlisted	,
        t1.OtherAFloatShares	,
        t1.RestrictedAFloatShares	,
        t1.RestrinctStaffShares	,
        t1.Bshares	,
        t1.NonListedBShares	,
        t1.RestrictedBFloatShares	,
        t1.Hshares	,
        t1.OtherFloatShares	,
        t1.Sshares	,
        t1.Nshares	,
        t1.Dshares	,
        t1.GDRshares	,
        t1.RestrictedShares	,
        t1.StateHolding	,
        t1.SLegalPersonHolding	,
        t1.OtherDCapitalHolding	,
        t1.DLegalPersonHolding	,
        t1.DNaturalPersonHolding	,
        t1.ForeignHolding	,
        t1.FLegalPersonHolding	,
        t1.FNaturalPersonHolding	,
        t1.OtherRestrictedShares	,
        t1.Rpt_RestrictedShares	,
        t1.Rpt_StateHolding	,
        t1.Rpt_SLegalPersonHolding	,
        t1.Rpt_OtherDCapitalHolding	,
        t1.Rpt_DLegalPersonHolding	,
        t1.Rpt_DNaturalPersonHolding	,
        t1.Rpt_ForeignHolding	,
        t1.Rpt_FLegalPersonHolding	,
        t1.Rpt_FNaturalPersonHolding	,
        t1.Rpt_FloatListed	,
        t1.Rpt_AFloatListed	,
        t1.Rpt_BFloatListed	,
        t1.Rpt_FFloatListed	,
        t1.Rpt_OtherFloatShares	,
        t1.Rpt_TotalShares	,
        t1.Ashares	,
        t1.NonRestrictedShares	,
        t1.BsharesTotal	,
        t1.ListedBShares	,
        t1.NonListedRestrictedBShares	,
        t1.ForeignHoldingAshares	,
        t1.RestrictedAShares	,
        t1.Rpt_ManagementShares	,
        t1.TotalShares	,
        t1.ChangeType	,
        t1.ChangeReason	,
        t1.XGRQ	,
        t1.JSID)
      VALUES
        (t2.ID	,
          t2.CompanyCode	,
          t2.InfoSource	,
          t2.EndDate	,
          t2.InfoPublDate	,
          t2.PerValue	,
          t2.NonListedShares	,
          t2.PromoterShares	,
          t2.StateShares	,
          t2.SLegalPersonShares	,
          t2.DLegalPersonShares	,
          t2.FLegalPersonShares	,
          t2.OtherPromoterShares	,
          t2.RaisedLPShares	,
          t2.RaisedSLPShares	,
          t2.NaturalPersonHoldLPShares	,
          t2.StaffShares	,
          t2.RightsIssueTransferred	,
          t2.PreferredAndOtherShares	,
          t2.PreferredShares	,
          t2.FloatShare	,
          t2.AFloats	,
          t2.AFloatListed	,
          t2.ManagementShares	,
          t2.StategicInvestorShares	,
          t2.CommonLPShares	,
          t2.MutualFundShares	,
          t2.AdditionalIssueUnlisted	,
          t2.RightsIssueUnlisted	,
          t2.OtherAFloatShares	,
          t2.RestrictedAFloatShares	,
          t2.RestrinctStaffShares	,
          t2.Bshares	,
          t2.NonListedBShares	,
          t2.RestrictedBFloatShares	,
          t2.Hshares	,
          t2.OtherFloatShares	,
          t2.Sshares	,
          t2.Nshares	,
          t2.Dshares	,
          t2.GDRshares	,
          t2.RestrictedShares	,
          t2.StateHolding	,
          t2.SLegalPersonHolding	,
          t2.OtherDCapitalHolding	,
          t2.DLegalPersonHolding	,
          t2.DNaturalPersonHolding	,
          t2.ForeignHolding	,
          t2.FLegalPersonHolding	,
          t2.FNaturalPersonHolding	,
          t2.OtherRestrictedShares	,
          t2.Rpt_RestrictedShares	,
          t2.Rpt_StateHolding	,
          t2.Rpt_SLegalPersonHolding	,
          t2.Rpt_OtherDCapitalHolding	,
          t2.Rpt_DLegalPersonHolding	,
          t2.Rpt_DNaturalPersonHolding	,
          t2.Rpt_ForeignHolding	,
          t2.Rpt_FLegalPersonHolding	,
          t2.Rpt_FNaturalPersonHolding	,
          t2.Rpt_FloatListed	,
          t2.Rpt_AFloatListed	,
          t2.Rpt_BFloatListed	,
          t2.Rpt_FFloatListed	,
          t2.Rpt_OtherFloatShares	,
          t2.Rpt_TotalShares	,
          t2.Ashares	,
          t2.NonRestrictedShares	,
          t2.BsharesTotal	,
          t2.ListedBShares	,
          t2.NonListedRestrictedBShares	,
          t2.ForeignHoldingAshares	,
          t2.RestrictedAShares	,
          t2.Rpt_ManagementShares	,
          t2.TotalShares	,
          t2.ChangeType	,
          t2.ChangeReason	,
          t2.XGRQ	,
          t2.JSID
          );

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;
    errcode := 1;
    errmsg  := 'OK';
      --程序处理完成
	V_LOG_POINT := '999';
	V_MSG       := '程序处理完成';
	DATA_COLLECTING_LOG(V_PROC_NAME,
	                    V_LOG_POINT,
	                    V_MSG,
 	                    V_LOG_FLAG,
 	                    V_LOADROW);
    EXCEPTION
        WHEN OTHERS THEN
      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE
      V_MSG      := '发生错误' || SQLERRM;
      V_LOG_FLAG := 'E';
      DATA_COLLECTING_LOG(V_PROC_NAME,
                          V_LOG_POINT,
                          V_MSG,
                          V_LOG_FLAG,
                          V_LOADROW);
      errcode := 0;
      errmsg  := SQLCODE || ' ' || SQLERRM;
  end cj_mis_lc_newestsharestru;


end pkg_etl_data_mis;
/
