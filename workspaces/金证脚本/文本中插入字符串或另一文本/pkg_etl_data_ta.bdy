create or replace package body pkg_etl_data_ta is

  --采集基金信息
  procedure cj_ods_ta_fund(errcode out int, errmsg out varchar2) is
    vn_count integer;
    vn_fundtype varchar2(4);
    vn_fundfinetype varchar2(4);
    vn_distributorcode varchar2(3);
    vn_fundclass varchar2(2);
    vn_userid  varchar2(128);
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

    select nvl(t1.paramvalue,t.defaultvalue) into vn_distributorcode from  sys_param_define t
      left join sys_param_value t1 on t.paramid=t1.paramid
     where t.paramid='76101';

    select nvl(t1.paramvalue,t.defaultvalue) into vn_userid from  sys_param_define t
      left join sys_param_value t1 on t.paramid=t1.paramid
     where t.paramid='76104';

    for p in (select * from t_ta_cfg_fund) loop
      select count(*) into vn_count  from rdt_fund_value t
       where t.pcode = p.fundcode;
      if p.datasource = '0' then --自建TA
        vn_fundtype := pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'16',p.fundtype);
        vn_fundfinetype := pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'20',p.fundtype);
      elsif p.datasource = '1' then --LOFTA
        vn_fundtype := pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'12',p.fundtype);
        vn_fundfinetype := pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'13',p.fundtype);
      elsif p.datasource = '2' then --ETFTA
        vn_fundtype := pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'16',p.fundtype);
        vn_fundfinetype := 'Z5';
      end if;
      vn_fundclass := pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'21',p.feetixtype);

      if vn_count > 0 then
        update rdt_fund_value t set t.fvalue = p.tano where t.pcode = p.fundcode and t.fname = 'tano' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.fundcode where t.pcode = p.fundcode and t.fname = 'fund_code' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.fundname where t.pcode = p.fundcode and t.fname = 'fund_abbr' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.fundengname where t.pcode = p.fundcode and t.fname = 'fund_name' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = vn_fundtype where t.pcode = p.fundcode and t.fname = 'fund_type' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = vn_fundfinetype where t.pcode = p.fundcode and t.fname = 'fund_finetype' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = vn_fundclass where t.pcode = p.fundcode and t.fname = 'fund_classify' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.approvaldate where t.pcode = p.fundcode and t.fname = 'approvaldate' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.ipostartdate where t.pcode = p.fundcode and t.fname = 'ipobegindate' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.ipoenddate where t.pcode = p.fundcode and t.fname = 'ipoenddate' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.establishdate where t.pcode = p.fundcode and t.fname = 'effectdate' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = decode(p.terminationdate,0,'',p.terminationdate) where t.pcode = p.fundcode and t.fname = 'enddate' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = to_char(p.foundscale) where t.pcode = p.fundcode and t.fname = 'foundscale' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = to_char(nvl(p.facevalue,'1')) where t.pcode = p.fundcode and t.fname = 'beginunitnetvalue' and t.mflag = '0';  --发行面值 未取到默认为1
        update rdt_fund_value t set t.fvalue = p.fundstatus where t.pcode = p.fundcode and t.fname = 'fund_status' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.managercode where t.pcode = p.fundcode and t.fname = 'manager' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = to_char(p.lowsubscribesum) where t.pcode = p.fundcode and t.fname = 'lowsubscribesum' and t.mflag = '0';

        update rdt_fund_value t set t.fvalue = to_char(p.allotminsize) where t.pcode = p.fundcode and t.fname = 'lowpurchasingsum' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.cfundt0flag where t.pcode = p.fundcode and t.fname = 'issupport_to_cash' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.profittype where t.pcode = p.fundcode and t.fname = 'netvalueflag' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = decode(vn_fundclass,'0','1','2') where t.pcode = p.fundcode and t.fname = 'raisetype' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.currencytype where t.pcode = p.fundcode and t.fname = 'raisecurrency' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = to_char(nvl(p.confirmedday,'1')) where t.pcode = p.fundcode and t.fname = 'confirmedday' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = to_char(p.cycletype) where t.pcode = p.fundcode and t.fname = 'cycletype' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.dividend where t.pcode = p.fundcode and t.fname = 'dividend' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.achievementpayflag where t.pcode = p.fundcode and t.fname = 'achievementpayflag' and t.mflag = '0';
        update rdt_fund_value t set t.fvalue = p.custodiancode where t.pcode = p.fundcode and t.fname = 'trustee' and t.mflag = '0';

        update rdt_fund_value t set t.fvalue = '0' where t.pcode = p.fundcode and t.fname = 'isenhancement' and t.mflag = '0';         --产品资产是否存在增信措施 默认取0-否
        update rdt_fund_value t set t.fvalue = '1' where t.pcode = p.fundcode and t.fname = 'valfrequency' and t.mflag = '0';          --估值频率 默认取1-日
        update rdt_fund_value t set t.fvalue = '0' where t.pcode = p.fundcode and t.fname = 'ishedgingstrategy' and t.mflag = '0';     --是否采用对冲策略 默认取0-否
        update rdt_fund_value t set t.fvalue = '0' where t.pcode = p.fundcode and t.fname = 'isquantizationstrategy' and t.mflag = '0';  --是否采用量化策略 默认取0-否
        update rdt_fund_value t set t.fvalue = '0' where t.pcode = p.fundcode and t.fname = 'isthirdtrustee' and t.mflag = '0';        --是否第三方托管 默认取0-否
        update rdt_fund_value t set t.fvalue = '0' where t.pcode = p.fundcode and t.fname = 'ishkfund' and t.mflag = '0';              --是否内地互认基金 默认取0-否
        update rdt_fund_value t set t.fvalue = '0' where t.pcode = p.fundcode and t.fname = 'ischannelbusiness' and t.mflag = '0';     --是否通道内业务 默认取0-否
      else
        insert into rdt_fund_value
          (pcode, fname, fvalue)
          (select p.fundcode, 'tano', p.tano from dual
           union select p.fundcode, 'fund_code', p.fundcode         from dual
           union select p.fundcode, 'fund_account',' '        from dual
           union select p.fundcode, 'fund_abbr', p.fundname         from dual
           union select p.fundcode, 'fund_name', p.fundengname      from dual
           union select p.fundcode, 'fund_type', vn_fundtype        from dual
           union select p.fundcode, 'fund_finetype', vn_fundfinetype    from dual
           union select p.fundcode, 'fund_classify', vn_fundclass   from dual
           union select p.fundcode, 'approvaldate', p.approvaldate   from dual
           union select p.fundcode, 'ipobegindate', p.ipostartdate  from dual
           union select p.fundcode, 'ipoenddate', p.ipoenddate      from dual
           union select p.fundcode, 'effectdate', p.establishdate   from dual
           union select p.fundcode, 'enddate', decode(p.terminationdate,0,'',p.terminationdate) from dual
           union select p.fundcode, 'foundscale', to_char(p.foundscale) from dual
           union select p.fundcode, 'beginunitnetvalue', to_char(nvl(p.facevalue,'1')) from dual  --发行面值，未取到默认为1
           union select p.fundcode, 'fund_status', p.fundstatus     from dual
           union select p.fundcode, 'manager', p.managercode        from dual
           union select p.fundcode, 'lowsubscribesum', to_char(p.lowsubscribesum) from dual
           union select p.fundcode, 'lowpurchasingsum', to_char(p.allotminsize) from dual
           union select p.fundcode, 'issupport_to_cash', p.cfundt0flag   from dual
           union select p.fundcode, 'netvalueflag', p.profittype  from dual
           union select p.fundcode, 'raisetype', decode(vn_fundclass,'0','1','2') from dual
           union select p.fundcode, 'raisecurrency', p.currencytype from dual
           union select p.fundcode, 'principalcurrency', p.currencytype from dual
           union select p.fundcode, 'profitcurrency', p.currencytype from dual
           union select p.fundcode, 'confirmedday', to_char(nvl(p.confirmedday,'1'))    from dual --申请确认天数 未取到值默认为1
           union select p.fundcode, 'cycletype', to_char(p.cycletype)  from dual
           union select p.fundcode, 'dividend', p.dividend  from dual
           union select p.fundcode, 'achievementpayflag', p.achievementpayflag from dual
           union select p.fundcode, 'trustee', p.custodiancode   from dual
           union select p.fundcode, 'bailee_obligation', '0'   from dual
           union select p.fundcode, 'op_method', '2'   from dual
           union select p.fundcode, 'fund_structure', '1'   from dual
           union select p.fundcode, 'managetype', '1'   from dual
           union select p.fundcode, 'isclass', decode(p.feetixtype,'2','1','3','1','8','1','0') from dual
           union select p.fundcode, 'isbystages','0' from dual
           union select p.fundcode, 'organizationform','A' from dual
           union select p.fundcode, 'attributes',decode(p.datasource,'1','3','2','4','0') from dual
           union select p.fundcode, 'istransfer','0' from dual
           union select p.fundcode, 'iskeepprincipal','0' from dual
           union select p.fundcode, 'status', '4'   from dual
           union select p.fundcode, 'isenhancement' ,'0' from dual     --产品资产是否存在增信措施 默认取0-否
           union select p.fundcode, 'valfrequency', '1' from dual      --估值频率 默认取1-日
           union select p.fundcode, 'ishedgingstrategy', '0' from dual --是否采用对冲策略 默认取0-否
           union select p.fundcode, 'isquantizationstrategy', '0' from dual --是否采用量化策略 默认取0-否
           union select p.fundcode, 'isthirdtrustee', '0' from dual    --是否第三方托管 默认取0-否
           union select p.fundcode, 'ishkfund', '0' from dual          --是否内地互认基金 默认取0-否
           union select p.fundcode, 'ischannelbusiness', '0' from dual --是否通道内业务 默认取0-否
           );

        --初始化产品权限
        if vn_userid is not null then
          for k in (SELECT REGEXP_SUBSTR(vn_userid, '[^;]+', 1, rownum) as userid from dual
             connect by rownum <=LENGTH(vn_userid) - LENGTH(regexp_replace(vn_userid, ';', '')) + 1) loop
            select count(*) into vn_count from rdt_fundright t
              where t.userid = k.userid and t.fundcode = p.fundcode;
            if vn_count = 0 then
              insert into rdt_fundright(fundcode,userid,righttype)
               values(p.fundcode,k.userid,'1');
            end if;
          end loop;
        end if;
      end if;

        --鹏扬公募的货币基金按净值型处理
        if vn_distributorcode = '483' and vn_fundclass = '0' then
          update rdt_fund_value t set t.fvalue = '1' where t.pcode = p.fundcode and t.fname = 'netvalueflag' and t.mflag = '0';
        end if;
      if instr('2,3,8',p.feetixtype)>0 and (p.fundstatus = '1' or p.fundstatus is null) then
        select count(*) into vn_count from ods_fa_fundgrade t
         where t.fundcode = p.fundcode
           and (t.pfundcode = p.pfundcode or p.pfundcode is null);
        if vn_count = 0 then
          insert into ods_fa_fundgrade
            (fundcode,distributorname,settunit, funddate, auditorflag, fjxqsrq, pfundcode)
          values
            (p.fundcode,p.fundname,' ', '0', '0', ' ', p.pfundcode);
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
  end cj_ods_ta_fund;

  --采集基金净值表
  procedure cj_ods_ta_fundnav(errcode out int, errmsg out varchar2) is
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

    merge into ods_ta_cfg_fundnav ods
    using (select * from t_ta_cfg_fundnav) t
    on (ods.tano = t.tano and ods.fundcode = t.fundcode and ods.navdate = t.navdate)
    when matched then
      update
         set ods.datasource        = t.datasource,
             --ods.TANO              = t.TANO,
             --ods.FUNDCODE          = t.FUNDCODE,
             --ods.NAVDATE           = t.NAVDATE,
             ods.LASTFUNDSTATUS    = t.LASTFUNDSTATUS,
             ods.LASTNAV           = t.LASTNAV,
             ods.STATUS            = t.STATUS,
             ods.CLEARFLAG         = t.CLEARFLAG,
             ods.TODAYSHARE        = t.TODAYSHARE,
             ods.TODAYFUNDASSET    = t.TODAYFUNDASSET,
             ods.NAV               = t.NAV,
             ods.TOTALNAV          = t.TOTALNAV,
             ods.FUNDINCOME        = t.FUNDINCOME,
             ods.FUNDINCOMEUNIT    = t.FUNDINCOMEUNIT,
             ods.GROWTHRATE        = t.GROWTHRATE,
             ods.REALFUNDASSET     = t.REALFUNDASSET,
             ods.FOFFOREIGNCAPRATE = t.FOFFOREIGNCAPRATE,
             ods.SERVICEFEE        = t.SERVICEFEE,
             ods.NAVGROWTHRATE     = t.NAVGROWTHRATE,
             ods.MANAGEFEE         = t.MANAGEFEE,
             ods.SEASONGROWTHRATE  = t.SEASONGROWTHRATE
    when not matched then
      insert
        (datasource,
         TANO,
         FUNDCODE,
         NAVDATE,
         LASTFUNDSTATUS,
         LASTNAV,
         STATUS,
         CLEARFLAG,
         TODAYSHARE,
         TODAYFUNDASSET,
         NAV,
         TOTALNAV,
         FUNDINCOME,
         FUNDINCOMEUNIT,
         GROWTHRATE,
         REALFUNDASSET,
         FOFFOREIGNCAPRATE,
         SERVICEFEE,
         NAVGROWTHRATE,
         MANAGEFEE,
         SEASONGROWTHRATE)
      values
        (t.datasource,
         t.TANO,
         t.FUNDCODE,
         t.NAVDATE,
         t.LASTFUNDSTATUS,
         t.LASTNAV,
         t.STATUS,
         t.CLEARFLAG,
         t.TODAYSHARE,
         t.TODAYFUNDASSET,
         t.NAV,
         t.TOTALNAV,
         t.FUNDINCOME,
         t.FUNDINCOMEUNIT,
         t.GROWTHRATE,
         t.REALFUNDASSET,
         t.FOFFOREIGNCAPRATE,
         t.SERVICEFEE,
         t.NAVGROWTHRATE,
         t.MANAGEFEE,
         t.SEASONGROWTHRATE);

  	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

  for p in (select * from t_ta_cfg_fundnav v
     where v.lastfundstatus <> v.status and v.status = 'a') loop
      update rdt_fund_value t set t.fvalue = p.navdate
       where t.pcode = p.fundcode and t.fname = 'enddate';
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
  end cj_ods_ta_fundnav;

  --采集销售商信息
  procedure cj_ods_ta_distributor(errcode out int, errmsg out varchar2) is

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

    merge into ods_ta_cfg_distributor ods
    using (select * from t_ta_cfg_distributor) t
    on (ods.DISTRIBUTORCODE = t.DISTRIBUTORCODE)
    when matched then
      update
         set --ods.DISTRIBUTORCODE = t.DISTRIBUTORCODE,
             ods.DISTRIBUTORTYPE = t.DISTRIBUTORTYPE,
             ods.DISTRIBUTORNAME = t.DISTRIBUTORNAME
          --   ods.FULLNAME        = t.FULLNAME
    when not matched then
      insert
        (DISTRIBUTORCODE, DISTRIBUTORTYPE, DISTRIBUTORNAME, FULLNAME)
      values
        (t.DISTRIBUTORCODE,
         t.DISTRIBUTORTYPE,
         t.DISTRIBUTORNAME,
         t.FULLNAME);
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
  end cj_ods_ta_distributor;

  --采集份额折算表
  procedure cj_ods_ta_fund_navdis(errcode out int, errmsg out varchar2) is

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

    merge into ods_ta_cfg_fund_navdis ods
    using (select * from t_ta_cfg_fund_navdis) t
    on (ods.datasource = t.datasource and ods.FUNDCODE = t.FUNDCODE and ods.CONVERTDATE = t.CONVERTDATE)
    when matched then
      update
         set --ods.datasource       = t.datasource,
             --ods.FUNDCODE         = t.FUNDCODE,
             --ods.CONVERTDATE      = t.CONVERTDATE,
             ods.RANGE            = t.RANGE,
             ods.CONVERTMODE      = t.CONVERTMODE,
             ods.RATE             = t.RATE,
             ods.AFTERCONVERTNAV  = t.AFTERCONVERTNAV,
             ods.CONVERTDECTYPE   = t.CONVERTDECTYPE,
             ods.CONVERTMINUNIT   = t.CONVERTMINUNIT,
             ods.CONVERTTYPE      = t.CONVERTTYPE,
             ods.CONVERTFUNDASSET = t.CONVERTFUNDASSET,
             ods.REGISTTYPE       = t.REGISTTYPE,
             ods.COSTPRICETYPE    = t.COSTPRICETYPE,
             ods.STATUS           = t.STATUS,
             ods.REDEEMTYPE       = t.REDEEMTYPE
    when not matched then
      insert
        (datasource,
         FUNDCODE,
         CONVERTDATE,
         RANGE,
         CONVERTMODE,
         RATE,
         AFTERCONVERTNAV,
         CONVERTDECTYPE,
         CONVERTMINUNIT,
         CONVERTTYPE,
         CONVERTFUNDASSET,
         REGISTTYPE,
         COSTPRICETYPE,
         STATUS,
         REDEEMTYPE)
      values
        (t.datasource,
         t.FUNDCODE,
         t.CONVERTDATE,
         t.RANGE,
         t.CONVERTMODE,
         t.RATE,
         t.AFTERCONVERTNAV,
         t.CONVERTDECTYPE,
         t.CONVERTMINUNIT,
         t.CONVERTTYPE,
         t.CONVERTFUNDASSET,
         t.REGISTTYPE,
         t.COSTPRICETYPE,
         t.STATUS,
         t.REDEEMTYPE);
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
  end cj_ods_ta_fund_navdis;

  --采集账号确认表
  procedure cj_ods_ta_ack_acct(errcode out int, errmsg out varchar2) is
    vn_distributorcode  varchar2(1024);
    vc_orgid            varchar2(32);
    vc_orgidflag        varchar2(8);
    vn_userid           integer;
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

    --获取最父级部门
    select t.orgid into vc_orgid from sys_org t where t.parentorgid is null;

    --获取系统管理员id
    select t.userid into vn_userid from kd_userid t where t.userui = 1;

    --多机构版本标识
    select nvl(t1.paramvalue,t.defaultvalue) into vc_orgidflag from  sys_param_define t
    left join sys_param_value t1 on t.paramid=t1.paramid
    where t.paramid='76002';

    --默认销售商
    select nvl(t1.paramvalue,t.defaultvalue) into vn_distributorcode from  sys_param_define t
     left join sys_param_value t1 on t.paramid=t1.paramid
     where t.paramid='76101';

    --更新临时表数据
    update t_ta_ack_acct t
    set t.CERTIFICATETYPE = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'2',trim(t.CERTIFICATETYPE)),
        t.VOCATIONCODE = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'0',trim(t.VOCATIONCODE))
    where t.INVESTTYPE = '1';

    update t_ta_ack_acct t
    set t.CERTIFICATETYPE = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'3',trim(t.CERTIFICATETYPE)),
        t.INDUSTRYCODE = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'1',trim(t.INDUSTRYCODE))
    where t.INVESTTYPE = '0';

    update t_ta_ack_acct t
    set t.INSTREPRIDTYPE = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'2',trim(t.INSTREPRIDTYPE))
    where t.INVESTTYPE = '1';

    --客户国籍
    update t_ta_ack_acct t
    set t.NATIONALITY = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'10',trim(t.NATIONALITY));

    --经办人国籍
    update t_ta_ack_acct t
    set t.INSTREPRNATIONALITY = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'10',trim(t.INSTREPRNATIONALITY));

    --业务类型
    update t_ta_ack_acct t
    set t.businesscode = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'17',trim(t.businesscode));

    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    merge into ods_ta_ack_acct ods
    using (select * from t_ta_ack_acct) t
    on (ods.DATASOURCE = t.DATASOURCE and ods.TASERIALNO = t.TASERIALNO and ods.TRANSACTIONCFMDATE = t.TRANSACTIONCFMDATE)
    when matched then
      update
         set --ods.datasource           = t.datasource,
             ods.TANO                 = t.TANO,
             ods.APPSHEETSERIALNO     = t.APPSHEETSERIALNO,
            -- ods.TASERIALNO           = t.TASERIALNO,
             ods.BUSINESSCODE         = t.BUSINESSCODE,
            -- ods.TRANSACTIONCFMDATE   = t.TRANSACTIONCFMDATE,
             ods.TAACCOUNTID          = t.TAACCOUNTID,
             ods.DISTRIBUTORCODE      = t.DISTRIBUTORCODE,
             ods.TRANSACTIONACCOUNTID = t.TRANSACTIONACCOUNTID,
             ods.BRANCHCODE           = t.BRANCHCODE,
             ods.REGIONCODE           = t.REGIONCODE,
             ods.ACCEPTMETHOD         = t.ACCEPTMETHOD,
             ods.TRANSACTIONDATE      = t.TRANSACTIONDATE,
             ods.TRANSACTIONTIME      = t.TRANSACTIONTIME,
             ods.FROMTAFLAG           = t.FROMTAFLAG,
             ods.CUSTNO               = t.CUSTNO,
             ods.INVESTNAME           = t.INVESTNAME,
             ods.INVESTTYPE           = t.INVESTTYPE,
             ods.CERTIFICATETYPE      = t.CERTIFICATETYPE,
             ods.CERTIFICATENO        = t.CERTIFICATENO,
             ods.CERTIFICATEVALIDDATE = t.CERTIFICATEVALIDDATE,
             ods.NATIONALITY          = t.NATIONALITY,
             ods.SEX                  = t.SEX,
             ods.EDUCATIONLEVEL       = t.EDUCATIONLEVEL,
             ods.VOCATIONCODE         = t.VOCATIONCODE,
             ods.INDUSTRYCODE         = t.INDUSTRYCODE,
             ods.ANNUALINCOME         = t.ANNUALINCOME,
             ods.POSTCODE             = t.POSTCODE,
             ods.ADDRESS              = t.ADDRESS,
             ods.EMAIL                = t.EMAIL,
             ods.MOBILETELNO          = t.MOBILETELNO,
             ods.MOBILETELNO2         = t.MOBILETELNO2,
             ods.HOMETELNO            = t.HOMETELNO,
             ods.OFFICETELNO          = t.OFFICETELNO,
             ods.FAXNO                = t.FAXNO,
             ods.PAGERNO              = t.PAGERNO,
             ods.BENEFITACCTNAME      = t.BENEFITACCTNAME,
             ods.BENEFITACCTNO        = t.BENEFITACCTNO,
             ods.BENEFITBANKNAME      = t.BENEFITBANKNAME,
             ods.BENEFITBANKCODE      = t.BENEFITBANKCODE,
             ods.REGISTEREDOFFICE     = t.REGISTEREDOFFICE,
             ods.BUSINESSSCOPE        = t.BUSINESSSCOPE,
             ods.TAXCERTIFICATENO     = t.TAXCERTIFICATENO,
             ods.INSTREPRNAME         = t.INSTREPRNAME,
             ods.INSTREPRNATIONALITY  = t.INSTREPRNATIONALITY,
             ods.INSTREPRIDTYPE       = t.INSTREPRIDTYPE,
             ods.INSTREPRIDCODE       = t.INSTREPRIDCODE,
             ods.RETURNCODE           = t.RETURNCODE,
             ods.RETURNMSG            = t.RETURNMSG
    when not matched then
      insert
        (datasource,
         TANO,
         APPSHEETSERIALNO,
         TASERIALNO,
         BUSINESSCODE,
         TRANSACTIONCFMDATE,
         TAACCOUNTID,
         DISTRIBUTORCODE,
         TRANSACTIONACCOUNTID,
         BRANCHCODE,
         REGIONCODE,
         ACCEPTMETHOD,
         TRANSACTIONDATE,
         TRANSACTIONTIME,
         FROMTAFLAG,
         CUSTNO,
         INVESTNAME,
         INVESTTYPE,
         CERTIFICATETYPE,
         CERTIFICATENO,
         CERTIFICATEVALIDDATE,
         NATIONALITY,
         SEX,
         EDUCATIONLEVEL,
         VOCATIONCODE,
         INDUSTRYCODE,
         ANNUALINCOME,
         POSTCODE,
         ADDRESS,
         EMAIL,
         MOBILETELNO,
         MOBILETELNO2,
         HOMETELNO,
         OFFICETELNO,
         FAXNO,
         PAGERNO,
         BENEFITACCTNAME,
         BENEFITACCTNO,
         BENEFITBANKNAME,
         BENEFITBANKCODE,
         REGISTEREDOFFICE,
         BUSINESSSCOPE,
         TAXCERTIFICATENO,
         INSTREPRNAME,
         INSTREPRNATIONALITY,
         INSTREPRIDTYPE,
         INSTREPRIDCODE,
         RETURNCODE,
         RETURNMSG)
      values
        (t.datasource,
         t.TANO,
         t.APPSHEETSERIALNO,
         t.TASERIALNO,
         t.BUSINESSCODE,
         t.TRANSACTIONCFMDATE,
         t.TAACCOUNTID,
         t.DISTRIBUTORCODE,
         t.TRANSACTIONACCOUNTID,
         t.BRANCHCODE,
         t.REGIONCODE,
         t.ACCEPTMETHOD,
         t.TRANSACTIONDATE,
         t.TRANSACTIONTIME,
         t.FROMTAFLAG,
         t.CUSTNO,
         t.INVESTNAME,
         t.INVESTTYPE,
         t.CERTIFICATETYPE,
         t.CERTIFICATENO,
         t.CERTIFICATEVALIDDATE,
         t.NATIONALITY,
         t.SEX,
         t.EDUCATIONLEVEL,
         t.VOCATIONCODE,
         t.INDUSTRYCODE,
         t.ANNUALINCOME,
         t.POSTCODE,
         t.ADDRESS,
         t.EMAIL,
         t.MOBILETELNO,
         t.MOBILETELNO2,
         t.HOMETELNO,
         t.OFFICETELNO,
         t.FAXNO,
         t.PAGERNO,
         t.BENEFITACCTNAME,
         t.BENEFITACCTNO,
         t.BENEFITBANKNAME,
         t.BENEFITBANKCODE,
         t.REGISTEREDOFFICE,
         t.BUSINESSSCOPE,
         t.TAXCERTIFICATENO,
         t.INSTREPRNAME,
         t.INSTREPRNATIONALITY,
         t.INSTREPRIDTYPE,
         t.INSTREPRIDCODE,
         t.RETURNCODE,
         t.RETURNMSG);
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
  end cj_ods_ta_ack_acct;

  --采集投资者信息
  procedure cj_ods_ta_acct_cust(errcode out int, errmsg out varchar2) is
     vn_distributorcode  varchar2(1024);
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

    --默认销售商
    select nvl(t1.paramvalue,t.defaultvalue) into vn_distributorcode from  sys_param_define t
        left join sys_param_value t1 on t.paramid=t1.paramid
        where t.paramid='76101';

    --更新临时表数据
    --update t_ta_acct_cust t
    --set t.certificatetype = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'2',trim(t.certificatetype)),
    --    t.vocationcode = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'0',trim(t.vocationcode))
    --where t.investtype = '1';

    --update t_ta_acct_cust t
    --set t.certificatetype = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'3',trim(t.certificatetype)),
    --    t.industrycode = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'1',trim(t.industrycode))
    --where t.investtype = '0';

    --法人代表
    --update t_ta_acct_cust t
    --set t.INSTREPRIDTYPE = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'2',trim(t.INSTREPRIDTYPE));

    --控制人或实际控股人
    --update t_ta_acct_cust t
    --set t.STOCKHOLDERIDTYPE = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'2',trim(t.STOCKHOLDERIDTYPE));

    --经办人
    --update t_ta_acct_cust t
    --set t.TRANSACTORCERTTYPE = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'2',trim(t.TRANSACTORCERTTYPE));

    --客户国籍
    --update t_ta_acct_cust t
    --set t.NATIONALITY = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'10',trim(t.NATIONALITY));

    --经办人国籍
    --update t_ta_acct_cust t
    --set t.INSTREPRNATIONALITY = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'10',trim(t.INSTREPRNATIONALITY));

    --控股股东国籍
    --update t_ta_acct_cust t
    --set t.STOCKHOLDERNATIONALITY = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'10',trim(t.STOCKHOLDERNATIONALITY));

    --性别
    --update t_ta_acct_cust t
    --set t.sex = pkg_pub.f_get_dictmap('rdt','0',vn_distributorcode,'15',trim(t.sex));

    --更新地区代码 add by lvz 20190318
    update t_ta_acct_cust t
    set t.areacode = pkg_pub_rh.f_get_areacode_rh(t.investname,t.address,t.areacode,t.investtype,t.certificatetype,
    t.certificateno,t.postcode,t.instrepridtype,t.instrepridcode,t.transactorcerttype,
    t.transactorcertno);

    --更新投资者细分类型 add bu lvz 20190318
    update t_ta_acct_cust t
    set t.investtypedetail = pkg_pub_rh.f_get_investtype_rh(t.investtype,t.certificatetype,t.investname);

    update t_ta_acct_cust t
       set t.certificatetype = pkg_pub.f_get_dictmap('rdt','0','***','10',t.certificatetype)
    where t.datasource = '2'; --ETFTA

    --处理投资者分类转换
    update t_ta_acct_cust t
    set t.institutiontype=pkg_pub.f_trans_institutiontype(t.investname,t.investtype,t.institutiontype);

    update t_ta_acct_cust t
    set t.investtypeclassify=pkg_pub.f_get_dictmap('rdt','0','***','t2zj',t.institutiontype);

    --博时资本 资管TA转换对应
     update t_ta_acct_cust t
    set t.investtypeclassify=decode(t.investtype,'1','001',
     (case when length(pkg_pub.f_get_dictmap('rdt','0','***','ORGI',t.investtype||t.orginvesttype)) != 3
       then pkg_pub.f_trans_institutiontype(t.investname,t.investtype,t.institutiontype)
     else
       pkg_pub.f_get_dictmap('rdt','0','***','ORGI',t.investtype||t.orginvesttype)
     end))
     where t.datasource = '3';

    update t_ta_acct_cust t
       set t.certificatetype = pkg_pub.f_get_dictmap('rdt','0','***','10',t.certificatetype)
    where t.datasource = '3';
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    --更新投资者类型 投资者类型为机构，但实际为产品的；将投资者类型更新为产品
    update t_ta_acct_cust t set t.investtype = '2'
    where t.investtype = '0' and t.investtypeclassify like '2%';
    	--统计行数
	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;
    commit;

    merge into ods_ta_acct_cust ods
    using (select * from t_ta_acct_cust) t
    on (ods.DATASOURCE = t.DATASOURCE and ods.TAACCOUNTID = t.TAACCOUNTID)
    when matched then
      update
         set --ods.datasource                   = t.datasource,
             ods.TANO                         = t.TANO,
             --ods.TAACCOUNTID                  = t.TAACCOUNTID,
             ods.CUSTNO                       = t.CUSTNO,
             ods.INVESTNAME                   = t.INVESTNAME,
             --ods.INVESTTYPE                   = t.INVESTTYPE,
             ods.CERTIFICATETYPE              = t.CERTIFICATETYPE,
             ods.CERTIFICATENO                = t.CERTIFICATENO,
             ods.CERTIFICATEVALIDDATE         = t.CERTIFICATEVALIDDATE,
             ods.INSTITUTIONTYPE              = t.INSTITUTIONTYPE,
             ods.NATIONALITY                  = t.NATIONALITY,
             ods.SEX                          = t.SEX,
             ods.EDUCATIONLEVEL               = t.EDUCATIONLEVEL,
             ods.VOCATIONCODE                 = t.VOCATIONCODE,
             ods.ANNUALINCOME                 = t.ANNUALINCOME,
             ods.INDUSTRYCODE                 = t.INDUSTRYCODE,
             ods.EMAIL                        = t.EMAIL,
             --ods.AREACODE                     = t.AREACODE,
             ods.MOBILETELNO                  = t.MOBILETELNO,
             ods.MOBILETELNO2                 = t.MOBILETELNO2,
             ods.HOMETELNO                    = t.HOMETELNO,
             ods.ADDRESS                      = t.ADDRESS,
             ods.POSTCODE                     = t.POSTCODE,
             ods.REGISTEREDOFFICE             = t.REGISTEREDOFFICE,
             ods.BUSINESSSCOPE                = t.BUSINESSSCOPE,
             ods.TAXCERTIFICATENO             = t.TAXCERTIFICATENO,
             ods.INSTREPRNAME                 = t.INSTREPRNAME,
             ods.INSTREPRNATIONALITY          = t.INSTREPRNATIONALITY,
             ods.INSTREPRIDTYPE               = t.INSTREPRIDTYPE,
             ods.INSTREPRIDCODE               = t.INSTREPRIDCODE,
             ods.INSTREPRCERTIFICATEVALIDDATE = t.INSTREPRCERTIFICATEVALIDDATE,
             ods.STOCKHOLDERNAME              = t.STOCKHOLDERNAME,
             ods.STOCKHOLDERNATIONALITY       = t.STOCKHOLDERNATIONALITY,
             ods.STOCKHOLDERIDTYPE            = t.STOCKHOLDERIDTYPE,
             ods.STOCKHOLDERIDCODE            = t.STOCKHOLDERIDCODE,
             ods.STOCKHOLDERIDVALIDDATE       = t.STOCKHOLDERIDVALIDDATE,
             ods.STATUS                       = t.STATUS,
             ods.OPENDATE                     = t.OPENDATE,
             ods.CANCELDATE                   = t.CANCELDATE,
             ods.TRANSACTORNAME               = t.TRANSACTORNAME,
             ods.TRANSACTORCERTTYPE           = t.TRANSACTORCERTTYPE,
             ods.TRANSACTORCERTNO             = t.TRANSACTORCERTNO,
             ods.TRANSACTORVALIDDATE          = t.TRANSACTORVALIDDATE,
             ods.MONEYACCOUNT                 = t.MONEYACCOUNT,
             ods.DEPOSITACCT                  = t.DEPOSITACCT,
             ods.DEPOSITACCTNAME              = t.DEPOSITACCTNAME,
             ods.REGISTCAPITAL                = t.REGISTCAPITAL,
             ods.CURRENY                      = t.CURRENY,
             ods.BIRTHDAY                     = t.BIRTHDAY,
             ods.ASSOCIATEMONEY               = t.ASSOCIATEMONEY,
             ods.TRANSACTORMOBILETELNO        = t.TRANSACTORMOBILETELNO,
             ods.HOMEANNUALINCOME             = t.HOMEANNUALINCOME,
             ods.orginvesttype                = t.orginvesttype
    when not matched then
      insert
        (datasource,
         TANO,
         TAACCOUNTID,
         CUSTNO,
         INVESTNAME,
         INVESTTYPE,
         CERTIFICATETYPE,
         CERTIFICATENO,
         CERTIFICATEVALIDDATE,
         INSTITUTIONTYPE,
         NATIONALITY,
         SEX,
         EDUCATIONLEVEL,
         VOCATIONCODE,
         ANNUALINCOME,
         INDUSTRYCODE,
         EMAIL,
         AREACODE,
         MOBILETELNO,
         MOBILETELNO2,
         HOMETELNO,
         ADDRESS,
         POSTCODE,
         REGISTEREDOFFICE,
         BUSINESSSCOPE,
         TAXCERTIFICATENO,
         INSTREPRNAME,
         INSTREPRNATIONALITY,
         INSTREPRIDTYPE,
         INSTREPRIDCODE,
         INSTREPRCERTIFICATEVALIDDATE,
         STOCKHOLDERNAME,
         STOCKHOLDERNATIONALITY,
         STOCKHOLDERIDTYPE,
         STOCKHOLDERIDCODE,
         STOCKHOLDERIDVALIDDATE,
         STATUS,
         OPENDATE,
         CANCELDATE,
         TRANSACTORNAME,
         TRANSACTORCERTTYPE,
         TRANSACTORCERTNO,
         TRANSACTORVALIDDATE,
         MONEYACCOUNT,
         DEPOSITACCT,
         DEPOSITACCTNAME,
         REGISTCAPITAL,
         CURRENY,
         BIRTHDAY,
         ASSOCIATEMONEY,
         TRANSACTORMOBILETELNO,
         INVESTTYPEDETAIL,
         INVESTTYPECLASSIFY,
         HOMEANNUALINCOME)
      values
        (t.datasource,
         t.TANO,
         t.TAACCOUNTID,
         t.CUSTNO,
         t.INVESTNAME,
         t.INVESTTYPE,
         t.CERTIFICATETYPE,
         t.CERTIFICATENO,
         t.CERTIFICATEVALIDDATE,
         t.INSTITUTIONTYPE,
         t.NATIONALITY,
         t.SEX,
         t.EDUCATIONLEVEL,
         t.VOCATIONCODE,
         t.ANNUALINCOME,
         t.INDUSTRYCODE,
         t.EMAIL,
         t.AREACODE,
         t.MOBILETELNO,
         t.MOBILETELNO2,
         t.HOMETELNO,
         t.ADDRESS,
         t.POSTCODE,
         t.REGISTEREDOFFICE,
         t.BUSINESSSCOPE,
         t.TAXCERTIFICATENO,
         t.INSTREPRNAME,
         t.INSTREPRNATIONALITY,
         t.INSTREPRIDTYPE,
         t.INSTREPRIDCODE,
         t.INSTREPRCERTIFICATEVALIDDATE,
         t.STOCKHOLDERNAME,
         t.STOCKHOLDERNATIONALITY,
         t.STOCKHOLDERIDTYPE,
         t.STOCKHOLDERIDCODE,
         t.STOCKHOLDERIDVALIDDATE,
         t.STATUS,
         t.OPENDATE,
         t.CANCELDATE,
         t.TRANSACTORNAME,
         t.TRANSACTORCERTTYPE,
         t.TRANSACTORCERTNO,
         t.TRANSACTORVALIDDATE,
         t.MONEYACCOUNT,
         t.DEPOSITACCT,
         t.DEPOSITACCTNAME,
         t.REGISTCAPITAL,
         t.CURRENY,
         t.BIRTHDAY,
         t.ASSOCIATEMONEY,
         t.TRANSACTORMOBILETELNO,
         t.investtypedetail,
         t.investtypeclassify,
         t.HOMEANNUALINCOME);
  --从2.0.2升级到2.0.3时不会处理历史数据,额外添加处理
  update ods_ta_acct_cust t
     set t.institutiontype = pkg_pub.f_trans_institutiontype(t.investname,
                                                             t.investtype,
                                                             t.institutiontype)
   where t.institutiontype is null
      or t.institutiontype not in
         (select d.subitem from sys_dictvalue d where d.dictitem = '76400');
  update ods_ta_acct_cust t
     set t.investtypeclassify = pkg_pub.f_get_dictmap('rdt',
                                                      '0',
                                                      '***',
                                                      't2zj',
                                                      t.institutiontype)
   where t.investtypeclassify is null
      or t.investtypeclassify not in
         (select t.mapafter from rdt_dict_map t where t.mapcolumntype = 't2zj');
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
  end cj_ods_ta_acct_cust;

  --采集交易确认
  procedure cj_ods_ta_ack_trans(errcode out int, errmsg out varchar2) is

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

    merge into ods_ta_ack_trans ods
    using (select * from t_ta_ack_trans) t
    on (ods.DATASOURCE = t.DATASOURCE and ods.TASERIALNO = t.TASERIALNO and ods.TRANSACTIONCFMDATE = t.TRANSACTIONCFMDATE)
    when matched then
      update
         set --ods.datasource                 = t.datasource,
             ods.TANO                       = t.TANO,
             ods.APPSHEETSERIALNO           = t.APPSHEETSERIALNO,
             --ods.TASERIALNO                 = t.TASERIALNO,
             --ods.TRANSACTIONCFMDATE         = t.TRANSACTIONCFMDATE,
             ods.DETAILFLAG                 = t.DETAILFLAG,
             ods.FROMTAFLAG                 = t.FROMTAFLAG,
             ods.CUSTNO                     = t.CUSTNO,
             ods.INVESTTYPE                 = t.INVESTTYPE,
             ods.TAACCOUNTID                = t.TAACCOUNTID,
             ods.DISTRIBUTORCODE            = t.DISTRIBUTORCODE,
             ods.TRANSACTIONACCOUNTID       = t.TRANSACTIONACCOUNTID,
             ods.BRANCHCODE                 = t.BRANCHCODE,
             ods.REGIONCODE                 = t.REGIONCODE,
             ods.CUSTTYPE                   = t.CUSTTYPE,
             ods.FUNDCODE                   = t.FUNDCODE,
             ods.SHARETYPE                  = t.SHARETYPE,
             ods.CONTRACTNO                 = t.CONTRACTNO,
             ods.ICONTRACTNO                = t.ICONTRACTNO,
             ods.BUSINESSCODE               = t.BUSINESSCODE,
             ods.ACCEPTMETHOD               = t.ACCEPTMETHOD,
             ods.TRANSACTIONDATE            = t.TRANSACTIONDATE,
             ods.TRANSACTIONTIME            = t.TRANSACTIONTIME,
             ods.CURRENCYTYPE               = t.CURRENCYTYPE,
             ods.APPLICATIONVOL             = t.APPLICATIONVOL,
             ods.APPLICATIONAMOUNT          = t.APPLICATIONAMOUNT,
             ods.CONFIRMEDVOL               = t.CONFIRMEDVOL,
             ods.CONFIRMEDAMOUNT            = t.CONFIRMEDAMOUNT,
             ods.TARGETTAACCOUNTID          = t.TARGETTAACCOUNTID,
             ods.TARGETDISTRIBUTORCODE      = t.TARGETDISTRIBUTORCODE,
             ods.TARGETTRANSACTIONACCOUNTID = t.TARGETTRANSACTIONACCOUNTID,
             ods.TARGETBRANCHCODE           = t.TARGETBRANCHCODE,
             ods.TARGETFUNDCODE             = t.TARGETFUNDCODE,
             ods.TARGETSHARETYPE            = t.TARGETSHARETYPE,
             ods.ORIGINALSERIALNO           = t.ORIGINALSERIALNO,
             ods.ORIGINALAPPSHEETNO         = t.ORIGINALAPPSHEETNO,
             ods.CUSTMANAGERID              = t.CUSTMANAGERID,
             ods.NAV                        = t.NAV,
             ods.OPERWAY                    = t.OPERWAY,
             ods.CHARGE                     = t.CHARGE,
             ods.INSERTDATE                 = t.INSERTDATE,
             ods.RETURNCODE                 = t.RETURNCODE,
             ods.RETURNMSG                  = t.RETURNMSG,
             ods.RATEFEE                    = t.RATEFEE,
             ods.MINFEE                     = t.MINFEE,
             ods.TOTALTRANSFEE              = t.TOTALTRANSFEE,
             ods.STAMPDUTY                  = t.STAMPDUTY,
             ods.TRANSFERFEE                = t.TRANSFERFEE,
             ods.HANDLECHARGE               = t.HANDLECHARGE,
             ods.TAX                        = t.TAX,
             ods.OTHERFEE1                  = t.OTHERFEE1,
             ods.OTHERFEE2                  = t.OTHERFEE2,
             ods.OTHERFEE3                  = t.OTHERFEE3,
             ods.BACKFARESHARE              = t.BACKFARESHARE,
             ods.TOTALBACKENDLOAD           = t.TOTALBACKENDLOAD,
             ods.BACKFARE                   = t.BACKFARE,
             ods.RECUPERATEFEE              = t.RECUPERATEFEE,
             ods.CHANGEFEE                  = t.CHANGEFEE,
             ods.FUNDCHARGE                 = t.FUNDCHARGE,
             ods.MANAGERCHARGE              = t.MANAGERCHARGE,
             ods.DISTRIBUTORCHARGE          = t.DISTRIBUTORCHARGE,
             ods.BACKFUNDCHARGE             = t.BACKFUNDCHARGE,
             ods.BACKMANAGERCHARGE          = t.BACKMANAGERCHARGE,
             ods.BACKDISTRIBUTORCHARGE      = t.BACKDISTRIBUTORCHARGE,
             ods.ACHIEVEMENTPAY             = t.ACHIEVEMENTPAY,
             ods.INTEREST                   = t.INTEREST,
             ods.INTERESTTAX                = t.INTERESTTAX,
             ods.VOLUMEBYINTEREST           = t.VOLUMEBYINTEREST,
             ods.REDEMPTIONREASON           = t.REDEMPTIONREASON,
             ods.BUSINESSFINISHFLAG         = t.BUSINESSFINISHFLAG,
             ods.defdividendmethod          = t.defdividendmethod,
             ods.dividendperunit            = t.dividendperunit,
             ods.basisforcalculatingdividend= t.basisforcalculatingdividend,
             ods.registrationdate           = t.registrationdate
    when not matched then
      insert
        (datasource,
         tano,
         APPSHEETSERIALNO,
         TASERIALNO,
         TRANSACTIONCFMDATE,
         DETAILFLAG,
         FROMTAFLAG,
         CUSTNO,
         INVESTTYPE,
         TAACCOUNTID,
         DISTRIBUTORCODE,
         TRANSACTIONACCOUNTID,
         BRANCHCODE,
         REGIONCODE,
         CUSTTYPE,
         FUNDCODE,
         SHARETYPE,
         CONTRACTNO,
         ICONTRACTNO,
         BUSINESSCODE,
         ACCEPTMETHOD,
         TRANSACTIONDATE,
         TRANSACTIONTIME,
         CURRENCYTYPE,
         APPLICATIONVOL,
         APPLICATIONAMOUNT,
         CONFIRMEDVOL,
         CONFIRMEDAMOUNT,
         TARGETTAACCOUNTID,
         TARGETDISTRIBUTORCODE,
         TARGETTRANSACTIONACCOUNTID,
         TARGETBRANCHCODE,
         TARGETFUNDCODE,
         TARGETSHARETYPE,
         ORIGINALSERIALNO,
         ORIGINALAPPSHEETNO,
         CUSTMANAGERID,
         NAV,
         OPERWAY,
         CHARGE,
         INSERTDATE,
         RETURNCODE,
         RETURNMSG,
         RATEFEE,
         MINFEE,
         TOTALTRANSFEE,
         STAMPDUTY,
         TRANSFERFEE,
         HANDLECHARGE,
         TAX,
         OTHERFEE1,
         OTHERFEE2,
         OTHERFEE3,
         BACKFARESHARE,
         TOTALBACKENDLOAD,
         BACKFARE,
         RECUPERATEFEE,
         CHANGEFEE,
         FUNDCHARGE,
         MANAGERCHARGE,
         DISTRIBUTORCHARGE,
         BACKFUNDCHARGE,
         BACKMANAGERCHARGE,
         BACKDISTRIBUTORCHARGE,
         ACHIEVEMENTPAY,
         INTEREST,
         INTERESTTAX,
         VOLUMEBYINTEREST,
         REDEMPTIONREASON,
         BUSINESSFINISHFLAG,
         defdividendmethod,
         dividendperunit,
         basisforcalculatingdividend,
         registrationdate)
      values
        (t.datasource,
         t.tano,
         t.APPSHEETSERIALNO,
         t.TASERIALNO,
         t.TRANSACTIONCFMDATE,
         t.DETAILFLAG,
         t.FROMTAFLAG,
         t.CUSTNO,
         t.INVESTTYPE,
         t.TAACCOUNTID,
         t.DISTRIBUTORCODE,
         t.TRANSACTIONACCOUNTID,
         t.BRANCHCODE,
         t.REGIONCODE,
         t.CUSTTYPE,
         t.FUNDCODE,
         t.SHARETYPE,
         t.CONTRACTNO,
         t.ICONTRACTNO,
         t.BUSINESSCODE,
         t.ACCEPTMETHOD,
         t.TRANSACTIONDATE,
         t.TRANSACTIONTIME,
         t.CURRENCYTYPE,
         t.APPLICATIONVOL,
         t.APPLICATIONAMOUNT,
         t.CONFIRMEDVOL,
         t.CONFIRMEDAMOUNT,
         t.TARGETTAACCOUNTID,
         t.TARGETDISTRIBUTORCODE,
         t.TARGETTRANSACTIONACCOUNTID,
         t.TARGETBRANCHCODE,
         t.TARGETFUNDCODE,
         t.TARGETSHARETYPE,
         t.ORIGINALSERIALNO,
         t.ORIGINALAPPSHEETNO,
         t.CUSTMANAGERID,
         t.NAV,
         t.OPERWAY,
         t.CHARGE,
         t.INSERTDATE,
         t.RETURNCODE,
         t.RETURNMSG,
         t.RATEFEE,
         t.MINFEE,
         t.TOTALTRANSFEE,
         t.STAMPDUTY,
         t.TRANSFERFEE,
         t.HANDLECHARGE,
         t.TAX,
         t.OTHERFEE1,
         t.OTHERFEE2,
         t.OTHERFEE3,
         t.BACKFARESHARE,
         t.TOTALBACKENDLOAD,
         t.BACKFARE,
         t.RECUPERATEFEE,
         t.CHANGEFEE,
         t.FUNDCHARGE,
         t.MANAGERCHARGE,
         t.DISTRIBUTORCHARGE,
         t.BACKFUNDCHARGE,
         t.BACKMANAGERCHARGE,
         t.BACKDISTRIBUTORCHARGE,
         t.ACHIEVEMENTPAY,
         t.INTEREST,
         t.INTERESTTAX,
         t.VOLUMEBYINTEREST,
         t.REDEMPTIONREASON,
         t.BUSINESSFINISHFLAG,
         t.defdividendmethod,
         t.dividendperunit,
         t.basisforcalculatingdividend,
         t.registrationdate);
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
  end cj_ods_ta_ack_trans;

  --采集份额汇总
  procedure cj_ods_ta_bal_fund(errcode out int, errmsg out varchar2) is

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

    merge into ods_ta_bal_fund ods
    using (select * from t_ta_bal_fund) t
    on (ods.DATASOURCE = t.DATASOURCE and ods.tano = t.tano and ods.FUNDCODE = t.fundcode
    and ods.TAACCOUNTID = t.taaccountid and ods.DISTRIBUTORCODE = t.distributorcode
    and ods.BRANCHCODE = t.branchcode and ods.TRANSACTIONACCOUNTID = t.transactionaccountid)
    when matched then
      update
         set --ods.datasource               = t.datasource,
             --ods.TANO                     = t.TANO,
             --ods.FUNDCODE                 = t.FUNDCODE,
             --ods.TAACCOUNTID              = t.TAACCOUNTID,
             --ods.DISTRIBUTORCODE          = t.DISTRIBUTORCODE,
             --ods.TRANSACTIONACCOUNTID     = t.TRANSACTIONACCOUNTID,
             --ods.BRANCHCODE               = t.BRANCHCODE,
             ods.CONTRACTNO               = t.CONTRACTNO,
             ods.SHARETYPE                = t.SHARETYPE,
             ods.SHAREFLAG                = t.SHAREFLAG,
             ods.LAST_SHARECLASS          = t.LAST_SHARECLASS,
             ods.SHARECLASS               = t.SHARECLASS,
             ods.LAST_FUNDVOL             = t.LAST_FUNDVOL,
             ods.FUNDVOL                  = t.FUNDVOL,
             ods.LAST_FROZEN              = t.LAST_FROZEN,
             ods.FROZEN                   = t.FROZEN,
             ods.LAST_ABNMFROZEN          = t.LAST_ABNMFROZEN,
             ods.ABNMFROZEN               = t.ABNMFROZEN,
             ods.LAST_UNFOOTINCOME        = t.LAST_UNFOOTINCOME,
             ods.UNFOOTINCOME             = t.UNFOOTINCOME,
             ods.LAST_UNFOOTFROZENINCOME  = t.LAST_UNFOOTFROZENINCOME,
             ods.UNFOOTFROZENINCOME       = t.UNFOOTFROZENINCOME,
             ods.LAST_GUARANTEEDAMOUNT    = t.LAST_GUARANTEEDAMOUNT,
             ods.GUARANTEEDAMOUNT         = t.GUARANTEEDAMOUNT,
             ods.DEFDIVIDENDMETHODACCT    = t.DEFDIVIDENDMETHODACCT,
             ods.LAST_DEFDIVIDENDMETHOD   = t.LAST_DEFDIVIDENDMETHOD,
             ods.DEFDIVIDENDMETHOD        = t.DEFDIVIDENDMETHOD,
             ods.DIVIDENDRIGHTS           = t.DIVIDENDRIGHTS,
             ods.FROZEN_DIVIDENDRIGHTS    = t.FROZEN_DIVIDENDRIGHTS,
             ods.REMUNERATION             = t.REMUNERATION,
             ods.DIVIDENDCASH             = t.DIVIDENDCASH,
             ods.FROZEN_DIVIDENDCASH      = t.FROZEN_DIVIDENDCASH,
             ods.ACCTFROZEN_DIVIDENDCASH  = t.ACCTFROZEN_DIVIDENDCASH,
             ods.TRDFROZEN_DIVIDENDCASH   = t.TRDFROZEN_DIVIDENDCASH,
             ods.DIVIDENDSHARE            = t.DIVIDENDSHARE,
             ods.FROZEN_DIVIDENDSHARE     = t.FROZEN_DIVIDENDSHARE,
             ods.ACCTFROZEN_DIVIDENDSHARE = t.ACCTFROZEN_DIVIDENDSHARE,
             ods.TRDFROZEN_DIVIDENDSHARE  = t.TRDFROZEN_DIVIDENDSHARE,
             ods.FOOTINCOME               = t.FOOTINCOME,
             ods.TOTINVESTAMT             = t.TOTINVESTAMT,
             ods.TOTINCOMEAMT             = t.TOTINCOMEAMT,
             ods.FIRSTSUBDATE             = t.FIRSTSUBDATE,
             ods.TAADJUST                 = t.TAADJUST,
             ods.INSERTDATE               = t.INSERTDATE,
             ods.LASTUPDATEDATE           = t.LASTUPDATEDATE
    when not matched then
      insert
        (datasource,
         TANO,
         FUNDCODE,
         TAACCOUNTID,
         DISTRIBUTORCODE,
         TRANSACTIONACCOUNTID,
         BRANCHCODE,
         CONTRACTNO,
         SHARETYPE,
         SHAREFLAG,
         LAST_SHARECLASS,
         SHARECLASS,
         LAST_FUNDVOL,
         FUNDVOL,
         LAST_FROZEN,
         FROZEN,
         LAST_ABNMFROZEN,
         ABNMFROZEN,
         LAST_UNFOOTINCOME,
         UNFOOTINCOME,
         LAST_UNFOOTFROZENINCOME,
         UNFOOTFROZENINCOME,
         LAST_GUARANTEEDAMOUNT,
         GUARANTEEDAMOUNT,
         DEFDIVIDENDMETHODACCT,
         LAST_DEFDIVIDENDMETHOD,
         DEFDIVIDENDMETHOD,
         DIVIDENDRIGHTS,
         FROZEN_DIVIDENDRIGHTS,
         REMUNERATION,
         DIVIDENDCASH,
         FROZEN_DIVIDENDCASH,
         ACCTFROZEN_DIVIDENDCASH,
         TRDFROZEN_DIVIDENDCASH,
         DIVIDENDSHARE,
         FROZEN_DIVIDENDSHARE,
         ACCTFROZEN_DIVIDENDSHARE,
         TRDFROZEN_DIVIDENDSHARE,
         FOOTINCOME,
         TOTINVESTAMT,
         TOTINCOMEAMT,
         FIRSTSUBDATE,
         TAADJUST,
         INSERTDATE,
         LASTUPDATEDATE)
      values
        (t.datasource,
         t.TANO,
         t.FUNDCODE,
         t.TAACCOUNTID,
         t.DISTRIBUTORCODE,
         t.TRANSACTIONACCOUNTID,
         t.BRANCHCODE,
         t.CONTRACTNO,
         t.SHARETYPE,
         t.SHAREFLAG,
         t.LAST_SHARECLASS,
         t.SHARECLASS,
         t.LAST_FUNDVOL,
         t.FUNDVOL,
         t.LAST_FROZEN,
         t.FROZEN,
         t.LAST_ABNMFROZEN,
         t.ABNMFROZEN,
         t.LAST_UNFOOTINCOME,
         t.UNFOOTINCOME,
         t.LAST_UNFOOTFROZENINCOME,
         t.UNFOOTFROZENINCOME,
         t.LAST_GUARANTEEDAMOUNT,
         t.GUARANTEEDAMOUNT,
         t.DEFDIVIDENDMETHODACCT,
         t.LAST_DEFDIVIDENDMETHOD,
         t.DEFDIVIDENDMETHOD,
         t.DIVIDENDRIGHTS,
         t.FROZEN_DIVIDENDRIGHTS,
         t.REMUNERATION,
         t.DIVIDENDCASH,
         t.FROZEN_DIVIDENDCASH,
         t.ACCTFROZEN_DIVIDENDCASH,
         t.TRDFROZEN_DIVIDENDCASH,
         t.DIVIDENDSHARE,
         t.FROZEN_DIVIDENDSHARE,
         t.ACCTFROZEN_DIVIDENDSHARE,
         t.TRDFROZEN_DIVIDENDSHARE,
         t.FOOTINCOME,
         t.TOTINVESTAMT,
         t.TOTINCOMEAMT,
         t.FIRSTSUBDATE,
         t.TAADJUST,
         t.INSERTDATE,
         t.LASTUPDATEDATE);
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
  end cj_ods_ta_bal_fund;

  --采集份额明细
  procedure cj_ods_ta_bal_detail(errcode out int, errmsg out varchar2) is

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

    merge into ods_ta_bal_detail ods
    using (select * from t_ta_bal_detail) t
    on (ods.DATASOURCE = t.DATASOURCE and ods.TASERIALNO = t.TASERIALNO and ods.ORIGINALSERIALNO = t.ORIGINALSERIALNO and ods.INSERTDATE = t.INSERTDATE)
    when matched then
      update
         set --ods.datasource             = t.datasource,
             ods.FUNDCODE               = t.FUNDCODE,
             ods.TAACCOUNTID            = t.TAACCOUNTID,
             ods.DISTRIBUTORCODE        = t.DISTRIBUTORCODE,
             ods.TRANSACTIONACCOUNTID   = t.TRANSACTIONACCOUNTID,
             ods.BRANCHCODE             = t.BRANCHCODE,
             ods.CONTRACTNO             = t.CONTRACTNO,
             --ods.TASERIALNO             = t.TASERIALNO,
             --ods.ORIGINALSERIALNO       = t.ORIGINALSERIALNO,
             ods.APPSHEETSERIALNO       = t.APPSHEETSERIALNO,
             ods.BUSINESSCODE           = t.BUSINESSCODE,
             ods.TRANSACTIONCFMDATE     = t.TRANSACTIONCFMDATE,
             ods.SHAREREGISTERDATE      = t.SHAREREGISTERDATE,
             ods.SHARETYPE              = t.SHARETYPE,
             ods.LAST_FUNDVOL           = t.LAST_FUNDVOL,
             ods.CONFIRMEDVOL           = t.CONFIRMEDVOL,
             ods.CONFIRMEDAMOUNT        = t.CONFIRMEDAMOUNT,
             ods.FUNDVOL                = t.FUNDVOL,
             ods.LAST_FROZEN            = t.LAST_FROZEN,
             ods.FROZEN                 = t.FROZEN,
             ods.LAST_ABNMFROZEN        = t.LAST_ABNMFROZEN,
             ods.ABNMFROZEN             = t.ABNMFROZEN,
             ods.LAST_DEFDIVIDENDMETHOD = t.LAST_DEFDIVIDENDMETHOD,
             ods.DEFDIVIDENDMETHOD      = t.DEFDIVIDENDMETHOD,
             ods.DIVIDENDRIGHTS         = t.DIVIDENDRIGHTS,
             ods.FROZEN_DIVIDENDRIGHTS  = t.FROZEN_DIVIDENDRIGHTS,
             ods.DIVIDENDCASH           = t.DIVIDENDCASH,
             ods.FROZEN_DIVIDENDCASH    = t.FROZEN_DIVIDENDCASH,
             ods.DIVIDENDSHARE          = t.DIVIDENDSHARE,
             ods.FROZEN_DIVIDENDSHARE   = t.FROZEN_DIVIDENDSHARE,
             ods.LAST_DIVIDEND_SUM      = t.LAST_DIVIDEND_SUM,
             ods.TODAY_DIVIDEND         = t.TODAY_DIVIDEND,
             ods.NAV                    = t.NAV,
             ods.PROTOCOLCODE           = t.PROTOCOLCODE,
             ods.TAADJUST               = t.TAADJUST,
             --ods.INSERTDATE             = t.INSERTDATE,
             ods.LASTUPDATEDATE         = t.LASTUPDATEDATE
    when not matched then
      insert
        (datasource,
         FUNDCODE,
         TAACCOUNTID,
         DISTRIBUTORCODE,
         TRANSACTIONACCOUNTID,
         BRANCHCODE,
         CONTRACTNO,
         TASERIALNO,
         ORIGINALSERIALNO,
         APPSHEETSERIALNO,
         BUSINESSCODE,
         TRANSACTIONCFMDATE,
         SHAREREGISTERDATE,
         SHARETYPE,
         LAST_FUNDVOL,
         CONFIRMEDVOL,
         CONFIRMEDAMOUNT,
         FUNDVOL,
         LAST_FROZEN,
         FROZEN,
         LAST_ABNMFROZEN,
         ABNMFROZEN,
         LAST_DEFDIVIDENDMETHOD,
         DEFDIVIDENDMETHOD,
         DIVIDENDRIGHTS,
         FROZEN_DIVIDENDRIGHTS,
         DIVIDENDCASH,
         FROZEN_DIVIDENDCASH,
         DIVIDENDSHARE,
         FROZEN_DIVIDENDSHARE,
         LAST_DIVIDEND_SUM,
         TODAY_DIVIDEND,
         NAV,
         PROTOCOLCODE,
         TAADJUST,
         INSERTDATE,
         LASTUPDATEDATE)
      values
        (t.datasource,
         t.FUNDCODE,
         t.TAACCOUNTID,
         t.DISTRIBUTORCODE,
         t.TRANSACTIONACCOUNTID,
         t.BRANCHCODE,
         t.CONTRACTNO,
         t.TASERIALNO,
         t.ORIGINALSERIALNO,
         t.APPSHEETSERIALNO,
         t.BUSINESSCODE,
         t.TRANSACTIONCFMDATE,
         t.SHAREREGISTERDATE,
         t.SHARETYPE,
         t.LAST_FUNDVOL,
         t.CONFIRMEDVOL,
         t.CONFIRMEDAMOUNT,
         t.FUNDVOL,
         t.LAST_FROZEN,
         t.FROZEN,
         t.LAST_ABNMFROZEN,
         t.ABNMFROZEN,
         t.LAST_DEFDIVIDENDMETHOD,
         t.DEFDIVIDENDMETHOD,
         t.DIVIDENDRIGHTS,
         t.FROZEN_DIVIDENDRIGHTS,
         t.DIVIDENDCASH,
         t.FROZEN_DIVIDENDCASH,
         t.DIVIDENDSHARE,
         t.FROZEN_DIVIDENDSHARE,
         t.LAST_DIVIDEND_SUM,
         t.TODAY_DIVIDEND,
         t.NAV,
         t.PROTOCOLCODE,
         t.TAADJUST,
         t.INSERTDATE,
         t.LASTUPDATEDATE);
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
  end cj_ods_ta_bal_detail;

  --采集份变动
  procedure cj_ods_ta_bal_detail_chg(errcode out int, errmsg out varchar2) is

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

    merge into ods_ta_bal_detail_chg ods
    using (select * from t_ta_bal_detail_chg) t
    on (ods.DATASOURCE = t.DATASOURCE and ods.TASERIALNO = t.TASERIALNO)
    when matched then
      update
         set --ods.datasource                  = t.datasource,
             ods.TANO                        = t.TANO,
             ods.detailflag                  = t.detailflag,
             ods.FUNDCODE                    = t.FUNDCODE,
             ods.TAACCOUNTID                 = t.TAACCOUNTID,
             ods.DISTRIBUTORCODE             = t.DISTRIBUTORCODE,
             ods.TRANSACTIONACCOUNTID        = t.TRANSACTIONACCOUNTID,
             ods.BRANCHCODE                  = t.BRANCHCODE,
             ods.CONTRACTNO                  = t.CONTRACTNO,
             ods.SNO                         = t.SNO,
             ods.RETURNMSG                   = t.RETURNMSG,
             ods.RETURNCODE                  = t.RETURNCODE,
             --ods.TASERIALNO                  = t.TASERIALNO,
             ods.TASERIALNOTOTAL             = t.TASERIALNOTOTAL,
             ods.APPSHEETSERIALNO            = t.APPSHEETSERIALNO,
             ods.ORIGINALTASERIALNO          = t.ORIGINALTASERIALNO,
             ods.ORIGINALAPPSHEETSERIALNO    = t.ORIGINALAPPSHEETSERIALNO,
             ods.ORIGINALBUSINESSCODE        = t.ORIGINALBUSINESSCODE,
             ods.SHAREREGISTERDATE           = t.SHAREREGISTERDATE,
             ods.ORIGINALCONFIRMEDVOL        = t.ORIGINALCONFIRMEDVOL,
             ods.BUSINESSCODE                = t.BUSINESSCODE,
             ods.TRANSACTIONCFMDATE          = t.TRANSACTIONCFMDATE,
             ods.SHARETYPE                   = t.SHARETYPE,
             ods.NAV                         = t.NAV,
             ods.CONFIRMEDVOL                = t.CONFIRMEDVOL,
             ods.CONFIRMEDAMOUNT             = t.CONFIRMEDAMOUNT,
             ods.LAST_FUNDVOL                = t.LAST_FUNDVOL,
             ods.LAST_ABNMFROZEN             = t.LAST_ABNMFROZEN,
             ods.AFTER_FUNDVOL               = t.AFTER_FUNDVOL,
             ods.AFTER_ABNMFROZEN            = t.AFTER_ABNMFROZEN,
             ods.FUNDVOL                     = t.FUNDVOL,
             ods.FROZEN                      = t.FROZEN,
             ods.ABNMFROZEN                  = t.ABNMFROZEN,
             ods.HFROZEN                     = t.HFROZEN,
             ods.UNFOOTINCOME                = t.UNFOOTINCOME,
             ods.UNFOOTFROZENINCOME          = t.UNFOOTFROZENINCOME,
             ods.FOOTINCOME                  = t.FOOTINCOME,
             ods.UNCOME                      = t.UNCOME,
             ods.MINUSFUNDVOL                = t.MINUSFUNDVOL,
             ods.LEFTVOL                     = t.LEFTVOL,
             ods.INSERTDATE                  = t.INSERTDATE,
             ods.DEFDIVIDENDMETHOD           = t.DEFDIVIDENDMETHOD,
             ods.BASISFORCALCULATINGDIVIDEND = t.BASISFORCALCULATINGDIVIDEND,
             ods.DIVIDENDPERUNIT             = t.DIVIDENDPERUNIT,
             ods.DIVIDENDAMOUNT              = t.DIVIDENDAMOUNT,
             ods.DIVIDENDINDEED              = t.DIVIDENDINDEED,
             ods.REGISTRATIONDATE            = t.REGISTRATIONDATE,
             ods.UNFROZENFLAG                = t.UNFROZENFLAG,
             ods.TRANSACTIONDATE             = t.TRANSACTIONDATE,
             ods.TRANSACTIONTIME             = t.TRANSACTIONTIME,
             ods.UNCOMEDIVIDENDSHARE         = t.UNCOMEDIVIDENDSHARE,
             ods.FROZENDIVIDENDSHARE         = t.FROZENDIVIDENDSHARE,
             ods.FROZENCAUSE                 = t.FROZENCAUSE,
             ods.FROZENDEADLINE              = t.FROZENDEADLINE,
             ods.ACCOUNTFROZENFLAG           = t.ACCOUNTFROZENFLAG,
             ods.FROMTAFLAG                  = t.FROMTAFLAG,
             ods.UNFROZENDATE                = t.UNFROZENDATE,
             ods.UNFROZENTIME                = t.UNFROZENTIME,
             ods.SHAREVALIDDATE              = t.SHAREVALIDDATE,
             ods.IPORATE                     = t.IPORATE,
             ods.TRADEPRICE                  = t.TRADEPRICE,
             ods.TRADEAPPID                  = t.TRADEAPPID,
             ods.MATCHEDID                   = t.MATCHEDID,
             ods.BEGINDATE                   = t.BEGINDATE
    when not matched then
      insert
        (datasource,
         TANO,
         detailflag,
         FUNDCODE,
         TAACCOUNTID,
         DISTRIBUTORCODE,
         TRANSACTIONACCOUNTID,
         BRANCHCODE,
         CONTRACTNO,
         SNO,
         RETURNMSG,
         RETURNCODE,
         TASERIALNO,
         TASERIALNOTOTAL,
         APPSHEETSERIALNO,
         ORIGINALTASERIALNO,
         ORIGINALAPPSHEETSERIALNO,
         ORIGINALBUSINESSCODE,
         SHAREREGISTERDATE,
         ORIGINALCONFIRMEDVOL,
         BUSINESSCODE,
         TRANSACTIONCFMDATE,
         SHARETYPE,
         NAV,
         CONFIRMEDVOL,
         CONFIRMEDAMOUNT,
         LAST_FUNDVOL,
         LAST_ABNMFROZEN,
         AFTER_FUNDVOL,
         AFTER_ABNMFROZEN,
         FUNDVOL,
         FROZEN,
         ABNMFROZEN,
         HFROZEN,
         UNFOOTINCOME,
         UNFOOTFROZENINCOME,
         FOOTINCOME,
         UNCOME,
         MINUSFUNDVOL,
         LEFTVOL,
         INSERTDATE,
         DEFDIVIDENDMETHOD,
         BASISFORCALCULATINGDIVIDEND,
         DIVIDENDPERUNIT,
         DIVIDENDAMOUNT,
         DIVIDENDINDEED,
         REGISTRATIONDATE,
         UNFROZENFLAG,
         TRANSACTIONDATE,
         TRANSACTIONTIME,
         UNCOMEDIVIDENDSHARE,
         FROZENDIVIDENDSHARE,
         FROZENCAUSE,
         FROZENDEADLINE,
         ACCOUNTFROZENFLAG,
         FROMTAFLAG,
         UNFROZENDATE,
         UNFROZENTIME,
         SHAREVALIDDATE,
         IPORATE,
         TRADEPRICE,
         TRADEAPPID,
         MATCHEDID,
         BEGINDATE)
      values
        (t.datasource,
         t.TANO,
         t.detailflag,
         t.FUNDCODE,
         t.TAACCOUNTID,
         t.DISTRIBUTORCODE,
         t.TRANSACTIONACCOUNTID,
         t.BRANCHCODE,
         t.CONTRACTNO,
         t.SNO,
         t.RETURNMSG,
         t.RETURNCODE,
         t.TASERIALNO,
         t.TASERIALNOTOTAL,
         t.APPSHEETSERIALNO,
         t.ORIGINALTASERIALNO,
         t.ORIGINALAPPSHEETSERIALNO,
         t.ORIGINALBUSINESSCODE,
         t.SHAREREGISTERDATE,
         t.ORIGINALCONFIRMEDVOL,
         t.BUSINESSCODE,
         t.TRANSACTIONCFMDATE,
         t.SHARETYPE,
         t.NAV,
         t.CONFIRMEDVOL,
         t.CONFIRMEDAMOUNT,
         t.LAST_FUNDVOL,
         t.LAST_ABNMFROZEN,
         t.AFTER_FUNDVOL,
         t.AFTER_ABNMFROZEN,
         t.FUNDVOL,
         t.FROZEN,
         t.ABNMFROZEN,
         t.HFROZEN,
         t.UNFOOTINCOME,
         t.UNFOOTFROZENINCOME,
         t.FOOTINCOME,
         t.UNCOME,
         t.MINUSFUNDVOL,
         t.LEFTVOL,
         t.INSERTDATE,
         t.DEFDIVIDENDMETHOD,
         t.BASISFORCALCULATINGDIVIDEND,
         t.DIVIDENDPERUNIT,
         t.DIVIDENDAMOUNT,
         t.DIVIDENDINDEED,
         t.REGISTRATIONDATE,
         t.UNFROZENFLAG,
         t.TRANSACTIONDATE,
         t.TRANSACTIONTIME,
         t.UNCOMEDIVIDENDSHARE,
         t.FROZENDIVIDENDSHARE,
         t.FROZENCAUSE,
         t.FROZENDEADLINE,
         t.ACCOUNTFROZENFLAG,
         t.FROMTAFLAG,
         t.UNFROZENDATE,
         t.UNFROZENTIME,
         t.SHAREVALIDDATE,
         t.IPORATE,
         t.TRADEPRICE,
         t.TRADEAPPID,
         t.MATCHEDID,
         t.BEGINDATE);
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
  end cj_ods_ta_bal_detail_chg;

  --采集当日失效份额变动
  procedure cj_ods_update_validdate(errcode out int, errmsg out varchar2) is

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

    for p in (select t.datasource,t.taserialno,t.sharevaliddate
      from t_ta_bal_detail_chg t) loop
      update ods_ta_bal_detail_chg t set t.sharevaliddate = p.sharevaliddate
       where t.datasource = p.datasource and t.taserialno = p.taserialno;
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
  end cj_ods_update_validdate;

  --采集自研TA份额汇总表每日变动信息
  procedure cj_ods_ta_bal_fund_chg(errcode out int, errmsg out varchar2) is

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
    merge into ODS_TA_BAL_FUND_CHG t1
    using  T_TA_BAL_FUND_CHG  t
    on ( t1.BAL_DETAIL_ID = t.BAL_DETAIL_ID)
    when not matched then
      insert
        ( t1.BAL_DETAIL_ID,
          t1.MON,
          t1.BEGINDT,
          t1.ENDDT,
          t1.TAACCOUNTID,
          t1.DISTRIBUTORCODE,
          t1.TRANSACTIONACCOUNTID,
          t1.FUNDCODE,
          t1.TRANSDT,
          t1.BALANCE,
          t1.AVAILABLE,
          t1.FROZEN,
          t1.HFROZEN,
          t1.ABNMFROZEN,
          t1.FORBIDREDEEM,
          t1.T_HOLDDATE ,
          t1.TANO,
          t1.INCS,
          t1.BROKER ,
          t1.CUSTTAGID,
          t1.CUSTID,
          t1.PERFHISID ,
          t1.PERFXID ,
          t1.CITYHISID ,
          t1.IMCITYID,
          t1.CUSTNOFHIS_ID ,
          t1.CIXID ,
          t1.CIX_FDAC_ID ,
          t1.BALANCE0 ,
          t1.INCOME0,
          t1.CIX_CF_ID ,
          t1.PERF_SEATNO ,
          t1.CFID ,
          t1.MAX_SEQ ,
          t1.MODIFYSEQ ,
          t1.INVTP ,
          t1.STAR_LEVEL,
          t1.DW_ETL_DT ,
          t1.DW_INS_DT,
          t1.DW_INS_TM )
      values
        ( t.BAL_DETAIL_ID,
          t.MON,
          to_char(cast(cast(t.BEGINDT as timestamp) as date),'yyyymmdd'),
          to_char(cast(cast(t.ENDDT as timestamp) as date), 'yyyymmdd'),
          t.TAACCOUNTID,
          t.DISTRIBUTORCODE,
          t.TRANSACTIONACCOUNTID,
          t.FUNDCODE,
          t.TRANSDT,
          t.BALANCE,
          t.AVAILABLE,
          t.FROZEN,
          t.HFROZEN,
          t.ABNMFROZEN,
          t.FORBIDREDEEM,
          t.T_HOLDDATE ,
          t.TANO,
          t.INCS,
          t.BROKER ,
          t.CUSTTAGID,
          t.CUSTID,
          t.PERFHISID ,
          t.PERFXID ,
          t.CITYHISID ,
          t.IMCITYID,
          t.CUSTNOFHIS_ID ,
          t.CIXID ,
          t.CIX_FDAC_ID ,
          t.BALANCE0 ,
          t.INCOME0,
          t.CIX_CF_ID ,
          t.PERF_SEATNO ,
          t.CFID ,
          t.MAX_SEQ ,
          t.MODIFYSEQ ,
          t.INVTP ,
          t.STAR_LEVEL,
          t.DW_ETL_DT ,
          t.DW_INS_DT,
          t.DW_INS_TM);
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
  end cj_ods_ta_bal_fund_chg;
end pkg_etl_data_ta;
/
