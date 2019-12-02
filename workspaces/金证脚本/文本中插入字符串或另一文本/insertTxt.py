import os

begin_begin = "  V_MSG       RDT_COLLECTING_LOG.MSG%TYPE; \n" \
    "    V_LOG_POINT RDT_COLLECTING_LOG.LOG_POINT%TYPE DEFAULT '001'; \n" \
    "    V_PROC_NAME RDT_COLLECTING_LOG.PROC_NAME%TYPE DEFAULT fn_who_am_i; \n" \
    "    V_LOG_FLAG  RDT_COLLECTING_LOG.LOG_FLAG%TYPE DEFAULT 'S'; \n" \
    "    V_LOADROW   RDT_COLLECTING_LOG.LOAD_ROW%TYPE DEFAULT 0; \n "

begin_after = "    --程序开始执行 \n" \
    "    V_LOG_POINT := '001'; \n" \
    "    V_MSG       := '程序开始执行'; \n" \
    "    DATA_COLLECTING_LOG(V_PROC_NAME, \n" \
    "                        V_LOG_POINT, \n" \
    "                        V_MSG, \n" \
    "                        V_LOG_FLAG, \n" \
    "                        V_LOADROW);\n"

commit_begin = "	--统计行数\n" \
    "	V_LOADROW := V_LOADROW + SQL%ROWCOUNT;\n"

ok_after = "    --程序处理完成\n" \
    "	V_LOG_POINT := '999';\n" \
    "	V_MSG       := '程序处理完成';\n" \
    "	DATA_COLLECTING_LOG(V_PROC_NAME,\n" \
    "	                    V_LOG_POINT,\n" \
    "	                    V_MSG,\n" \
    " 	                    V_LOG_FLAG,\n" \
    " 	                    V_LOADROW);\n"


WHENOTHERSTHEN_after = "      --CONSIDER LOGGING THE ERROR AND THEN RE-RAISE\n" \
      "      V_MSG      := '发生错误' || SQLERRM;\n" \
      "      V_LOG_FLAG := 'E';\n" \
      "      DATA_COLLECTING_LOG(V_PROC_NAME,\n" \
      "                          V_LOG_POINT,\n" \
      "                          V_MSG,\n" \
      "                          V_LOG_FLAG,\n" \
      "                          V_LOADROW);\n"

filepath = "pkg_etl_data_ta.bdy"
content = []
with open(filepath,'r') as f:
    for line in f:
        begin = line.find("begin\n")
        if begin != -1:
            line = line[:begin] + begin_begin + "   "+line[begin:] + begin_after

        commit = line.find("commit;\n")
        if commit != -1:
                line = line[:commit] + commit_begin + "    "+line[commit:]

        exception = line.lower().find("exception\n")
        if exception != -1:
            line = line[:exception] + ok_after + "    "+line[exception:]

        otherthen = line.lower().find("when others then\n")
        if otherthen != -1:
            line = "    "+line[:otherthen] + line[otherthen:] + WHENOTHERSTHEN_after

        content.append(line)

    s = ''.join(content)
    f = open(filepath, 'w+')  # 重新写入文件
    f.write(s)
    f.close()
    del content[:]  # 清空列表
    print(content)

