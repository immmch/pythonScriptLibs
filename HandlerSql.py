import time
from datetime import datetime
import re

# 处理 info文件
# (`id`,`createTime`,`updateTime`,`deviceID`,`deviceType`,`vin`,`sourceType`,`tenant`,`status`,`errorCode`,`errorMsg`,`extErrorMsg`,`sn`,`imei`)
# deviceID、imei、sn、vin、status
# `vehicle_lastly_register_info`(`id`,`vin`,`createTime`,`updateTime`,`deviceID`,`deviceType`,`sourceType`,`tenant`,`status`,`errorCode`,`errorMsg`,`extErrorMsg`,`imei`,`sn`) values
# `vehicle_register_info`(`id`,`createTime`,`updateTime`,`deviceID`,`deviceType`,`vin`,`sourceType`,`tenant`,`status`,`errorCode`,`errorMsg`,`extErrorMsg`,`sn`,`imei`) values

# lastly_info_map
lastly_info = {}
not_exist_lastly_info = {}
exist_lastly_info = {}
InsertSqlTemplate = 'insert into `vehicle_lastly_register_info`(`vin`,`createTime`,`updateTime`,`deviceID`,`deviceType`,`sourceType`,`tenant`,`status`,`errorCode`,`errorMsg`,`extErrorMsg`,`imei`,`sn`) values'
def handlerInfoSql():
    print("start handlerInfoSql")
    start_time = time.time()
    i = 0
    # 拼接行
    appendLine = ""
    # 上一行是否完整的行
    lastComL = True
    with open('info.sql', 'r', encoding='utf-8') as f, open('finalSql.sql', 'w', encoding='utf-8') as finalSql, open('InfoCanNotHandler.txt', 'w', encoding='utf-8') as infoCanNotHandler:
        for line in f:
            i += 1
            if i % 1000000 == 0:
                print(i)
            s1 = line.strip()
            # 非values值跳过
            if not s1 or s1.startswith(("insert into", "SET FOREIGN_KEY_CHECKS")):
                # print(f"{i}: {s1}")
                t = i
            else:
                # 有些数据一行不是完整的，需要拼接多行
                comL = s1.endswith('),') or s1.endswith(");")
                if not comL:
                    appendLine += s1  # 更简洁的写法
                    lastComL = comL
                else:
                    # 非完整的 SQL 行，需要拼接
                    if not lastComL:
                        appendLine += s1
                        s1 = appendLine
                        lastComL = True
                        print(f"{i}: append: {appendLine}")
                        # 中间变量要 reset
                        appendLine = ""
                    try:
                        # 开始解析逻辑
                        # 取消首尾空白字符
                        s1 = s1.strip('(),;')
                        array = s1.split(",")
                        if len(array) == 14:
                            for j in range(len(array)):
                                array[j] = array[j].strip("'")
                            deviceID = array[3]
                            vin = array[5]
                            tenant = array[7]
                            status = array[8]
                            sn = array[12]
                            imei = array[13]
                            if imei != 'null' and sn != 'null' and vin != 'null' and deviceID != 'null':
                                if imei and sn and vin and deviceID:
                                    key = deviceID + status + imei + sn + vin
                                    if key in lastly_info:
                                        exist_lastly_info[key] = array
                                    else:
                                        not_exist_lastly_info[key] = array
                        else:
                            # 错误数据手动处理
                            s3 = s1.strip("'")
                            array = re.split(r'\',\'', s3)
                            if len(array) == 14:
                                for j in range(len(array)):
                                    array[j] = array[j].strip("'")
                                deviceID = array[3]
                                vin = array[5]
                                status = array[8]
                                sn = array[12]
                                imei = array[13]
                                if imei != 'null' and sn != 'null' and vin != 'null' and deviceID != 'null':
                                    if imei and sn and vin and deviceID:
                                        key = deviceID + status + imei + sn + vin
                                        if key in lastly_info:
                                            exist_lastly_info[key] = array
                                        else:
                                            not_exist_lastly_info[key] = array
                            else:
                                infoCanNotHandler.writelines(line + "\n")
                    except Exception as e:
                        print(line, e, i)
                        break
            # if i > 1000000:
            #     break
        # 更新语句
        # for key in exist_lastly_info.keys():
        #     id = lastly_info[key]
        #     array = exist_lastly_info[key]
        #     createTime = int(array[1])
        #     updatedTime = datetime.fromtimestamp(createTime / 1000).strftime("%Y-%m-%d %H:%M:%S")  # [2]
        #     deviceID = array[3]
        #     vin = array[5]
        #     status = array[8]
        #     errorCode = array[9]
        #     errorMsg = array[10]
        #     extErrorMsg = array[11]
        #     sn = array[12]
        #     imei = array[13]
        #     afterFormated = (f"UPDATE vehicle_lastly_register_info SET updateTime = '{updatedTime}', deviceID = '{deviceID}',vin='{vin}',status='{status}',errorCode='{errorCode}',errorMsg='{errorMsg}',"
        #                      f" extErrorMsg='{extErrorMsg}',imei='{imei}',sn='{sn}' WHERE id = {id};\n")
        #     finalSql.writelines(afterFormated)
        # 插入语句
        for array in not_exist_lastly_info.values():
            createTime = int(array[1])
            updatedTime = datetime.fromtimestamp(createTime / 1000).strftime("%Y-%m-%d %H:%M:%S") # [2]
            deviceID = array[3]
            deviceType = array[4]
            vin = array[5]
            sourceType = array[6]
            # tenant = array[7]
            status = array[8]
            errorCode = array[9]
            errorMsg = array[10]
            extErrorMsg = array[11]
            sn = array[12]
            imei = array[13]
            # print(extErrorMsg)
            values = f"('{vin}','{createTime}','{updatedTime}','{deviceID}','{deviceType}','{sourceType}','','{status}','{errorCode}','{errorMsg}','{extErrorMsg}','{imei}','{sn}')"
            finalSql.writelines(InsertSqlTemplate + values + ';\n')
    end_time = time.time()
    print("end handlerInfoSql耗时: {:.2f}秒".format(end_time - start_time))


def handlerLastlySql():
    print("start handlerLastlySql")
    start_time = time.time()
    i = 0
    # 拼接行
    appendLine = ""
    # 上一行是否完整的行
    lastComL = True
    with open('lastly.sql', 'r', encoding='utf-8') as f,  open('LastlyCanNotHandler.txt', 'w', encoding='utf-8') as destFile:
        for line in f:
            i += 1
            # print(i)
            s1 = line.strip()
            # 非values值跳过
            if not s1 or s1.startswith(("insert into", "SET FOREIGN_KEY_CHECKS")):
                print(f"{i}: {s1}")
            else:
                # 有些数据一行不是完整的，需要拼接多行
                comL = s1.endswith('),') or s1.endswith(");")
                if not comL:
                    appendLine += s1  # 更简洁的写法
                    lastComL = comL
                else:
                    # 非完整的 SQL 行，需要拼接
                    if not lastComL:
                        appendLine += s1
                        s1 = appendLine
                        lastComL = True
                        print(f"{i}: {appendLine}")
                        # 中间变量要 reset
                        appendLine = ""
                    try:
                        # 开始解析逻辑
                        # 取消首尾空白字符
                        s1 = s1.strip('(),;')
                        array = s1.split(",")
                        if len(array) == 14:
                            for j in range(len(array)):
                                array[j] = array[j].strip("'")
                            vin = array[1]
                            id = array[0]
                            deviceID = array[4]
                            status = array[8]
                            imei = array[12]
                            sn = array[13]
                            key = deviceID+status+imei+sn+vin
                            lastly_info[key] = int(id)
                            # print(deviceID, vin, sn, status, imei)
                        else:
                            # 错误数据手动处理
                            destFile.writelines(line + "\n")
                    except Exception as e:
                        print(f"{i}: {line}", e)
                        break
    end_time = time.time()
    print("end handlerLastlySql,耗时: {:.2f}秒".format(end_time - start_time))

if __name__ == '__main__':
    handlerLastlySql()
    handlerInfoSql()