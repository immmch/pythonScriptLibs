import sys
import pymongo
import urllib.parse
import copy

'''
标题: vwa 全量车导出工程的性能测试，用于灌入大量测试数据

用法：
    1. 灌入数据输入两个参数： 起始数，终止数,示例如下： 
        python3 TUI-batchInsertData 0   1000    插入1000条数据
        python3 TUI-batchInsertData 5000   9000  插入4000条数据
        说明： 相同的数值生成的vin相同；
    2. 删除输入一个参数： [d|del|delete]，示例如下： 
        python3 TUI-batchInsertData delete
        python3 TUI-batchInsertData d
        python3 TUI-batchInsertData del

前提：
    1. 当前华为云mongodb版本号3.5，需要安装对应pymongo版本号：3.12.2
        pip install pymongo==3.12.3
        注意：mongo批量插入数据时，会回填ObjectId。导致dup key error！
    2. delete {"otherFields.modelName":"RCH_全量车测试数据"}
 
'''

password = urllib.parse.quote_plus('yxmAZCcmLPgxoIhv@Nn1')

batchStoreObj = []
client = pymongo.MongoClient('mongodb://rwuser:%s@10.19.12.44:8635,'
                             '10.19.12.181:8635/locationtest?replicaSet=replica' % password,
                             tls=True,
                             tlsCAFile='/home/ubuntu/renchenhao/pymongo/ca_mongo.crt',
                             authSource='admin',
                             tlsAllowInvalidCertificates=True)
db = client.location
collection = db.newest_snapshot_vwa


def insertData():
    # 准备批量插入的数据模版
    data = collection.find_one({"allUploadData.data": {'$exists': True}, "brandCode": "VWA"})
    del data['_id']
    vin = data['vin']
    data['otherFields']['modelName'] = 'RCH_全量车测试数据'
    truncated_vin = vin[0:-8]
    count = int(sys.argv[2]) - int(sys.argv[1])
    for i in range(int(sys.argv[1]), int(sys.argv[2])):
        data['vin'] = truncated_vin + str(i).zfill(8)
        dataClone = copy.deepcopy(data)
        batchStoreObj.append(dataClone)
        if len(batchStoreObj)%1000 == 0:
            # print("before insert_many batchStoreObj:" + str(batchStoreObj))
            collection.insert_many(batchStoreObj)
            # print("after insert_many batchStoreObj:" + str(batchStoreObj))
            batchStoreObj.clear()
            print("已插入文档数：" + str(i+1 - int(sys.argv[1])))

    print("测试数据灌入成功，共：" + str(count) + "条！")


def insertOrDel():
    if sys.argv[1] == 'd' or sys.argv[1] == 'delete' or sys.argv[1] == 'del':
        data = collection.delete_many({"otherFields.modelName": "RCH_全量车测试数据"})
        print(data.deleted_count, "个文档已删除!")
        print("测试数据清理完成！")
    elif len(sys.argv) == 3:
        insertData()
    else:
        print("非法输入，添加参数！")


if __name__ == '__main__':
    insertOrDel()
