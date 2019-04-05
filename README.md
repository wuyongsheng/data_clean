
> 从去年开始就一直在参与公司爬虫和大数据项目的测试，测试的对象主要是数据：测试爬虫爬取的数据的准确性、完整性、一致性、惟一性、适时性等，使用的测试方法有很多，这篇文章记录了我通过Python脚本对MongoDB数据库中的异常数据进行识别的一些操作，自认为写的还算简单高效

###  背景介绍
对于已经入库的数据（比如 MongoDB、ElasticSearch中的数据），为了验证数据的正确性，我们可以借助一些数据库的客户端工具(比如：MongoDB 的 NoSQLBooster，ElasticSearch 的 Kibana)，通过手工查询是否存在异常数据，但通常数据库中会有很多表、每个表有很多个字段，每个字段都要从多个维度（比如是否为空，字段是否缺失，是否存在异常字符等）进行验证，工作量会很大

对于那些普遍的、基本的验证操作，我们可以通过 Python 操作对应的数据库，通过脚本的方式进行验证，这样可以大大加快测试的速度

###  功能介绍
本文通过 Python 代码，对 MongoDB 数据库中的数据进行了以下几方面的验证：

- 重复数据的过滤
- 包含异常字符的数据的过滤
- 包含字段缺失的数据的过滤
- 包含多余字段的数据的过滤
- 非空字段为空的数据的过滤

### 测试脚本
在对数据进行测试之前，我们首先要了解的就是这些数据的表结构是怎样的：有哪些字段、字段的约束等，一般都会有相应的数据库设计文档进行说明，比如，本次我们要测试的数据的表结构如下：

列名 | 数据类型 | 是否空  | 备注
---|---|---|---|---
_id | Objectid | No | 主键
uniformSocialCreditCode | String | No |   统一社会信用代码
enterpriseName| String | No |企业名称 
legalRepresentative| String | Yes | 法定代表人
provinceCode| String | No | 6位省份代码

下面是测试的脚本，在脚本中，我进行了详尽的注释

```
'''导入MongoDB的驱动：pymongo '''
import pymongo
'''mylist 列表是我造的测试数据，将其插入到 MongoDB 数据库中'''
mylist = [
{"_id": 20, "uniformSocialCreditCode": "420001007161050020", "enterpriseName": "某科技公司01","legalRepresentative": "张三01","provinceCode":"420001","createDate":"20190320"},
{"_id": 21, "uniformSocialCreditCode": "420001007161050020", "enterpriseName": "某科技公司01","legalRepresentative": ""      ,"provinceCode":"420002","createDate":"20190321"},
{"_id": 22, "uniformSocialCreditCode": "420001007161050022", "enterpriseName": "某科技公司03","legalRepresentative": "张三03","provinceCode":"420003","createDate":"20190322"},
{"_id": 23, "uniformSocialCreditCode": "420001007161050022", "enterpriseName": "某科技公司03","legalRepresentative": ""      ,"provinceCode":""      ,"createDate":"20190323"},
{"_id": 24, "uniformSocialCreditCode": "4200010\t\r\n   24"                                  ,"legalRepresentative": "张三03","provinceCode":"420005","createDate":"20190324"},
{"_id": 25, "uniformSocialCreditCode": "420             25", "enterpriseName": "某科技公司06","legalRepresentative": ""       ,"provinceCode":"420006","createDate":"20190325"},
{"_id": 26, "uniformSocialCreditCode": "420001007161050026", "enterpriseName": "",            "legalRepresentative": ""      ,"provinceCode":"420007","createDate":"20190326"},
{"_id": 27,                                                  "enterpriseName": "某科技公司06",                               "provinceCode":"420008","createDate":"20190327"},
{"_id": 28,                                                  "enterpriseName": "某科技公司09","legalRepresentative": "张三09","provinceCode":"420009","createDate":"20190328"},
{"_id": 29, "uniformSocialCreditCode": "",                   "enterpriseName": "某科技公司19","legalRepresentative": ""      ,"provinceCode":"420\n ","createDate":"20190329"},
{"_id": 30, "uniformSocialCreditCode": "",                   "enterpriseName": "某科<html112","legalRepresentative": "张三11","provinceCode":"420011","createDate":"20190330"},
{"_id": 31, "uniformSocialCreditCode": "420001007161050031", "enterpriseName": "某科技公司13"             ,"legalRepresentative": "张三12","provinceCode":"420012","createDate":"20190331"},
{"_id": 32, "uniformSocialCreditCode": "420001007161050032", "enterpriseName": "某科技公司13","legalRepresentative": "<html>23","provinceCode":"420013","createDate":"20190401"},
{"_id": 33, "uniformSocialCreditCode": "420001007161050033", "enterpriseName": "某科技公司14","legalRepresentative": "张三14","provinceCode":"420014","createDate":"20190402"},
]

'''check()方法用来对数据库设计文档中所有的字段是否缺失、是否存在特殊字符以及非空字段是否为空进行检查'''
def check(full_list, not_null_list, special_list):
    '''for循环依次遍历参数列表中所有的字段'''
    for j in full_list:
        '''查询缺失的字段'''
        qurey_no_exists = {
            j: {
                "$exists": False
            }
        }
        '''查询字段缺失的记录，并返回缺失该字段的记录数量'''
        count1 = mycol.find(qurey_no_exists).count() 
        '''如果存在字段缺失的记录，则打印缺失该字段的记录的数量及部分记录'''
        if (count1 > 0):
            print("对比数据库设计文档，存在 " + j + " 字段缺失的记录，" + " 缺失的记录数为：" + str(count1))
            print("其中一条 "+j+" 字段缺失的记录为：")
            '''打印一条该字段缺失的记录，通过limit()函数中的参数，可控制打印的记录的数量'''
            for m in mycol.find(qurey_no_exists).limit(1):
                print(m)
        '''对 MongoDB 中的所有记录的该字段，遍历是否存在特殊字符列表中的异常字符'''
        for p in special_list:
            qurey_special = {
                j: {
                    "$regex": p
                }
            }
            '''查询包含异常字符的记录，并返回记录的数量'''
            count2 = mycol.find(qurey_special).count() 
            '''如果存在包含异常字符的记录，则打印记录的数量及部分记录'''
            if (count2 > 0):
                print("字段： " + j + " 存在异常字符：" + p + "， 记录数为： " + str(count2))
                print("其中的一条包含异常字符 " + p + " 的记录为：")
                '''打印一条包含异常字符的记录，通过limit()函数中的参数，可控制打印的记录的数量'''
                for k in mycol.find(qurey_special).limit(1):
                    print(k)
        '''对数据库设计文档中的非空字段进行判空'''
        if (j in not_null_list):
            qurey_null = {
                "$or": [{
                    j: ""
                }, {
                    j: []
                }]
            }
            '''如果存在非空字段为空的记录，则打印记录的数量及部分记录'''
            count3 = mycol.find(qurey_null).count()
            if (count3 > 0):
                print("数据库设计中 "+j+" 字段非空，"+"存在 " + j + " 字段为空的记录， " + " 为空的记录数为：" + str(count3))
                print("其中一条字段为空的记录为：")
                '''打印一条非空字段为空的记录，通过limit()函数中的参数，可控制打印的记录的数量'''
                for g in mycol.find(qurey_null).limit(1):
                    print(g)

'''check_duplicates()方法用来检查是否存在重复的数据，若MongoDB中，某个字段或某几个字段的组合不能重复，
        则将该字段或字段的组合以列表的方式传入到方法的参数 duplicates_list 中'''
def check_duplicates(duplicates_list): 
    
    '''下面的查询语句用到了MongoDB 的聚合 '''
    duplicates_query = [{
        "$group": {
#           聚合的分组初始为空
            "_id": {
            },
            "uniqueIds": {
                "$addToSet": "$_id"
            },
            
#         计算分组中记录的数量之和
            "count": {
                "$sum": 1
            }
        }
    }, {
#         匹配记录数量之和大于1的分组
        "$match": {
            "count": {
                "$gt": 1
            }
        }
    }]
    
    '''未传入分组的字段'''
    if(len(duplicates_list) == 0):
        print("输入的参数长度有误 ！！！")
    else:
        '''将传入的字段添加到查询语句的分组中（"_id"所对应的值）'''
        for i in range(len(duplicates_list)):
            duplicates_query[0]["$group"]["_id"][duplicates_list[i]] = "$" + duplicates_list[i]
        cursor = mycol.aggregate(duplicates_query)
        '''打印存在重复数据的字段的组合'''
        try:
            for doc in cursor:
                print("字段  " + str(doc["_id"])+" 的组合存在重复数据")
        finally:
            cursor.close() 

'''check_redundant()方法用来检查 MongoDB 中的记录是否存在多于的字段
     （多于的字段指的是不包含在数据库设计文档中的字段）'''
def check_redundant():
    '''读取集合中的一条记录'''
    for i in mycol.find().limit(1):
        '''记录是字典形式的，获取字典的键值并转化为列表'''
        l = list(i.keys())
        print("MongoDB中的记录的字段为：" + "\n" + str(l))
        '''检查MongoDB中的字段是否有不包含在数据库设计文档中的字段'''
        for k in l:
            if (k not in full_list):
                print("对比数据库设计文档，MongoDb 中 " + k + " 字段多余")
                print("其中的一条 "+ k +" 字段多余的记录为： " + "\n" + str(i))
            if(k == ''):
                print("MongoDB 中存在名称为空的字段")
                
if __name__ == "__main__":
 print("--------" + "开始测试" + "--------")
 try:
    '''指定数据库'''
    db_name  = "wys_db"
    '''指定集合'''
    col_name = "wys_col"
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient[db_name]
    mycol = mydb[col_name]
    '''异常字符列表，只列举了部分字符'''
    special_list = ['     ', '\t', '\r', '\n', '<html', '<meta', 'null']
    '''指定可以确定重复数据的字段组合，统一社会信用代码和企业名称的组合可唯一确定一个企业，选这两个字段'''
    duplicates_list = ["uniformSocialCreditCode","enterpriseName"]
    '''full_list 用来放数据库设计文档中的所有字段 '''
    full_list = ["_id","uniformSocialCreditCode","enterpriseName","legalRepresentative","provinceCode"]
    '''not_null_list 用来放数据库设计文档中的非空字段'''
    not_null_list = ["_id","uniformSocialCreditCode","enterpriseName","provinceCode"]
    '''将测试数据插入到  MongoDB 中'''
    mycol.insert_many(mylist)
    '''检查是否存在重复数据'''
    check_duplicates(duplicates_list)
    '''检查是否存在字段缺失、包含特殊字符、非空字段为空的数据'''
    check(full_list, not_null_list, special_list)
    '''检查 MongoDB 中是否有字段多于'''
    check_redundant()
 finally:
    '''测试执行完成之后，将插入的测试数据删除，以免第二次运行时插入重复的数据报错'''
    myclient.drop_database("wys_db")
    print("--------" + "完成测试" + "--------")
    
```
### 测试运行的结脚本
脚本执行后，会向MongoDB数据库中插入数据，如图所示：

![MongoDB查询.png](https://upload-images.jianshu.io/upload_images/12273007-eb3e972eeaa1293b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



测试运行的结果如下：

```
--------开始测试--------
字段  {'uniformSocialCreditCode': '420001007161050022', 'enterpriseName': '某科技公司03'} 的组合存在重复数据
字段  {'uniformSocialCreditCode': '420001007161050020', 'enterpriseName': '某科技公司01'} 的组合存在重复数据
对比数据库设计文档，存在 uniformSocialCreditCode 字段缺失的记录， 缺失的记录数为：2
其中一条 uniformSocialCreditCode 字段缺失的记录为：
{'_id': 27, 'enterpriseName': '某科技公司06', 'provinceCode': '420008', 'createDate': '20190327'}
字段： uniformSocialCreditCode 存在异常字符：     ， 记录数为： 1
其中的一条包含异常字符       的记录为：
{'_id': 25, 'uniformSocialCreditCode': '420             25', 'enterpriseName': '某科技公司06', 'legalRepresentative': '', 'provinceCode': '420006', 'createDate': '20190325'}
字段： uniformSocialCreditCode 存在异常字符：	， 记录数为： 1
其中的一条包含异常字符 	 的记录为：
{'_id': 24, 'uniformSocialCreditCode': '4200010\t\r\n   24', 'legalRepresentative': '张三03', 'provinceCode': '420005', 'createDate': '20190324'}
字段： uniformSocialCreditCode 存在异常字符：
， 记录数为： 1
其中的一条包含异常字符 
 的记录为：
{'_id': 24, 'uniformSocialCreditCode': '4200010\t\r\n   24', 'legalRepresentative': '张三03', 'provinceCode': '420005', 'createDate': '20190324'}
字段： uniformSocialCreditCode 存在异常字符：
， 记录数为： 1
其中的一条包含异常字符 
 的记录为：
{'_id': 24, 'uniformSocialCreditCode': '4200010\t\r\n   24', 'legalRepresentative': '张三03', 'provinceCode': '420005', 'createDate': '20190324'}
数据库设计中 uniformSocialCreditCode 字段非空，存在 uniformSocialCreditCode 字段为空的记录，  为空的记录数为：2
其中一条字段为空的记录为：
{'_id': 29, 'uniformSocialCreditCode': '', 'enterpriseName': '某科技公司19', 'legalRepresentative': '', 'provinceCode': '420\n ', 'createDate': '20190329'}
对比数据库设计文档，存在 enterpriseName 字段缺失的记录， 缺失的记录数为：1
其中一条 enterpriseName 字段缺失的记录为：
{'_id': 24, 'uniformSocialCreditCode': '4200010\t\r\n   24', 'legalRepresentative': '张三03', 'provinceCode': '420005', 'createDate': '20190324'}
字段： enterpriseName 存在异常字符：<html， 记录数为： 1
其中的一条包含异常字符 <html 的记录为：
{'_id': 30, 'uniformSocialCreditCode': '', 'enterpriseName': '某科<html112', 'legalRepresentative': '张三11', 'provinceCode': '420011', 'createDate': '20190330'}
数据库设计中 enterpriseName 字段非空，存在 enterpriseName 字段为空的记录，  为空的记录数为：1
其中一条字段为空的记录为：
{'_id': 26, 'uniformSocialCreditCode': '420001007161050026', 'enterpriseName': '', 'legalRepresentative': '', 'provinceCode': '420007', 'createDate': '20190326'}
对比数据库设计文档，存在 legalRepresentative 字段缺失的记录， 缺失的记录数为：1
其中一条 legalRepresentative 字段缺失的记录为：
{'_id': 27, 'enterpriseName': '某科技公司06', 'provinceCode': '420008', 'createDate': '20190327'}
字段： legalRepresentative 存在异常字符：<html， 记录数为： 1
其中的一条包含异常字符 <html 的记录为：
{'_id': 32, 'uniformSocialCreditCode': '420001007161050032', 'enterpriseName': '某科技公司13', 'legalRepresentative': '<html>23', 'provinceCode': '420013', 'createDate': '20190401'}
字段： provinceCode 存在异常字符：
， 记录数为： 1
其中的一条包含异常字符 
 的记录为：
{'_id': 29, 'uniformSocialCreditCode': '', 'enterpriseName': '某科技公司19', 'legalRepresentative': '', 'provinceCode': '420\n ', 'createDate': '20190329'}
数据库设计中 provinceCode 字段非空，存在 provinceCode 字段为空的记录，  为空的记录数为：1
其中一条字段为空的记录为：
{'_id': 23, 'uniformSocialCreditCode': '420001007161050022', 'enterpriseName': '某科技公司03', 'legalRepresentative': '', 'provinceCode': '', 'createDate': '20190323'}
MongoDB中的记录的字段为：
['_id', 'uniformSocialCreditCode', 'enterpriseName', 'legalRepresentative', 'provinceCode', 'createDate']
对比数据库设计文档，MongoDb 中 createDate 字段多余
其中的一条 createDate 字段多余的记录为： 
{'_id': 20, 'uniformSocialCreditCode': '420001007161050020', 'enterpriseName': '某科技公司01', 'legalRepresentative': '张三01', 'provinceCode': '420001', 'createDate': '20190320'}
--------完成测试--------
