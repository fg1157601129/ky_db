import os
import xlwt
import json
import zlib
import logging
import pymysql
import psycopg2
import yaml
from get_path import get_base_path

logger = logging.getLogger(__name__)


def switch_gzip(content):
    # content_str = bytes(content).decode().strip(b'\x00'.decode())
    content_str = zlib.decompress(bytes(content).lstrip(b'GZIP')).decode().strip(b'\x00'.decode())
    return content_str


def get_db_config(db):
    """

    :param : db mysql pg type string
    :return: db config type dict {}
    """

    file = os.path.join(get_base_path(), 'dbconfig.yaml')
    try:
        with open(file, 'r', encoding='utf-8') as f:
            data = yaml.load(f.read(), Loader=yaml.Loader)
    except Exception as e:
        print(f"文件：{file}无法打开，报错信息：{e}")

    if db.upper() == 'MYSQL':
        mysql_config = data.get('mysql', {})
        return mysql_config
    elif db.upper() == 'PG':
        pg_config = data.get('postgresql', {})
        return pg_config
    else:
        print(f"您输入的数据库：{db}不存在,请输入要查询的数据库配置如mysql或者pg")
        return {}


def get_db_connection(db):
    """

    :param db_config:
    :return:
    """
    db_config = get_db_config(db)
    print(f"当前数据库配置{db_config}")
    if not db_config:
        print(f"数据库配置为空{db_config}，请检查数据库配置，谢谢")
    elif db.upper() == 'MYSQL':
        conn = pymysql.connect(
            host=db_config.get('host', ''),
            port=db_config.get('port', ''),
            user=db_config.get('user', ''),
            passwd=db_config.get('passwd', ''),
            db=db_config.get('database', ''),
            charset='utf8mb4'
            # use_unicode=True
        )
        print("mysql db conn")
        return conn
    else:
        conn = psycopg2.connect(
            host=db_config.get('host', ''),
            port=db_config.get('port', ''),
            user=db_config.get('user', ''),
            password=db_config.get('passwd', ''),
            database=db_config.get('database', '')
        )
        return conn


def mysql_execute_query(sql, args=None):
    """

    :param sql:
    :param param:
    :return:
    """
    try:
        conn = get_db_connection('mysql')

        cursor = conn.cursor()

        if args is None:
            row_count = cursor.execute(sql)
            # print(row_count)
        else:
            row_count = cursor.execute(sql, args)

        rows = None
        # if row_count > 0:
        #     rows = cursor.fetchall()
        #     fields = cursor.description

        rows = cursor.fetchall()
        fields = cursor.description
        # conn.commit()
        logger.info(f"successfully run query {sql} with param {args}, row_count is {row_count} ")
        cursor.close()
        conn.close()
        return rows, fields
    except Exception:
        logging.exception(f"caught an exceptin during execute query {sql} with {args}")
        conn.rollback()
        cursor.close()
        conn.close()
        return None


def pg_execute_query(sql, param=None):
    """

    :param sql:
    :param param:
    :return:
    """
    conn = get_db_connection('pg')
    cursor = conn.cursor()
    try:


        if param is None:
            row_count = cursor.execute(sql)
            print(row_count)
        else:
            row_count = cursor.execute(sql, param)

        rows = cursor.fetchall()
        fields = cursor.description
        # conn.commit()
        logger.info(f"successfully run query {sql} with param {param}, row_count is {row_count} ")
        cursor.close()
        conn.close()
        return rows, fields
    except Exception:
        logging.exception(f"caught an exceptin during execute query {sql} with {param}")
        conn.rollback()
        cursor.close()
        conn.close()
        return None


def print_pg_data(sql):
    data, field = pg_execute_query(sql)
    # 打印 field
    for f in field:
        print(f[0], '\t', end=" ")
    print(" ")
    # 打印 content
    for d in data:

        ln = len(d)
        for i in range(ln):
            if isinstance(d[i], memoryview):
                # 解压缩数据
                kind = str(d[i].tobytes())
                # print(kind)
                # print(type(kind))
                if kind.startswith("b'GZIP"):
                    tmp = zlib.decompress(bytes(d[i]).lstrip(b'GZIP'))
                    rst = json.loads(tmp)
                    print(rst, '\t', end=" ")
                else:
                    print(d[i].tobytes(), '\t', end=" ")
                # 不解压缩
                # print(d[i].tobytes(), '\t', end=" ")
            else:
                print(d[i], '\t', end=" ")
        print(" ")


def print_mysql_data(sql):
    data, field = mysql_execute_query(sql)
    # 打印 field
    for f in field:
        print(f[0], '\t', end=" ")
    print(" ")
    # 打印 content
    for d in data:
        for e in d:
            if isinstance(e, bytes):
                # 解压缩数据
                kind = str(e)
                # print(kind)
                # print(type(kind))
                if kind.startswith("b'GZIP"):
                    unzip_e = switch_gzip(e)
                    print(unzip_e, '\t', end=" ")
                # 不解压缩
                else:
                    print(e, '\t', end=" ")
            else:
                print(e, '\t', end=" ")
        print(" ")


if __name__ == '__main__':
    # # pg
    # sql = "select * from ke_test_team_44x where META_TABLE_KEY like '/FG_Test_001/table/%'"
    # print_pg_data(sql)

    # mysql
    # 查询指定模型
    #sql = "select * from ke_test_team_44x where META_TABLE_KEY like '/FG_Test_001/table_exd/SSB.DATES.json';"
    sql = "select * from ke_test_team_44x where META_TABLE_KEY like '/FG_Test_001/table/SSB.DATES.json';"

    sql2 = "select * from ke_test_team_44x where META_TABLE_KEY like '/_global/project/FG_Test_001.json';"
    print_mysql_data(sql)