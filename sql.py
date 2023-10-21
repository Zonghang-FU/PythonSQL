import sqlite3
import csv

# 将CSV文件导入到SQLite数据库
def import_csv_to_sqlite(file_name, table_name, cursor):
    with open(file_name, 'r') as f:
        # 读取列名
        columns = next(csv.reader(f))
        placeholders = ', '.join('?' for column in columns)

        # 创建表
        cursor.execute(f'CREATE TABLE {table_name} ({", ".join(columns)} text)')

        # 逐行读取并插入到数据库
        for row in csv.reader(f):
            cursor.execute(f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})', row)

# 创建内存中的数据库
conn = sqlite3.connect(':memory:')
cur = conn.cursor()

# 导入四个CSV文件到不同的表
files_to_tables = {
    'file1.csv': 'table1',
    'file2.csv': 'table2',
    'file3.csv': 'table3',
    'file4.csv': 'table4'
}

for file_name, table_name in files_to_tables.items():
    import_csv_to_sqlite(file_name, table_name, cur)

# 执行INNER JOIN查询（这里只是一个示例，你可以修改为你需要的查询）
query = '''
SELECT * FROM table1
INNER JOIN table2 ON table1.id = table2.id
INNER JOIN table3 ON table1.id = table3.id
INNER JOIN table4 ON table1.id = table4.id
'''  # 更改为你需要的连接条件
cur.execute(query)

# 将结果保存到新的CSV文件
with open('result.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    # 写入列名
    writer.writerow([desc[0] for desc in cur.description])
    # 写入查询结果
    writer.writerows(cur.fetchall())

conn.close()
