import pdfplumber
import re
import numpy as np
import pandas as pd
import jieba
import ltp


def extract_core_content(pdf_path):
    """
    提取判决书核心内容

    Args:
        pdf_path: 判决书 PDF 文件路径

    Returns:
        提取的核心内容
    """

    # 读取 PDF 文档
    with pdfplumber.open(pdf_path) as pdf:
        pages = pdf.pages

    # 提取事件
    events = []
    for page in pages:
        for text in page.extract_text().split("\n"):
            # 使用正则表达式匹配事件
            matches = re.findall(r"(.*)[\s]+(发生|发生了|发生于)(.*)", text)
            if matches:
                events.append(matches[0])

    # 使用最大正向匹配法提取事件要素
    events = [
        [
            re.findall(r"[\s]+(.*)[\s]+", event)[0],
            re.findall(r"[\s]+(.*)[\s]+", event)[1],
            re.findall(r"[\s]+(.*)[\s]+", event)[2],
        ]
        for event in events
    ]

    # 使用分词工具提取事件要素
    events = [
        [
            ltp.segmentor.segment(event[0]),
            ltp.segmentor.segment(event[1]),
            ltp.segmentor.segment(event[2]),
        ]
        for event in events
    ]

    # 使用命名实体识别工具提取事件要素
    events = [
        [
            ltp.ner.ner(event[0]),
            ltp.ner.ner(event[1]),
            ltp.ner.ner(event[2]),
        ]
        for event in events
    ]

    # 将事件要素拼接成字符串
    events = [
        "{0[0]} {0[1]} {0[2]}".format(event)
        for event in events
    ]

    return events


def write_to_database(events, database_path):
    """
    将提取的核心内容写入数据库

    Args:
        events: 提取的核心内容
        database_path: 数据库路径
    """

    # 连接数据库
    connection = connect_to_database(database_path)
    cursor = connection.cursor()

    # 查询判决号是否存在
    cursor.execute(
        """
        SELECT COUNT(*) FROM events WHERE number = %s;
        """,
        (events[0],),
    )
    count = cursor.fetchone()[0]

    # 如果判决号存在，则不重复写入
    if count > 0:
        return

    # 写入数据
    cursor.execute(
        """
        INSERT INTO events (number, content) VALUES (%s, %s);
        """,
        (events[0], events[1]),
    )

    # 提交事务
    connection.commit()

    # 关闭连接
    cursor.close()
    connection.close()


def main():
    pdf_path = "判决书.pdf"
    events = extract_core_content(pdf_path)
    print(events)

    # 将提取的核心内容写入数据库
    database_path = "database.db"
    write_to_database(events, database_path)


if __name__ == "__main__":
    main()
