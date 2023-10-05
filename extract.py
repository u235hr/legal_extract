import os
import spacy
import psycopg2
import jieba.analyse
from ltp import LTP, StnSplit
from PyPDF2 import PdfReader

def extract_text_from_pdf(pdf_path):
    pdf = PdfReader(pdf_path)
    text = ''
    for page in pdf.pages:
        text += page.extract_text()
    return text

def extract(text):
    # 使用 Spacy 进行句法分析
    nlp = spacy.load("zh_core_web_sm")
    doc = nlp(text)
    sentences = []
    for sent in doc.sents:
        sentences.append(sent.text)

    # 使用 ltp 进行句子分割
    sents_list = StnSplit().split(text)

    # 使用 jieba 进行关键词提取
    keywords = jieba.analyse.extract_tags(' '.join(sents_list), topK=20)

    # 将提取的关键信息写入数据库
    conn = psycopg2.connect(host="localhost", port=24052, user="postgres", password="hurao415", dbname="legal_corpus")
    cursor = conn.cursor()

    cursor.execute("SELECT to_regclass('public.info')")
    if cursor.fetchone()[0] is None:
        cursor.execute("CREATE TABLE info (id SERIAL PRIMARY KEY, case_type VARCHAR(255), case_no VARCHAR(255), court VARCHAR(255), judge VARCHAR(255), date DATE, plaintiff VARCHAR(255), defendant VARCHAR(255), third_party VARCHAR(255), fact VARCHAR(255), law VARCHAR(255), result VARCHAR(255))")

    for keyword in keywords:
        cursor.execute("INSERT INTO info (case_type) VALUES (%s)", (keyword,))
    conn.commit()
    cursor.close()
    conn.close()

    return sentences, keywords

if __name__ == "__main__":
    pdf_path = "C:\判决书\赵小松、向利上诉李立华、唐大轩民间借贷纠纷二审判决书.pdf"
    text = extract_text_from_pdf(pdf_path)
    extract(text)
