import os
import math
import re
import numpy as np
import pandas as pd
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

from SqlDriver import SqlDriver


# 实例化SQL驱动器
sql_driver = SqlDriver()

# 获取语料库的所有excel表相对路径数据
dir_list = [
    "./table_process/jcss_corpus.xlsx",
    "./table_process/xtkf_corpus.xlsx"
]

counter = 0
for current_dir in tqdm(dir_list):
    # 数据读取: 当前excel
    df = pd.read_excel(current_dir)

    # 数据预处理
    df.replace(np.nan, "", inplace=True)

    for j in range(len(df)):
        corpus_classes = df.loc[j, "class_Name"]
        if "," in corpus_classes:
            corpus_class_list = corpus_classes.split(",")
            delimiter = ","
        else:
            corpus_class_list = corpus_classes.split("、")
            delimiter = "、"

        for i in range(len(corpus_class_list)):
            # 获取对应字段值
            tdmc_id = counter
            doc_name = re.search(r"/(.*?)$", current_dir).group().split("/")[-1].split(".")[0]

            class_name = corpus_class_list[i]
            value = df.loc[j, "Value"] if "Value" in df.columns.values else ""
            index = df.loc[j, "Index"] if "Index" in df.columns.values else ""

            sql = """
                insert into t_digital_measure_corpus_class (
                    tdmc_id,
                    doc_name,
                    class_name,
                    class_value,
                    class_index,
                    class_delimiter
                ) values (
                    {},
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}'
                )
            """.format(
                tdmc_id,
                doc_name,
                class_name,
                value,
                index,
                delimiter
            )

            sql_driver.execute_write(sql)
            counter += 1

print("done")










