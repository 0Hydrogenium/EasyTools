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
dir_list = []
for type_name in ["sjgc", "xtkf", "ywyy", "zxsj"]:
    current_list = os.listdir("./corpus/{}".format(type_name))
    dir_list.extend([
        x + y for x, y in
        zip(["./corpus/{}/".format(type_name)]*len(current_list), current_list)
    ])

counter = 0
for investment_channel in ["省公司可控费用", "综合计划", "自筹费用"]:
    for current_dir in tqdm(dir_list):
        # 数据读取: 当前excel
        df = pd.read_excel(current_dir)

        # 数据预处理
        df.replace(np.nan, "", inplace=True)

        for i in range(len(df)):
            # 获取对应字段值
            tdmc_id = counter
            project_classify = re.search(r"corpus/(.*?)/", current_dir).group().replace("corpus", "").replace("/", "")
            table_name = re.search(r"/(.*?)$", current_dir).group().split("/")[-1].split(".")[0]

            factor_index = df.loc[i, "Index"] if "Index" in df.columns.values else ""
            factor_name = df.loc[i, "Factor"] if "Factor" in df.columns.values else ""
            factor_item = df.loc[i, "Item"] if "Item" in df.columns.values else ""
            one_location = df.loc[i, "Location1"] if "Location1" in df.columns.values else ""
            two_location = df.loc[i, "Location2"] if "Location2" in df.columns.values else ""
            three_location = df.loc[i, "Location3"] if "Location3" in df.columns.values else ""
            four_location = df.loc[i, "Location4"] if "Location4" in df.columns.values else ""
            five_location = df.loc[i, "Location5"] if "Location5" in df.columns.values else ""
            six_location = df.loc[i, "Location6"] if "Location6" in df.columns.values else ""
            class_id = df.loc[i, "Class_ID"] if "Class_ID" in df.columns.values else ""
            class_name = df.loc[i, "Class_Name"] if "Class_Name" in df.columns.values else ""
            scores_value = df.loc[i, "Value"] if "Value" in df.columns.values else ""

            corpus = df.loc[i, "Corpus"] if "Corpus" in df.columns.values else ""

            sql = """
                insert into t_digital_measure_corpus (
                    tdmc_id,
                    investment_channel,
                    project_classify,
                    table_name,
                    factor_index,
                    factor_name,
                    factor_item,
                    one_location,
                    two_location,
                    three_location,
                    four_location,
                    five_location,
                    six_location,
                    class_id,
                    class_name,
                    scores_value,
                    corpus
                ) values (
                    {},
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}',
                    '{}'
                )
            """.format(
                tdmc_id,
                investment_channel,
                project_classify,
                table_name,
                factor_index,
                factor_name,
                factor_item,
                one_location,
                two_location,
                three_location,
                four_location,
                five_location,
                six_location,
                class_id,
                class_name,
                scores_value,
                corpus
            )

            sql_driver.execute_write(sql)
            counter += 1

print("done")










