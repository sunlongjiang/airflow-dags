from pyspark.sql import SparkSession

import sys

"""

CREATE EXTERNAL TABLE hungry_studio.dws_block_blast_gp_block_action_round_di(
  `device_id` string COMMENT '设备id', 
  `app_id` string COMMENT '应用id', 
  `install_datetime` timestamp COMMENT '安装时间', 
  `os` string COMMENT '操作系统', 
  `ip` string COMMENT 'ip', 
  `country` string COMMENT '国家', 
  `city` string COMMENT '城市', 
  `distinct_id` string COMMENT '用户id', 
  `event_name` string COMMENT '事件名称', 
  min_event_timestamp  string COMMENT '轮开始时间戳 ', 
  min_event_time string COMMENT '轮开始时间',
  max_event_timestamp  string COMMENT '轮最后落块时间戳', 
  max_event_datetime string COMMENT '轮最后落块时间',

  `game_id` string COMMENT '游戏-局id' , 
  `game_type` string COMMENT '游戏模式（0无尽 2关卡 6土耳其方块 10谜题 99新手引导）', 
  `round_id` string COMMENT '局内累计轮数', 
  `travel_id` string COMMENT '期数', 
  `travel_lv` string COMMENT '该期关卡号', 
  `matrix` string COMMENT '当前牌面数据', 
  clear_screen_cnt bigint COMMENT '清屏次数', 
blast_cnt bigint comment '消除次数',
blast_row_col_cnt bigint comment '消除行列数',
is_combo_status_cnt bigint comment '在combo状态的步数',

common_block_cnt bigint comment '块基础得分',
round_score bigint comment '单轮得分',

time_diff_in_seconds double comment '单轮耗时',

matrix_complex_value string comment '轮初始排面复杂度',
rec_strategy string comment '轮初始理论出块策略',
rec_strategy_fact string comment '轮初始实际出块策略',
index_pos_list string COMMENT '轮内出块-位置顺序 左中右 012 中右左',


block_step_cnt bigint comment '步数',
game_combo_cnt bigint comment 'combo段数',
game_combo_step_cnt bigint comment 'combo 总长度',
game_combo_block_step_cnt bigint comment 'combo内消除步数',
multiple_block_step_cnt bigint comment '多消步数',
game_combo_block_step_cnt_rate double comment 'combo消除比例 一局，在combo内消除的次数/combo总长度',
game_combo_step_cnt_rate double comment 'combo浓度 一局，combo总长度/摆放块次数',
avg_game_combo_step_cnt double comment 'combo平均段长度 一局，combo总长度/combo段数',
block_step_cnt_rate double comment '消除比例 一局，消除次数/摆放块次数',
multiple_block_step_cnt_rate double comment '多消比例'        
)
COMMENT 'block blast ios 游戏-局-轮粒度轻度汇总数据'
  PARTITIONED by(dt string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://hungry-studio-data-warehouse/dws/block_blast/gp/block_action_round_di'
  TBLPROPERTIES (
  'classification'='parquet')

ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (time_action_in_seconds double COMMENT '落块-动作时间');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (time_think_in_seconds double COMMENT '落块-思考时间');

Modified by zhaodawei at 2024.07.31
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di CHANGE COLUMN round_score round_score bigint COMMENT '单轮得分【废弃】';
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (gain_score int COMMENT '本局截止到该块的累计得分');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (gain_score_per_done int COMMENT '单块得分');

20240826 zhaodawei  增加字段
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (clean_cnt int COMMENT '记录 单次落块得分');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (clean_cnt_avg int COMMENT '记录 单次落块得分');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (is_clean_screen int COMMENT '是否清屏（1=是，0=否）');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (weight_avg float COMMENT '放置后盘面权重');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (put_rate_avg float COMMENT '放置后贴边率');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (accumulate_clean_times int COMMENT '累计消除次数');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (accumulate_clean_cnt int COMMENT '累计消除行数');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (app_version string COMMENT 'app版本');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (clean_screen_cnt int COMMENT '清屏次数');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (is_final_round boolean COMMENT '此轮是不是最后一轮');


20241017 zhaodawei  增加字段
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (combo_cnt int COMMENT '第几次连击');

sunlongjiang

ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (revive_cnt int COMMENT '复活次数');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (color_list string COMMENT '颜色列表');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (is_valid int COMMENT '是否有效');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (is_solution int COMMENT '是否有解');


-- 20241210
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (game_way_type int COMMENT '方案对应的批次');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (user_waynum string COMMENT '用户方案号');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (active_ab_game_way_type int COMMENT '当前活跃ab实验批次');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (nowwayarr array<string> COMMENT '活跃ab加入者方案号');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (latest_now_way int COMMENT '活跃ab加入者方案号中最后一个');


ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (weight_list array<string> COMMENT '牌面复杂度列表');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (latest_weight string COMMENT '轮结束牌面复制度'); 


ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (session_id int COMMENT '每次冷启，截止下一次冷启');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (combo5_cnt string COMMENT '到combo5的次数');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (combo10_cnt string COMMENT '到combo10的次数');
ALTER TABLE hungry_studio.dws_block_blast_gp_block_action_round_di ADD COLUMNS (combo15_cnt string COMMENT '到combo15的次数');

"""
"""

"""
import json

import numpy as np
import json
import sys
import time
import copy
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
import itertools

block_type_mappings = {
    1: [
        [1]
    ],
    6: [
        [1, 1],
        [1, -1]
    ],
    27: [
        [1, -1],
        [1, 1]
    ],
    28: [
        [-1, 1],
        [1, 1]
    ],
    15: [
        [1, 1],
        [-1, 1]
    ],
    9: [
        [1, 1],
        [1, 1]
    ],
    3: [
        [1, 1]
    ],
    2: [
        [1], [1]
    ],
    5: [
        [1, 1, 1]
    ],
    4: [
        [1],
        [1],
        [1]
    ],
    17: [
        [1, 1, 1, 1]
    ],
    7: [
        [1],
        [1],
        [1],
        [1]
    ],
    11: [
        [1, 1, 1, 1, 1]
    ],
    24: [
        [-1, -1, 1],
        [-1, -1, 1],
        [1, 1, 1]
    ],
    12: [
        [1, 1, 1],
        [-1, -1, 1],
        [-1, -1, 1]
    ],
    23: [
        [1, -1, -1],
        [1, -1, -1],
        [1, 1, 1]
    ],
    21: [
        [1, 1, 1],
        [1, -1, -1],
        [1, -1, -1]
    ],
    13: [
        [1, 1, 1],
        [1, 1, 1],
        [1, 1, 1]
    ],
    31: [
        [1, 1],
        [1, -1],
        [1, -1]
    ],
    30: [
        [1, 1, 1],
        [-1, -1, 1]
    ],
    29: [
        [-1, 1],
        [-1, 1],
        [1, 1]
    ],
    8: [
        [1, -1, -1],
        [1, 1, 1]
    ],
    32: [
        [1, 1],
        [-1, 1],
        [-1, 1]
    ],
    33: [
        [-1, -1, 1],
        [1, 1, 1]
    ],
    42: [
        [1, -1],
        [1, -1],
        [1, 1]
    ],
    34: [
        [1, 1, 1],
        [1, -1, -1]
    ],
    10: [
        [-1, 1, -1],
        [1, 1, 1]
    ],
    20: [
        [1, -1],
        [1, 1],
        [1, -1]
    ],
    26: [
        [1, 1, 1],
        [-1, 1, -1]
    ],
    25: [
        [-1, 1],
        [1, 1],
        [-1, 1]
    ],
    19: [
        [-1, 1],
        [1, 1],
        [1, -1]
    ],
    18: [
        [1, 1, -1],
        [-1, 1, 1]
    ],
    16: [
        [1, -1],
        [1, 1],
        [-1, 1]
    ],
    14: [
        [-1, 1, 1],
        [1, 1, -1]
    ],
    35: [
        [1, 1, 1],
        [1, 1, 1]
    ],
    36: [
        [1, 1],
        [1, 1],
        [1, 1]
    ],
    37: [
        [1, -1],
        [-1, 1]
    ],
    38: [
        [-1, 1],
        [1, -1]
    ],
    39: [
        [1, -1, -1],
        [-1, 1, -1],
        [-1, -1, 1]
    ],
    40: [
        [-1, -1, 1],
        [-1, 1, -1],
        [1, -1, -1]
    ],
    41: [
        [-1, -1, 1],
        [-1, 1, -1],
        [1, -1, -1]
    ],
    22: [
        [1],
        [1],
        [1],
        [1],
        [1]
    ]

}


def can_place(board: np.ndarray, block: np.ndarray, row: int, col: int):
    """检查是否可以在给定位置放置块。"""
    for r in range(block.shape[0]):
        for c in range(block.shape[1]):
            if block[r, c] > 0:  # 块中存在块
                if (row + r >= board.shape[0] or
                        col + c >= board.shape[1] or
                        board[row + r, col + c] >= 0):  # 超出边界或冲突
                    return False
    return True


def place_block(board: np.ndarray, block: np.ndarray, row: int, col: int, value: int):
    """在盘面上放置或移除块，并检查消除整行或整列。"""
    for r in range(block.shape[0]):
        for c in range(block.shape[1]):
            if block[r, c] >= 0:  # 块中存在块
                board[row + r, col + c] = value
    rs = []
    # 检查消除整行
    for r in range(board.shape[0]):
        if np.all(board[r, :] >= 0):  # 整行都是块
            rs.append(r)

    cs = []
    # 检查消除整列
    for c in range(board.shape[1]):
        if np.all(board[:, c] >= 0):  # 整列都是块
            cs.append(c)

    for r in rs:
        board[r, :] = -1  # 消除整行
    for c in cs:
        board[:, c] = -1  # 消除整列
    return len(rs) + len(cs)


def backtrack(board: np.ndarray, blocks: list, index: int):
    """回溯算法检查是否可以放置所有块。"""
    if index == len(blocks):  # 所有块都已放置
        return 1

    block = blocks[index]
    try:
        for row in range(board.shape[0]):
            for col in range(board.shape[1]):
                if can_place(board, block, row, col):
                    board_new = copy.deepcopy(board)
                    place_block(board_new, block, row, col, 1)  # 放置块
                    if backtrack(board_new, blocks, index + 1):
                        return 1
    except IndexError as e:
        print(board)
        print(e)
        raise Exception("Index Error")
    return 0


def backtrack_cnt(board: np.ndarray, blocks: list, index: int):
    solution_cnt = 0
    """回溯算法检查是否可以放置所有块。"""
    if index == len(blocks):  # 所有块都已放置
        return 1

    block = blocks[index]
    for row in range(board.shape[0]):
        for col in range(board.shape[1]):
            if can_place(board, block, row, col):
                board_new = copy.deepcopy(board)
                place_block(board_new, block, row, col, 1)  # 放置块
                solution_cnt += backtrack_cnt(board_new, blocks, index + 1)
                if solution_cnt >= 10:
                    return solution_cnt

    return solution_cnt


def process_solution_cnt(matrix_str, given_block_types, is_valid):
    start_time = time.time()
    if is_valid == 0:
        return (0, 0.0)
    blocks = json.loads(given_block_types)
    matrix = json.loads(matrix_str)

    block_perms = itertools.permutations(blocks)
    block_perms_unique = set(block_perms)

    solution_cnt = 0

    for block_perm in block_perms_unique:
        blocks_instance = []
        for block_type in list(block_perm):
            blocks_instance.append(np.array(block_type_mappings[block_type]))
        solution_cnt += backtrack_cnt(np.array(matrix), blocks_instance, 0)
        if solution_cnt >= 10:
            break
    if solution_cnt >= 10:
        solution_cnt = 10
    return (solution_cnt, time.time() - start_time)


def process_solution(matrix_str, given_block_types, is_valid):
    start_time = time.time()
    if is_valid < 0:
        return (-1, 0.0)
    blocks = json.loads(given_block_types)
    matrix = json.loads(matrix_str)

    block_perms = itertools.permutations(blocks)
    block_perms_unique = set(block_perms)

    is_solution = 0

    for block_perm in block_perms_unique:
        blocks_instance = []
        for block_type in list(block_perm):
            blocks_instance.append(np.array(block_type_mappings[block_type]))
        if backtrack(np.array(matrix), blocks_instance, 0):
            is_solution = 1
            break
    return (is_solution, time.time() - start_time)


# @jit(nopython=True)
def backtrack_is_eleminate(board: np.ndarray, blocks: list, index: int):
    """回溯算法检查是否可以放置所有块。"""
    if index == len(blocks):  # 所有块都已放置
        return 0

    block = blocks[index]
    for row in range(board.shape[0]):
        for col in range(board.shape[1]):
            if can_place(board, block, row, col):
                board_new = copy.deepcopy(board)
                eleminate_cnt = place_block(board_new, block, row, col, 1)  # 放置块
                if eleminate_cnt > 0 or backtrack_is_eleminate(board_new, blocks, index + 1) == 1:
                    return 1

    return 0


def max_eleminate_cnt(board, block):
    """回溯算法检查是否可以放置所有块。"""
    max_eleminate_cnt = 0
    for row in range(board.shape[0]):
        for col in range(board.shape[1]):
            if can_place(board, block, row, col):
                board_new = copy.deepcopy(board)
                eleminate_cnt = place_block(board_new, block, row, col, 1)  # 放置块
                if eleminate_cnt > max_eleminate_cnt:
                    max_eleminate_cnt = eleminate_cnt
    return max_eleminate_cnt


def process_block_eleminate(matrix_str, given_block_types, is_valid):
    if is_valid == 0:
        return []
    res = []
    blocks = json.loads(given_block_types)
    matrix = json.loads(matrix_str)

    for block in blocks:
        res.append(max_eleminate_cnt(np.array(matrix), np.array(block_type_mappings[block])))
    return res


def process_eleminate(matrix_str, given_block_types, is_valid, block_eleminate):
    start_time = time.time()
    if is_valid == 0:
        return (0, 0.0)
    if any(m > 0 for m in block_eleminate):
        return (1, 0.0)
    blocks = json.loads(given_block_types)
    matrix = json.loads(matrix_str)

    block_perms = itertools.permutations(blocks)
    block_perms_unique = set(block_perms)
    for block_perm in block_perms_unique:
        blocks_instance = []
        for block_type in list(block_perm):
            blocks_instance.append(np.array(block_type_mappings[block_type]))
        if backtrack_is_eleminate(np.array(matrix), blocks_instance, 0):
            time_taken = time.time() - start_time
            return (1, time_taken)

    return (0, time.time() - start_time)


def process_valid(matrix_str,blocks):
    if is_valid_matrix(matrix_str) < 0:
        return is_valid_matrix(matrix_str)
    elif is_valid_blocks(blocks) < 0:
        return is_valid_blocks(blocks)
    return 1
def is_valid_blocks(blocks):
    try:
        if not blocks:
            return -200
        elif any( not str(m).isdigit() for m in json.loads(blocks)):
            return -202
        elif any( int(m) < 0 or int(m) > 42 for m in json.loads(blocks)):
            return -201
        else:
            return 1
    except (json.JSONDecodeError, TypeError):
        # 如果解析失败，说明不是有效的二维数组字符串
        return -203
def is_valid_matrix(matrix_str):
    try:
        # 将字符串解析为二维列表
        matrix = json.loads(matrix_str)

        # 检查是否是列表并且每行列数为 8
        if isinstance(matrix, list) and all(isinstance(item, int) for row in matrix for item in row) and len(
                matrix) == 8 and all(isinstance(row, list) and len(row) == 8 for row in matrix):
            return 1
        elif not all(isinstance(item, int) for row in matrix for item in row):
            return -100
        elif not len(matrix) == 8 or not all(isinstance(row, list) and len(row) == 8 for row in matrix):
            return -101
        else:
            return -102
    except (json.JSONDecodeError, TypeError):
        # 如果解析失败，说明不是有效的二维数组字符串
        return -103


def get_pos_step_info(org_array):
    org_array = [json.loads(s) for s in org_array]
    array_012 = [{"1": "0"}, {"2": "1"}, {"3": "2"}]
    array_021 = [{"1": "0"}, {"2": "2"}, {"3": "1"}]
    array_102 = [{"1": "1"}, {"2": "0"}, {"3": "2"}]
    array_120 = [{"1": "1"}, {"2": "2"}, {"3": "0"}]
    array_201 = [{"1": "2"}, {"2": "0"}, {"3": "1"}]
    array_210 = [{"1": "2"}, {"2": "1"}, {"3": "0"}]
    final_data = ''
    if len(org_array) == 1:
        final_data = org_array[0].get("1")

    else:

        is_012 = all(element in array_012 for element in org_array)
        is_021 = all(element in array_021 for element in org_array)
        is_102 = all(element in array_102 for element in org_array)
        is_120 = all(element in array_120 for element in org_array)
        is_201 = all(element in array_201 for element in org_array)
        is_210 = all(element in array_210 for element in org_array)

        if is_012:
            final_data = '012'
        elif is_021:
            final_data = '021'
        elif is_102:
            final_data = '102'
        elif is_120:
            final_data = '120'
        elif is_201:
            final_data = '201'
        elif is_210:
            final_data = '210'
        else:
            final_data = ''
    return final_data


import copy
import json
import traceback
import numpy as np
import sys


class BaseSignInfo:
    def __init__(self):
        self.square_block_sign = -1
        self.square_empty_sign = 1


class BaseAreaThreshold:
    square_line_length = 8
    square_width_length = 8
    square_line_threshold = 10
    square_line_part_threshold = 15
    square_empty_threshold = 40
    square_empty_threshold_easy = 52
    square_recommend_num = 3
    square_cool_num = 1
    square_cool_percent = 0.035
    square_error_num = '00'
    square_degree_zoom = 2
    square_degree_max = 0.4  # 难度系数最大波动
    square_degree_normal = 0.8  # 基础难度系数
    square_sample_degree_max = 1.4
    square_sample_degree_min = 0.7
    square_area_normalize_max_list = [11, 9, 64, 44, 48, 22, 8, 7, 8, 8, 25, 537]
    square_area_normalize_min_list = [1, 0, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    square_area_threshold_time = 100
    square_area_threshold_num = 5
    square_area_threshold_death_percent = 100


class BaseSquareThreshold:
    """
    方块难度比例类，根据上个方块的选取，迭代盘面复杂度
    """
    square_degree_one = 0.9
    square_degree_two = 0.95
    square_degree_three = 1.05
    square_degree_four = 1.1


class EmptyAreaListClass:
    empty_area_list = [[-1, -1, -1, -1, -1, -1, -1, -1], [-1, -1, -1, -1, -1, -1, -1, -1],
                       [-1, -1, -1, -1, -1, -1, -1, -1], [-1, -1, -1, -1, -1, -1, -1, -1],
                       [-1, -1, -1, -1, -1, -1, -1, -1], [-1, -1, -1, -1, -1, -1, -1, -1],
                       [-1, -1, -1, -1, -1, -1, -1, -1], [-1, -1, -1, -1, -1, -1, -1, -1]]


class BaseSquareClass:
    class_square_type_one = '1'
    class_square_type_two = '2'
    class_square_type_three = '3'
    class_square_type_four = '4'
    class_square_type_dict = {0: class_square_type_one, 1: class_square_type_two, 2: class_square_type_three,
                              3: class_square_type_four}


class BaseTypeClass:
    """
    拐角、行列字典
    """
    class_line = 0
    class_corner = 1
    class_type_dict = {0: class_line, 1: class_corner}


class BaseCornerClass:
    """
    四拐角字典、行列字典
    """
    class_corner = '1'
    class_direction = '2'
    class_corner_type_one = 1
    class_corner_type_two = 2
    class_corner_type_three = 3
    class_corner_type_four = 4
    class_square_line = '1'
    class_square_width = '2'
    class_square_corner_direction_dict = {0: class_corner, 1: class_direction}
    class_corner_type_dict = {0: class_corner_type_one, 1: class_corner_type_two, 2: class_corner_type_three,
                              3: class_corner_type_four}


class BasePercentConfig:
    """
    比例列表
    """
    empty_percent_list = [0.15, 0.33, 0.7]
    empty_percent_easy_list = [0.2, 0.55, 1]
    error_percent_list = [0.25, 0.5, 0.75]
    line_width_error_list = [0.5]
    normal_percent_list = [0.15, 0.55, 0.9]
    cool_percent_list = [0.035]
    cool_percent_row_area_list = [0.5]

    second_corner_direction_percent_list = [2]
    third_corner_direction_percent_list = [0.5]
    fourth_direction_percent_list = [0.5]


class BaseRecommendSquareConfig:
    """
    方块选取标记
    """
    EmptySign = 'empty'
    CoolSign = 'cool'
    NormalSign = 'normal'
    RandomSign = 'random'
    TriplicateSign = 'triplicate'


class TestAreaList:
    area_test_list = [[[-1.0, 6.0, -1.0, 2.0, 2.0, 6.0, -1.0, -1.0], [-1.0, 6.0, -1.0, 2.0, 2.0, 6.0, 1.0, 1.0],
                       [-1.0, -1.0, -1.0, 2.0, 2.0, 1.0, -1.0, 1.0], [6.0, 6.0, 2.0, 2.0, 2.0, 1.0, -1.0, 1.0],
                       [-1.0, 2.0, 2.0, 2.0, 2.0, 1.0, 1.0, -1.0], [-1.0, 2.0, 2.0, 2.0, 2.0, -1.0, 1.0, 1.0],
                       [1.0, 2.0, -1.0, -1.0, -1.0, 1.0, 1.0, -1.0], [1.0, 1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0]],
                      [[4.0, 4.0, 2.0, 2.0, 1.0, 1.0, -1.0, -1.0], [4.0, -1.0, 2.0, 2.0, 1.0, 1.0, 1.0, 1.0],
                       [-1.0, -1.0, 2.0, 2.0, -1.0, -1.0, 7.0, 1.0], [-1.0, 1.0, -1.0, -1.0, 3.0, 3.0, 3.0, 1.0],
                       [1.0, -1.0, -1.0, 3.0, -1.0, 3.0, 3.0, 3.0], [-1.0, 1.0, 6.0, 3.0, -1.0, 5.0, 3.0, 3.0],
                       [-1.0, -1.0, 6.0, -1.0, -1.0, 5.0, -3.0, 3.0], [-1.0, -1.0, -1.0, -1.0, -1.0, 5.0, -1.0, 1.0]],
                      [[5.0, -1.0, -1.0, -1.0, -1.0, 6.0, 6.0, -1.0], [5.0, -1.0, -1.0, -1.0, -1.0, 6.0, 6.0, -1.0],
                       [5.0, -1.0, -1.0, -1.0, -1.0, 6.0, 6.0, -1.0], [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0],
                       [-1.0, 6.0, 6.0, 6.0, 3.0, 3.0, 3.0, 5.0], [-1.0, -1.0, -1.0, -1.0, -1.0, 6.0, 6.0, -1.0],
                       [-1.0, -1.0, -1.0, 6.0, -1.0, 6.0, 6.0, -1.0], [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0]],
                      [[-1.0, -1.0, 2.0, -1.0, -1.0, 2.0, -1.0, -1.0], [-1.0, -1.0, 2.0, -1.0, -1.0, 2.0, -1.0, -1.0],
                       [-1.0, -1.0, 2.0, -1.0, -1.0, 2.0, -1.0, -1.0], [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0],
                       [-1.0, 1.0, -1.0, -1.0, -1.0, 1.0, -1.0, -1.0], [-1.0, 1.0, 1.0, -1.0, -1.0, 6.0, -1.0, -1.0],
                       [4.0, 4.0, 1.0, -1.0, -1.0, -1.0, -1.0, -1.0], [4.0, 3.0, 3.0, -1.0, 3.0, 3.0, -1.0, -1.0]],
                      [[-1.0, -1.0, 6.0, 6.0, 6.0, -1.0, -1.0, -1.0], [-1.0, -1.0, -1.0, -1.0, 6.0, -1.0, -1.0, -1.0],
                       [-1.0, -1.0, -1.0, -1.0, 6.0, -1.0, -1.0, -1.0],
                       [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0],
                       [5.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0], [5.0, -1.0, 6.0, 6.0, 1.0, -1.0, -1.0, -1.0],
                       [5.0, -1.0, 6.0, 6.0, 3.0, -1.0, -1.0, -1.0], [5.0, -1.0, 4.0, 3.0, 3.0, -1.0, -1.0, -1.0]]]


sys.path.append('../')


# 盘面类
class BaseAreaInfo(BaseSignInfo):
    def __init__(self, origin_list):
        super(BaseAreaInfo, self).__init__()
        """
        初始化类函数
        block_sign和empty_sign代表已填充/未填充的矩阵表示
        area_list 用于存储转换后的盘面信息
        area_length_size 盘面长度
        area_width_size 盘面宽度
        area_empty_num 未填充区域数
        area_block_num 已填充区域数
        square_empty_num 未填充方块数
        line_length_sum 行包含的边数
        line_width_sum 列包含的边数
        line_part_num 边所对应的段数
        line_outside_num 暴露在外面的边数
        corner_dict 拐角字典
        corner_sum 阳角数量
        corner_inside_num 阴角数量
        """
        self.area_list = []
        if origin_list:
            for origin_list_sample in origin_list:
                self.area_list.append(copy.copy(origin_list_sample))
        else:
            self.area_list.append([])
        self.area_length_size = 0
        self.area_width_size = 0
        self.area_empty_num = 0
        self.area_block_num = 0
        self.square_empty_num = 0
        self.line_length_sum = 0
        self.line_width_sum = 0
        self.line_part_num = 0
        self.line_outside_num = 0
        self.corner_dict = {}
        self.corner_sum = 0
        self.corner_inside_num = 0

    def convert_list(self):
        """
        该函数用于转化盘面数字，-1转化为1，其余数字转化为-1，用于plt.show展示符合预期图片
        :return: 转化后的盘面
        """
        for area_iter_i in range(len(self.area_list)):
            for area_iter_j in range(len(self.area_list[area_iter_i])):
                if self.area_list[area_iter_i][area_iter_j] > -1:
                    self.area_list[area_iter_i][area_iter_j] = self.square_block_sign
                else:
                    self.area_list[area_iter_i][area_iter_j] = self.square_empty_sign
        self.get_size()

    def get_size(self):
        self.area_length_size = len(self.area_list)
        self.area_width_size = len(self.area_list[0])

    def get_area_num(self):
        """
        获取盘面块、可放置方块数
        :return:未填充区域数、已填充区域数、未填充方块数
        """
        # result_list用于记录区域盘面信息，每一次迭代会填充一份区域
        result_list = [[0] * self.area_length_size for _i in range(self.area_width_size)]
        area_empty_num = 0
        area_block_num = 0
        square_empty_num = 0
        for iter_i in range(self.area_length_size):
            for iter_j in range(self.area_width_size):
                if self.area_list[iter_i][iter_j] == self.square_empty_sign:
                    square_empty_num += 1
                if result_list[iter_i][iter_j] != 0:
                    continue
                result_list[iter_i][iter_j] = 1
                self.area_test(result_list, iter_i, iter_j)
                if self.area_list[iter_i][iter_j] == self.square_block_sign:
                    area_block_num += 1
                else:
                    area_empty_num += 1
        self.area_empty_num = area_empty_num
        self.area_block_num = area_block_num
        self.square_empty_num = square_empty_num
        # return area_empty_num, area_block_num, square_empty_num

    def area_test(self, result_list, origin_i, origin_j):
        """
        :param result_list: 区域盘面信息
        :param origin_i: 所需判定坐标x
        :param origin_j: 所需判定坐标y
        :return:
        """
        target_list = []
        self.judge_sample(result_list, target_list, origin_i, origin_j)
        while target_list:
            temp_x, temp_y = target_list[0]
            target_list.pop(0)
            self.judge_sample(result_list, target_list, temp_x, temp_y)

    def judge_sample(self, result_list, target_list, origin_i, origin_j):
        """
        :param result_list: 区域盘面信息
        :param target_list: 待确认点列表
        :param origin_i: 所需判定坐标x
        :param origin_j: 所需判定坐标y
        :return:
        """
        if self.judge_area(result_list, origin_i + 1, origin_j, self.area_list[origin_i][origin_j]):
            target_list.append([origin_i + 1, origin_j])
        if self.judge_area(result_list, origin_i - 1, origin_j, self.area_list[origin_i][origin_j]):
            target_list.append([origin_i - 1, origin_j])
        if self.judge_area(result_list, origin_i, origin_j + 1, self.area_list[origin_i][origin_j]):
            target_list.append([origin_i, origin_j + 1])
        if self.judge_area(result_list, origin_i, origin_j - 1, self.area_list[origin_i][origin_j]):
            target_list.append([origin_i, origin_j - 1])

    def judge_area(self, result_list, temp_i, temp_j, temp_sign):
        """
        :param result_list: 区域盘面信息
        :param temp_i: 待确认点坐标x
        :param temp_j: 待确认点坐标y
        :param temp_sign: 父节点盘面填充信息
        :return:
        """
        # print(temp_i, temp_j)
        if 0 <= temp_i < self.area_length_size and 0 <= temp_j < self.area_width_size and \
                result_list[temp_i][temp_j] == 0 and temp_sign * self.area_list[temp_i][temp_j] > 0:
            result_list[temp_i][temp_j] = 1
            return True
        else:
            return False

    def get_line_outside_num(self):
        line_outside_num = 0
        for iter_i in range(self.area_length_size):
            for iter_j in range(self.area_width_size):
                if iter_j != self.area_width_size - 1 and self.area_list[iter_i][iter_j] * self.area_list[iter_i][
                    iter_j + 1] < 0:
                    line_outside_num += 1
                if iter_i != self.area_length_size - 1 and self.area_list[iter_i][iter_j] * self.area_list[iter_i + 1][
                    iter_j] < 0:
                    line_outside_num += 1
        self.line_outside_num = line_outside_num

    def get_line_num(self):
        """
        该函数用于获取盘面的边、段数据
        :return:返回盘面所包含的边数和段数
        """
        part_num = 1
        line_length_sum = 0
        line_width_sum = 0
        line_part_num = 0
        for iter_i in range(self.area_length_size):
            line_length_num = 0
            line_width_num = 0
            for iter_j in range(self.area_width_size):
                line_length_sum, line_length_num, line_part_num = self.single_point_judge(iter_i, iter_j, part_num,
                                                                                          line_part_num,
                                                                                          line_length_sum,
                                                                                          line_length_num, 0)
                line_width_sum, line_width_num, line_part_num = self.single_point_judge(iter_j, iter_i, part_num,
                                                                                        line_part_num, line_width_sum,
                                                                                        line_width_num, 1)
            if line_length_num > part_num:
                line_length_sum += line_length_num
                line_part_num += 1
            if line_width_num > part_num:
                line_width_sum += line_width_num
                line_part_num += 1
        # print(line_length_sum, line_width_sum, line_part_num)
        line_length_num = 0
        line_width_num = 0
        for iter_i in range(self.area_length_size):
            if self.area_list[self.area_width_size - 1][iter_i] == self.square_block_sign:
                line_length_num += 1
            else:
                if line_length_num > part_num:
                    line_length_sum += line_length_num
                    line_part_num += 1
                line_length_num = 0
        if line_length_num > part_num:
            line_part_num += 1
            line_length_sum += line_length_num
        for iter_i in range(self.area_width_size):
            if self.area_list[iter_i][self.area_length_size - 1] == self.square_block_sign:
                line_width_num += 1
            else:
                if line_width_num > part_num:
                    line_part_num += 1
                    line_width_sum += line_width_num
                line_width_num = 0
        if line_width_num > part_num:
            line_part_num += 1
            line_width_sum += line_width_num
        self.line_length_sum = line_length_sum
        self.line_width_sum = line_width_sum
        self.line_part_num = line_part_num

    def single_point_judge(self, origin_i, origin_j, part_num, temp_part_num, temp_sum, temp_num, target_sign):
        """
        该函数用于统计行、列的边、段数
        :param origin_i:所传入的坐标x
        :param origin_j:所传入的坐标y
        :param part_num:边分割标识，通过该数值定义边的长度(单为1是否为边的定义)
        :param temp_part_num:边分割段
        :param temp_sum:行/列的边数总和
        :param temp_num:行/列的边数临时变量
        :param target_sign:用于标记是行扫描还是列扫描
        :return:行/列的边数总和、行/列的边数临时变量、边分割段
        """
        if target_sign == 0:
            # 获取行列比较对象
            first_sign = self.area_list[origin_i - 1][origin_j]
            last_sign = self.area_list[origin_i][origin_j - 1]
        else:
            # 获取行列比较对象
            first_sign = self.area_list[origin_i][origin_j - 1]
            last_sign = self.area_list[origin_i - 1][origin_j]
        if (target_sign == 0 and origin_i == 0) or (target_sign == 1 and origin_j == 0):
            if self.area_list[origin_i][origin_j] == self.square_block_sign:
                # 当前块为已填充，则数量直接自增1即可
                temp_num += 1
            else:
                # 当前块为未填充，叠加即可
                if temp_num > part_num:
                    temp_sum += temp_num
                    temp_part_num += 1
                temp_num = 0
        else:
            if first_sign == self.area_list[origin_i][origin_j]:
                # 当前方块和上一行同位置相等时
                if temp_num > part_num:
                    temp_sum += temp_num
                    temp_part_num += 1
                temp_num = 0
            else:
                # 当前方块和上一行同位置不相等时
                if (target_sign == 0 and origin_j == 0) or (target_sign == 1 and origin_i == 0):
                    # 如果是第一列，自增1即可
                    temp_num += 1
                else:
                    # 在不是第一列的前提下
                    if last_sign != self.area_list[origin_i][origin_j]:
                        # 如果和同行前列数据不一致时
                        if temp_num > part_num:
                            temp_sum += temp_num
                            temp_part_num += 1
                        temp_num = 1
                    else:
                        temp_num += 1
        return temp_sum, temp_num, temp_part_num

    def get_angle_num(self):
        """
        该函数功能为获取不同类型拐角数量
        依次从上到下、从左到右判断拐角数量
        :return:获取拐角信息
        """
        corner_dict = {BaseCornerClass.class_corner_type_one: 0, BaseCornerClass.class_corner_type_two: 0,
                       BaseCornerClass.class_corner_type_three: 0, BaseCornerClass.class_corner_type_four: 0}
        for iter_i in range(self.area_width_size - 1):
            # 最后一行无需判断
            for iter_j in range(self.area_length_size):
                if iter_j == 0:
                    self.judge_angle_right(corner_dict, iter_i, iter_j)
                elif iter_j == self.area_length_size - 1:
                    self.judge_angle_left(corner_dict, iter_i, iter_j)
                else:
                    self.judge_angle_left(corner_dict, iter_i, iter_j)
                    self.judge_angle_right(corner_dict, iter_i, iter_j)
        self.corner_dict = corner_dict

    def get_angle_num_and_list(self):
        """
        该函数功能为获取不同类型拐角数量
        依次从上到下、从左到右判断拐角数量
        :return:获取拐角信息
        """
        corner_dict = {BaseCornerClass.class_corner_type_one: 0, BaseCornerClass.class_corner_type_two: 0,
                       BaseCornerClass.class_corner_type_three: 0, BaseCornerClass.class_corner_type_four: 0}
        for iter_i in range(self.area_width_size - 1):
            # 最后一行无需判断
            for iter_j in range(self.area_length_size):
                if iter_j == 0:
                    self.judge_angle_right_list(corner_dict, iter_i, iter_j)
                elif iter_j == self.area_length_size - 1:
                    self.judge_angle_left_list(corner_dict, iter_i, iter_j)
                else:
                    self.judge_angle_left_list(corner_dict, iter_i, iter_j)
                    self.judge_angle_right_list(corner_dict, iter_i, iter_j)
        self.corner_dict = corner_dict

    def judge_angle_left_list(self, corner_dict, origin_i, origin_j):
        """
        以该顶点为中心，判断左下方是否存在拐角
        :param corner_dict:拐角列表
        :param origin_i:坐标x
        :param origin_j:坐标y
        :return:
        """
        if self.area_list[origin_i][origin_j] == self.area_list[origin_i + 1][origin_j - 1]:
            # 标志点和对角的方块相等，则无需判断
            return
        if self.area_list[origin_i + 1][origin_j] != self.area_list[origin_i][origin_j - 1]:
            # 标志点左侧和下侧的方块不相等，则无需判断
            return
        if self.area_list[origin_i + 1][origin_j] == self.area_list[origin_i][origin_j]:
            # 两对角相等，并且标志点右侧角和标志点相同
            if self.area_list[origin_i][origin_j] == self.square_empty_sign:
                corner_dict[BaseCornerClass.class_corner_type_four] += 1
                self.corner_sum += 1
            else:
                # 阴角
                self.corner_inside_num += 1
        else:
            # 两对角相等，并且标志点右侧角和标志点不同
            if self.area_list[origin_i][origin_j] == self.square_block_sign:
                corner_dict[BaseCornerClass.class_corner_type_two] += 1
                self.corner_sum += 1
            else:
                # 阴角
                self.corner_inside_num += 1

    def judge_angle_right_list(self, corner_dict, origin_i, origin_j):
        """
        以该顶点为中心，判断右下方是否存在拐角
        :param corner_dict:拐角列表
        :param origin_i:坐标x
        :param origin_j:坐标y
        :return:
        """
        if self.area_list[origin_i + 1][origin_j] != self.area_list[origin_i][origin_j + 1]:
            # 标志点和对角的方块相等，则无需判断
            return
        if self.area_list[origin_i][origin_j] == self.area_list[origin_i + 1][origin_j + 1]:
            # 判断是否存在[[0,1][1,0]]、[[1,0],[0,1]]这类对拐角
            # 仅从单侧判断一次即可
            if self.area_list[origin_i][origin_j] != self.area_list[origin_i + 1][origin_j] and \
                    self.area_list[origin_i + 1][origin_j] == self.area_list[origin_i][origin_j + 1]:
                if self.area_list[origin_i][origin_j] == self.square_block_sign:
                    corner_dict[BaseCornerClass.class_corner_type_one] += 1
                    corner_dict[BaseCornerClass.class_corner_type_three] += 1
                else:
                    corner_dict[BaseCornerClass.class_corner_type_two] += 1
                    corner_dict[BaseCornerClass.class_corner_type_four] += 1
                self.corner_sum += 2
            return
        if self.area_list[origin_i + 1][origin_j] == self.area_list[origin_i][origin_j]:
            # 两对角相等，并且标志点右侧角和标志点相同
            if self.area_list[origin_i][origin_j] == self.square_empty_sign:
                # 阳角
                corner_dict[BaseCornerClass.class_corner_type_three] += 1
                self.corner_sum += 1
            else:
                # 阴角
                self.corner_inside_num += 1
        else:
            # 两对角相等，并且标志点右侧角和标志点不同
            if self.area_list[origin_i][origin_j] == self.square_block_sign:
                # 阳角
                corner_dict[BaseCornerClass.class_corner_type_one] += 1
                self.corner_sum += 1
            else:
                # 阴角
                self.corner_inside_num += 1

    def judge_angle_left(self, corner_dict, origin_i, origin_j):
        """
        以该顶点为中心，判断左下方是否存在拐角
        :param corner_dict:拐角列表
        :param origin_i:坐标x
        :param origin_j:坐标y
        :return:
        """
        if self.area_list[origin_i][origin_j] == self.area_list[origin_i + 1][origin_j - 1]:
            # 标志点和对角的方块相等，则无需判断
            return
        if self.area_list[origin_i + 1][origin_j] != self.area_list[origin_i][origin_j - 1]:
            # 标志点左侧和下侧的方块不相等，则无需判断
            return
        if self.area_list[origin_i + 1][origin_j] == self.area_list[origin_i][origin_j]:
            # 两对角相等，并且标志点右侧角和标志点相同
            if self.area_list[origin_i][origin_j] == self.square_empty_sign:
                corner_dict[BaseCornerClass.class_corner_type_four] += 1
                self.corner_sum += 1
        else:
            # 两对角相等，并且标志点右侧角和标志点不同
            if self.area_list[origin_i][origin_j] == self.square_block_sign:
                corner_dict[BaseCornerClass.class_corner_type_two] += 1
                self.corner_sum += 1

    def judge_angle_right(self, corner_dict, origin_i, origin_j):
        """
        以该顶点为中心，判断右下方是否存在拐角
        :param corner_dict:拐角列表
        :param origin_i:坐标x
        :param origin_j:坐标y
        :return:
        """
        if self.area_list[origin_i + 1][origin_j] != self.area_list[origin_i][origin_j + 1]:
            # 标志点和对角的方块相等，则无需判断
            return
        if self.area_list[origin_i][origin_j] == self.area_list[origin_i + 1][origin_j + 1]:
            # 判断是否存在[[0,1][1,0]]、[[1,0],[0,1]]这类对拐角
            # 仅从单侧判断一次即可
            if self.area_list[origin_i][origin_j] != self.area_list[origin_i + 1][origin_j] and \
                    self.area_list[origin_i + 1][origin_j] == self.area_list[origin_i][origin_j + 1]:
                if self.area_list[origin_i][origin_j] == self.square_block_sign:
                    corner_dict[BaseCornerClass.class_corner_type_one] += 1
                    corner_dict[BaseCornerClass.class_corner_type_three] += 1
                else:
                    corner_dict[BaseCornerClass.class_corner_type_two] += 1
                    corner_dict[BaseCornerClass.class_corner_type_four] += 1
                self.corner_sum += 2
            return
        if self.area_list[origin_i + 1][origin_j] == self.area_list[origin_i][origin_j]:
            # 两对角相等，并且标志点右侧角和标志点相同
            if self.area_list[origin_i][origin_j] == self.square_empty_sign:
                # 阳角
                corner_dict[BaseCornerClass.class_corner_type_three] += 1
                self.corner_sum += 1
        else:
            # 两对角相等，并且标志点右侧角和标志点不同
            if self.area_list[origin_i][origin_j] == self.square_block_sign:
                # 阳角
                corner_dict[BaseCornerClass.class_corner_type_one] += 1
                self.corner_sum += 1

    def area_image_show(self):
        """
        列表画图函数
        :return: 图形化展示
        """
        import matplotlib.pyplot as plt
        plt.imshow(self.area_list, cmap='gray')
        # plt.grid()
        plt.show()

    def get_area_info(self):
        """
        获取盘面所有属性数据，不包含拐角列表
        :return:
        """
        self.convert_list()
        self.get_area_num()
        self.get_line_num()
        self.get_angle_num()

    def get_area_info_and_corner_list(self):
        """
        获取盘面所有属性数据
        :return:
        """
        self.convert_list()
        # self.get_area_num()
        # self.get_line_num()
        # self.get_line_outside_num()
        # self.get_angle_num_and_list()


sys.path.append('../')


class BaseSquareInfo(BaseSignInfo):
    def __init__(self, block_list=None):
        super(BaseSquareInfo, self).__init__()
        """
        block_num 方块编号
        block_amount 方块数量
        block_list 方块区域分布
        block_shift 方块偏移量，指第一行有几个empty_sign
        block_length 方块长度
        block_width 方块宽度
        """
        if block_list is None:
            block_list = [[]]
        self.block_num = -1
        self.block_amount = -1
        self.block_shift = -1
        self.block_length = -1
        self.block_width = -1
        self.block_list = block_list

    # def square_image_show(self):
    #     """
    #     列表画图函数
    #     :return: 图形化展示
    #     """
    #     temp_list = []
    #     max_length = -1
    #     empty_sign = False
    #     temp_judge_count = 0
    #     for iter_i in range(len(self.block_list)):
    #         for iter_j in range(len(self.block_list[iter_i])):
    #             max_length = max(max_length, iter_j)
    #             temp_judge_count += 1
    #             if not empty_sign and self.block_list[iter_i][iter_j] == self.square_empty_sign:
    #                 empty_sign = True
    #     if empty_sign or (max_length + 1) * len(self.block_list) > temp_judge_count:
    #         for iter_i in range(len(self.block_list)):
    #             sample_list = []
    #             for iter_j in range(max_length + 1):
    #                 if iter_j < len(self.block_list[iter_i]):
    #                     sample_list.append(self.block_list[iter_i][iter_j])
    #                 else:
    #                     sample_list.append(self.square_empty_sign)
    #             temp_list.append(copy.copy(sample_list))
    #     else:
    #         temp_list = self.block_list
    #     plt.imshow(temp_list, cmap='gray')
    #     plt.grid()
    #     plt.show()


class SquareType01Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType01Info, self).__init__()
        self.block_num = '01'
        self.block_amount = 1
        self.block_shift = 0
        self.block_length = 1
        self.block_width = 1
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [[self.square_block_sign]]


class SquareType02Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType02Info, self).__init__()
        self.block_num = '02'
        self.block_amount = 2
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 1
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [[self.square_block_sign],
                           [self.square_block_sign]]


class SquareType03Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType03Info, self).__init__()
        self.block_num = '03'
        self.block_amount = 2
        self.block_shift = 0
        self.block_length = 1
        self.block_width = 2
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [[self.square_block_sign, self.square_block_sign]]


class SquareType04Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType04Info, self).__init__()
        self.block_num = '04'
        self.block_amount = 3
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 1
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [[self.square_block_sign],
                           [self.square_block_sign],
                           [self.square_block_sign]]


class SquareType05Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType05Info, self).__init__()
        self.block_num = '05'
        self.block_amount = 3
        self.block_shift = 0
        self.block_length = 1
        self.block_width = 3
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [[self.square_block_sign, self.square_block_sign, self.square_block_sign]]


class SquareType06Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType06Info, self).__init__()
        self.block_num = '06'
        self.block_amount = 3
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 2
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign]]


class SquareType07Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType07Info, self).__init__()
        self.block_num = '07'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 4
        self.block_width = 1
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [[self.square_block_sign],
                           [self.square_block_sign],
                           [self.square_block_sign],
                           [self.square_block_sign]]


class SquareType08Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType08Info, self).__init__()
        self.block_num = '08'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 3
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign, self.square_block_sign]]


class SquareType09Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType09Info, self).__init__()
        self.block_num = '09'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 2
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [[self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign]]


class SquareType10Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType10Info, self).__init__()
        self.block_num = '10'
        self.block_amount = 4
        self.block_shift = 1
        self.block_length = 2
        self.block_width = 3
        self.block_corner_inside = 2
        self.block_corner_outside = 6
        self.block_list = [[self.square_empty_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign, self.square_block_sign]]


class SquareType11Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType11Info, self).__init__()
        self.block_num = '11'
        self.block_amount = 5
        self.block_shift = 0
        self.block_length = 1
        self.block_width = 5
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [
            [self.square_block_sign, self.square_block_sign, self.square_block_sign, self.square_block_sign,
             self.square_block_sign]]


class SquareType12Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType12Info, self).__init__()
        self.block_num = '12'
        self.block_amount = 5
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 3
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign, self.square_block_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_empty_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_empty_sign, self.square_block_sign]]


class SquareType13Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType13Info, self).__init__()
        self.block_num = '13'
        self.block_amount = 9
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 3
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [[self.square_block_sign, self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign, self.square_block_sign]]


class SquareType14Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType14Info, self).__init__()
        self.block_num = '14'
        self.block_amount = 4
        self.block_shift = 1
        self.block_length = 2
        self.block_width = 3
        self.block_corner_inside = 2
        self.block_corner_outside = 6
        self.block_list = [[self.square_empty_sign, self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign]]


class SquareType15Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType15Info, self).__init__()
        self.block_num = '15'
        self.block_amount = 3
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 2
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign]]


class SquareType16Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType16Info, self).__init__()
        self.block_num = '16'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 2
        self.block_corner_inside = 2
        self.block_corner_outside = 6
        self.block_list = [[self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign]]


class SquareType17Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType17Info, self).__init__()
        self.block_num = '17'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 1
        self.block_width = 4
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [
            [self.square_block_sign, self.square_block_sign, self.square_block_sign, self.square_block_sign]]


class SquareType18Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType18Info, self).__init__()
        self.block_num = '18'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 3
        self.block_corner_inside = 2
        self.block_corner_outside = 6
        self.block_list = [[self.square_block_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign, self.square_block_sign]]


class SquareType19Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType19Info, self).__init__()
        self.block_num = '19'
        self.block_amount = 4
        self.block_shift = 1
        self.block_length = 3
        self.block_width = 2
        self.block_corner_inside = 2
        self.block_corner_outside = 6
        self.block_list = [[self.square_empty_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign]]


class SquareType20Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType20Info, self).__init__()
        self.block_num = '20'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 2
        self.block_corner_inside = 2
        self.block_corner_outside = 6
        self.block_list = [[self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign]]


class SquareType21Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType21Info, self).__init__()
        self.block_num = '21'
        self.block_amount = 5
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 3
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign, self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign],
                           [self.square_block_sign]]


class SquareType22Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType22Info, self).__init__()
        self.block_num = '22'
        self.block_amount = 5
        self.block_shift = 0
        self.block_length = 5
        self.block_width = 1
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [[self.square_block_sign],
                           [self.square_block_sign],
                           [self.square_block_sign],
                           [self.square_block_sign],
                           [self.square_block_sign]]


class SquareType23Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType23Info, self).__init__()
        self.block_num = '23'
        self.block_amount = 5
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 3
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign],
                           [self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign, self.square_block_sign]]


class SquareType24Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType24Info, self).__init__()
        self.block_num = '24'
        self.block_amount = 5
        self.block_shift = 2
        self.block_length = 3
        self.block_width = 3
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_empty_sign, self.square_empty_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_empty_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign, self.square_block_sign]]


class SquareType25Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType25Info, self).__init__()
        self.block_num = '25'
        self.block_amount = 4
        self.block_shift = 1
        self.block_length = 3
        self.block_width = 2
        self.block_corner_inside = 2
        self.block_corner_outside = 6
        self.block_list = [[self.square_empty_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign]]


class SquareType26Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType26Info, self).__init__()
        self.block_num = '26'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 3
        self.block_corner_inside = 2
        self.block_corner_outside = 6
        self.block_list = [[self.square_block_sign, self.square_block_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign]]


class SquareType27Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType27Info, self).__init__()
        self.block_num = '27'
        self.block_amount = 3
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 2
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign]]


class SquareType28Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType28Info, self).__init__()
        self.block_num = '28'
        self.block_amount = 3
        self.block_shift = 1
        self.block_length = 2
        self.block_width = 2
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_empty_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign]]


class SquareType29Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType29Info, self).__init__()
        self.block_num = '29'
        self.block_amount = 4
        self.block_shift = 1
        self.block_length = 3
        self.block_width = 2
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_empty_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign]]


class SquareType30Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType30Info, self).__init__()
        self.block_num = '30'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 3
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign, self.square_block_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_empty_sign, self.square_block_sign]]


class SquareType31Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType31Info, self).__init__()
        self.block_num = '31'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 2
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign],
                           [self.square_block_sign]]


class SquareType32Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType32Info, self).__init__()
        self.block_num = '32'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 2
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign]]


class SquareType33Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType33Info, self).__init__()
        self.block_num = '33'
        self.block_amount = 4
        self.block_shift = 2
        self.block_length = 2
        self.block_width = 3
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_empty_sign, self.square_empty_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign, self.square_block_sign]]


class SquareType34Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType34Info, self).__init__()
        self.block_num = '34'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 3
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign, self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign]]


class SquareType35Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType35Info, self).__init__()
        self.block_num = '35'
        self.block_amount = 6
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 3
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [[self.square_block_sign, self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign, self.square_block_sign]]


class SquareType36Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType36Info, self).__init__()
        self.block_num = '36'
        self.block_amount = 6
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 2
        self.block_corner_inside = 0
        self.block_corner_outside = 4
        self.block_list = [[self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign]]


class SquareType37Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType37Info, self).__init__()
        self.block_num = '37'
        self.block_amount = 2
        self.block_shift = 0
        self.block_length = 2
        self.block_width = 2
        self.block_corner_inside = 2
        self.block_corner_outside = 8
        self.block_list = [[self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign]]


class SquareType38Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType38Info, self).__init__()
        self.block_num = '38'
        self.block_amount = 2
        self.block_shift = 1
        self.block_length = 2
        self.block_width = 2
        self.block_corner_inside = 2
        self.block_corner_outside = 8
        self.block_list = [[self.square_empty_sign, self.square_block_sign],
                           [self.square_block_sign]]


class SquareType39Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType39Info, self).__init__()
        self.block_num = '39'
        self.block_amount = 3
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 3
        self.block_corner_inside = 4
        self.block_corner_outside = 12
        self.block_list = [[self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_empty_sign, self.square_block_sign]]


class SquareType40Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType40Info, self).__init__()
        self.block_num = '40'
        self.block_amount = 3
        self.block_shift = 2
        self.block_length = 3
        self.block_width = 3
        self.block_corner_inside = 4
        self.block_corner_outside = 12
        self.block_list = [[self.square_empty_sign, self.square_empty_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign],
                           [self.square_block_sign]]


class SquareType41Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType41Info, self).__init__()
        self.block_num = '41'
        self.block_amount = 3
        self.block_shift = 2
        self.block_length = 3
        self.block_width = 3
        self.block_corner_inside = 4
        self.block_corner_outside = 12
        self.block_list = [[self.square_empty_sign, self.square_empty_sign, self.square_block_sign],
                           [self.square_empty_sign, self.square_block_sign],
                           [self.square_block_sign]]


class SquareType42Info(BaseSquareInfo):
    def __init__(self):
        super(SquareType42Info, self).__init__()
        self.block_num = '42'
        self.block_amount = 4
        self.block_shift = 0
        self.block_length = 3
        self.block_width = 2
        self.block_corner_inside = 1
        self.block_corner_outside = 5
        self.block_list = [[self.square_block_sign],
                           [self.square_block_sign],
                           [self.square_block_sign, self.square_block_sign]]


def get_square_class_dict():
    square_class_dict = {BaseSquareClass.class_square_type_one: ['01', '02', '03'],
                         BaseSquareClass.class_square_type_two: [
                             '10', '14', '16', '18', '19', '20', '25', '26', '37', '38', '39', '40'],
                         BaseSquareClass.class_square_type_three: [
                             '04', '05', '06', '07', '08', '09', '15', '17', '27', '28', '29', '30', '31', '32', '33',
                             '34', '42'],
                         BaseSquareClass.class_square_type_four: ['11', '12', '13', '21', '22', '23', '24', '35', '36']}
    return square_class_dict


def get_second_corner_class_dict():
    second_corner_class_dict = {BaseCornerClass.class_corner_type_one: ['10', '14', '19', '25', '38', '40'],
                                BaseCornerClass.class_corner_type_two: ['10', '16', '18', '20', '37', '39'],
                                BaseCornerClass.class_corner_type_three: ['14', '19', '20', '26', '38', '40'],
                                BaseCornerClass.class_corner_type_four: ['16', '18', '25', '26', '37', '39']}
    return second_corner_class_dict


def get_third_corner_class_dict():
    third_corner_class_dict = {BaseCornerClass.class_corner_type_one: ['28', '29', '33'],
                               BaseCornerClass.class_corner_type_two: ['08', '27', '42'],
                               BaseCornerClass.class_corner_type_three: ['06', '31', '34'],
                               BaseCornerClass.class_corner_type_four: ['15', '30', '32']}
    return third_corner_class_dict


def get_third_direction_dict():
    third_line_width_dict = {BaseCornerClass.class_square_line: ['05', '09', '17'],
                             BaseCornerClass.class_square_width: ['04', '07', '09']}
    return third_line_width_dict


def get_fourth_corner_class_dict():
    corner_class_dict = {BaseCornerClass.class_corner_type_one: ['24'], BaseCornerClass.class_corner_type_two: ['23'],
                         BaseCornerClass.class_corner_type_three: ['21'],
                         BaseCornerClass.class_corner_type_four: ['12']}
    return corner_class_dict


def get_fourth_direction_dict():
    fourth_line_width_dict = {BaseCornerClass.class_square_line: ['11', '13', '35'],
                              BaseCornerClass.class_square_width: ['13', '22', '36']}
    return fourth_line_width_dict


def get_square_number_dict():
    square_number_dict = {
        '01': SquareType01Info(),
        '02': SquareType02Info(),
        '03': SquareType03Info(),
        '04': SquareType04Info(),
        '05': SquareType05Info(),
        '06': SquareType06Info(),
        '07': SquareType07Info(),
        '08': SquareType08Info(),
        '09': SquareType09Info(),
        '15': SquareType15Info(),
        '17': SquareType17Info(),
        '27': SquareType27Info(),
        '28': SquareType28Info(),
        '29': SquareType29Info(),
        '30': SquareType30Info(),
        '31': SquareType31Info(),
        '32': SquareType32Info(),
        '33': SquareType33Info(),
        '34': SquareType34Info(),
        '42': SquareType42Info(),
        '10': SquareType10Info(),
        '14': SquareType14Info(),
        '16': SquareType16Info(),
        '18': SquareType18Info(),
        '19': SquareType19Info(),
        '20': SquareType20Info(),
        '25': SquareType25Info(),
        '26': SquareType26Info(),
        '37': SquareType37Info(),
        '38': SquareType38Info(),
        '39': SquareType39Info(),
        '40': SquareType40Info(),
        '41': SquareType40Info(),
        '11': SquareType11Info(),
        '12': SquareType12Info(),
        '13': SquareType13Info(),
        '21': SquareType21Info(),
        '22': SquareType22Info(),
        '23': SquareType23Info(),
        '24': SquareType24Info(),
        '35': SquareType35Info(),
        '36': SquareType36Info()}
    return square_number_dict


def get_square_degree_dict():
    square_degree_dict = {'01': BaseSquareThreshold.square_degree_one,
                          '02': BaseSquareThreshold.square_degree_one,
                          '03': BaseSquareThreshold.square_degree_one,
                          '04': BaseSquareThreshold.square_degree_two,
                          '05': BaseSquareThreshold.square_degree_two,
                          '06': BaseSquareThreshold.square_degree_two,
                          '07': BaseSquareThreshold.square_degree_two,
                          '08': BaseSquareThreshold.square_degree_two,
                          '09': BaseSquareThreshold.square_degree_two,
                          '15': BaseSquareThreshold.square_degree_two,
                          '17': BaseSquareThreshold.square_degree_two,
                          '27': BaseSquareThreshold.square_degree_two,
                          '28': BaseSquareThreshold.square_degree_two,
                          '29': BaseSquareThreshold.square_degree_two,
                          '30': BaseSquareThreshold.square_degree_two,
                          '31': BaseSquareThreshold.square_degree_two,
                          '32': BaseSquareThreshold.square_degree_two,
                          '33': BaseSquareThreshold.square_degree_two,
                          '34': BaseSquareThreshold.square_degree_two,
                          '42': BaseSquareThreshold.square_degree_two,
                          '10': BaseSquareThreshold.square_degree_three,
                          '14': BaseSquareThreshold.square_degree_three,
                          '16': BaseSquareThreshold.square_degree_three,
                          '18': BaseSquareThreshold.square_degree_three,
                          '19': BaseSquareThreshold.square_degree_three,
                          '20': BaseSquareThreshold.square_degree_three,
                          '25': BaseSquareThreshold.square_degree_three,
                          '26': BaseSquareThreshold.square_degree_three,
                          '37': BaseSquareThreshold.square_degree_three,
                          '38': BaseSquareThreshold.square_degree_three,
                          '39': BaseSquareThreshold.square_degree_three,
                          '40': BaseSquareThreshold.square_degree_three,
                          '11': BaseSquareThreshold.square_degree_four,
                          '12': BaseSquareThreshold.square_degree_four,
                          '13': BaseSquareThreshold.square_degree_four,
                          '21': BaseSquareThreshold.square_degree_four,
                          '22': BaseSquareThreshold.square_degree_four,
                          '23': BaseSquareThreshold.square_degree_four,
                          '24': BaseSquareThreshold.square_degree_four,
                          '35': BaseSquareThreshold.square_degree_four,
                          '36': BaseSquareThreshold.square_degree_four}
    return square_degree_dict


def get_all_square_base_list():
    temp_list = []
    temp01 = SquareType01Info()
    temp_list.append(temp01)
    temp02 = SquareType02Info()
    temp_list.append(temp02)
    temp03 = SquareType03Info()
    temp_list.append(temp03)
    temp04 = SquareType04Info()
    temp_list.append(temp04)
    temp05 = SquareType05Info()
    temp_list.append(temp05)
    temp06 = SquareType06Info()
    temp_list.append(temp06)
    temp07 = SquareType07Info()
    temp_list.append(temp07)
    temp08 = SquareType08Info()
    temp_list.append(temp08)
    temp09 = SquareType09Info()
    temp_list.append(temp09)
    temp10 = SquareType10Info()
    temp_list.append(temp10)
    temp11 = SquareType11Info()
    temp_list.append(temp11)
    temp12 = SquareType12Info()
    temp_list.append(temp12)
    temp13 = SquareType13Info()
    temp_list.append(temp13)
    temp14 = SquareType14Info()
    temp_list.append(temp14)
    temp15 = SquareType15Info()
    temp_list.append(temp15)
    temp16 = SquareType16Info()
    temp_list.append(temp16)
    temp17 = SquareType17Info()
    temp_list.append(temp17)
    temp18 = SquareType18Info()
    temp_list.append(temp18)
    temp19 = SquareType19Info()
    temp_list.append(temp19)
    temp20 = SquareType20Info()
    temp_list.append(temp20)
    temp21 = SquareType21Info()
    temp_list.append(temp21)
    temp22 = SquareType22Info()
    temp_list.append(temp22)
    temp23 = SquareType23Info()
    temp_list.append(temp23)
    temp24 = SquareType24Info()
    temp_list.append(temp24)
    temp25 = SquareType25Info()
    temp_list.append(temp25)
    temp26 = SquareType26Info()
    temp_list.append(temp26)
    temp27 = SquareType27Info()
    temp_list.append(temp27)
    temp28 = SquareType28Info()
    temp_list.append(temp28)
    temp29 = SquareType29Info()
    temp_list.append(temp29)
    temp30 = SquareType30Info()
    temp_list.append(temp30)
    temp31 = SquareType31Info()
    temp_list.append(temp31)
    temp32 = SquareType32Info()
    temp_list.append(temp32)
    temp33 = SquareType33Info()
    temp_list.append(temp33)
    temp34 = SquareType34Info()
    temp_list.append(temp34)
    temp35 = SquareType35Info()
    temp_list.append(temp35)
    temp36 = SquareType36Info()
    temp_list.append(temp36)
    temp37 = SquareType37Info()
    temp_list.append(temp37)
    temp38 = SquareType38Info()
    temp_list.append(temp38)
    temp39 = SquareType39Info()
    temp_list.append(temp39)
    temp40 = SquareType40Info()
    temp_list.append(temp40)
    temp41 = SquareType41Info()
    temp_list.append(temp41)
    temp42 = SquareType42Info()
    temp_list.append(temp42)
    return temp_list


target_line_length = target_width_length = 8
square_list = get_all_square_base_list()
target_square_number_dict = get_square_number_dict()


def get_list_tuple(origin_list):
    """
    [[],[]]转化为[(),()]
    :param origin_list: 原始列表
    :return: 转化后的列表
    """
    target_list = []
    for origin_list_sample in origin_list:
        target_list.append(tuple(origin_list_sample))
    return target_list


def fill_judge_dict(origin_fill_tuple, area_length_size, area_width_size, square_info: BaseSquareInfo, origin_i,
                    origin_j):
    """
    该函数作用是判断盘面位置origin_i、origin_j是否能放入方块square_info
    :param origin_fill_tuple:原始盘面，使用[(),()]记录，避免破坏原始盘面
    :param area_length_size:原始盘面长度
    :param area_width_size:原始盘面宽度
    :param square_info:方块信息
    :param origin_i:需要判断的位置x
    :param origin_j:需要判断的位置y
    :return:方块可放入标记、方块可消除行列数、是否能消除某区域
    """
    # origin_fill_list:可修改的盘面列表

    # 判断该位置和方块匹配后是否超出盘面范围
    if origin_i + square_info.block_length > area_length_size or origin_j + square_info.block_width - square_info.block_shift > area_width_size or origin_j - square_info.block_shift < 0:
        return False

    # 方块逐个点判断，是否可放入盘面
    for square_iter_i in range(square_info.block_length):
        for square_iter_j in range(len(square_info.block_list[square_iter_i])):
            if square_info.block_list[square_iter_i][square_iter_j] != square_info.square_block_sign:
                # 方块类的非填充块
                continue
            target_x = origin_i + square_iter_i
            target_y = origin_j + square_iter_j - square_info.block_shift
            if origin_fill_tuple[target_x][target_y] != square_info.square_empty_sign:
                # 盘面和方块有已放置的冲突
                # 该位置、该方块无法成功放入
                return False
    return True


def cool_judge_dict(area_info_area_list: list, square_info: BaseSquareInfo, origin_fill_set, origin_fill_position_dict):
    """
    该函数根据盘面状态和输入块内容，获取方块可填充的坐标x、y，可消除的行列数，是否能完整填入标记
    :param area_info_area_list: 具体盘面二位列表
    :param square_info: 具体方块
    :param origin_fill_set: 可填入方块集合
    :param origin_fill_position_dict: 可消除行列的方块字典，k记录方块编号，v存储位置列表
    :return: 没有返回值，origin_fill_set、origin_fill_position_dict
    """
    # 最优方案：设置为不可修改对象
    # 浅拷贝性能最快，循环赋值中等速度，深拷贝最慢
    fill_list_tuple = get_list_tuple(area_info_area_list)

    # 逐可填充点获取方块与盘面信息
    for iter_i in range(target_width_length):
        for iter_j in range(target_line_length):
            if area_info_area_list[iter_i][iter_j] == square_info.square_empty_sign:
                # 代表盘面当前位置为空
                fill_sign = fill_judge_dict(fill_list_tuple, target_line_length, target_width_length, square_info,
                                            iter_i, iter_j)
                if not fill_sign:
                    # 方块无法在该位置填充
                    continue
                # print(square_info.block_num, iter_i, iter_j, fill_num)
                origin_fill_set.add(square_info.block_num)
                if square_info.block_num not in origin_fill_position_dict:
                    origin_fill_position_dict[square_info.block_num] = [(iter_i, iter_j)]
                else:
                    origin_fill_position_dict[square_info.block_num].append((iter_i, iter_j))


def get_fill_all_info_dict(area_info_area_list: list):
    """
    逐个遍历方块和盘面关系
    :param area_info: 盘面类
    :return: 可填充方块及位置字典
    """
    fill_set = set()
    fill_position_dict = {}
    for square_list_sample in square_list:
        cool_judge_dict(area_info_area_list, square_list_sample, fill_set, fill_position_dict)
    return list(fill_set), fill_position_dict


def judge_simple_area_square_correct(origin_area_fill_set, square_num):
    """
    用于判断方块是否能放入当前盘面
    :param origin_area_fill_set: 盘面可放入方块集合
    :param square_num: 方块编号
    :return: 返回是否能放入标志位
    """
    if square_num in origin_area_fill_set:
        return True
    return False


def get_tuple_list(origin_list):
    """
    [(),()]转化为[[],[]]
    :param origin_list: 原始列表
    :return: 转化后的列表
    """
    target_list = []
    for origin_list_sample in origin_list:
        target_list.append(list(origin_list_sample))
    return target_list


def get_modify_area_list_old(origin_area_info_list_tuple: list, square_info: BaseSquareInfo, position_x, position_y):
    """
    根据方块逐个修改盘面状态
    :param origin_area_info_list_tuple: 盘面类二维列表
    :param square_info: 方块类信息
    :param position_x: 所需放入的坐标x
    :param position_y: 所需放入的坐标y
    :return: 修改后的盘面列表
    """
    # 复制盘面类数组信息，避免破坏原始数据
    origin_area_list = get_tuple_list(origin_area_info_list_tuple)

    # 拼接方块和盘面
    for square_iter_i in range(square_info.block_length):
        for square_iter_j in range(len(square_info.block_list[square_iter_i])):
            if square_info.block_list[square_iter_i][square_iter_j] != square_info.square_block_sign:
                # 方块类的非填充块
                continue
            origin_area_list[position_x + square_iter_i][
                position_y + square_iter_j - square_info.block_shift] = square_info.square_block_sign

    target_length_set = set()
    target_width_set = set()

    # 记录可以完成消除的行列
    # 该位置、该方块可以成功放入，判断行列数
    for fill_list_iter_i in range(position_x, position_x + square_info.block_length):
        # 循环遍历，判断行是否有已填满行
        if sum(origin_area_list[fill_list_iter_i]) == square_info.square_block_sign * target_width_length:
            target_length_set.add(fill_list_iter_i)
    for fill_list_iter_j in range(position_y - square_info.block_shift,
                                  position_y + square_info.block_width - square_info.block_shift):
        # 循环遍历，判断列是否有已填满列
        file_sign = True
        for origin_area_list_sample in origin_area_list:
            if origin_area_list_sample[fill_list_iter_j] == square_info.square_empty_sign:
                file_sign = False
                break
        if file_sign:
            target_width_set.add(fill_list_iter_j)

    # 可消除行列的数据消除
    for target_length_set_sample in target_length_set:
        origin_area_list[target_length_set_sample] = [square_info.square_empty_sign] * target_width_length
    for target_width_set_sample in target_width_set:
        for origin_area_list_sample in origin_area_list:
            origin_area_list_sample[target_width_set_sample] = square_info.square_empty_sign

    # 转化为和初始一致的列表，用于初始化盘面类
    return origin_area_list


def cool_judge_sample_sign(area_info_area_list: list, square_info: BaseSquareInfo):
    """
    该函数根据盘面状态和输入块内容，获取方块可填充的坐标x、y，可消除的行列数，是否能完整填入标记
    :param area_info_area_list: 具体盘面二维列表
    :param square_info: 具体方块
    :return: 返回能否填充的标记位
    """
    # 最优方案：设置为不可修改对象
    # 浅拷贝性能最快，循环赋值中等速度，深拷贝最慢
    fill_list_tuple = get_list_tuple(area_info_area_list)

    # 逐可填充点获取方块与盘面信息
    for iter_i in range(target_width_length):
        for iter_j in range(target_line_length):
            if area_info_area_list[iter_i][iter_j] == square_info.square_empty_sign:
                # 代表盘面当前位置为空
                fill_sign = fill_judge_dict(fill_list_tuple, target_line_length, target_width_length, square_info,
                                            iter_i, iter_j)
                if fill_sign:
                    # 方块无法在该位置填充
                    return True
    return False


def get_fill_all_info_dict_sample(area_info_area_list: list, origin_sample_number):
    """
    单编号方块和盘面关系
    :param area_info_area_list: 盘面类
    :param origin_sample_number: 所需判断的单个元素
    :return: 可填充方块及位置字典
    """
    fill_set = set()
    fill_position_dict = {}
    cool_judge_dict(area_info_area_list, target_square_number_dict[origin_sample_number], fill_set, fill_position_dict)
    return list(fill_set), fill_position_dict


def judge_area_double_sample_death(origin_area_list, origin_second_recommend_square: BaseSquareInfo,
                                   origin_third_recommend_square: BaseSquareInfo):
    """
    判断第二块和第三块中，是否能填充到第一块的图形中
    :param origin_area_list: 第一块已填充完的二维数组
    :param origin_second_recommend_square: 第二个推荐方块
    :param origin_third_recommend_square: 第三个推荐方块
    :return: 返回死亡的标记位
    """
    # 构建临时盘面类，并获取仅针对第二个推荐方块的爽块等列表
    # temp_area_info = BaseAreaInfo(origin_area_list)
    # temp_area_info.convert_list()
    origin_area_list_tuple = get_list_tuple(origin_area_list)
    fill_list, fill_position_dict = get_fill_all_info_dict_sample(origin_area_list,
                                                                  origin_second_recommend_square.block_num)
    fill_num = 0
    death_num = 0
    calculate_num = 0
    # 当第二个推荐方块可放入盘面时
    if fill_position_dict:
        # 逐个位置遍历盘面
        for fill_position_x, fill_position_y in fill_position_dict[origin_second_recommend_square.block_num]:
            # 获取第二个方块已确定的盘面列表
            calculate_num += 1
            second_fill_area_list = get_modify_area_list_old(origin_area_list_tuple, origin_second_recommend_square,
                                                             fill_position_x, fill_position_y)

            # 获取仅针对第三个推荐方块的爽块等列表
            second_sign = cool_judge_sample_sign(second_fill_area_list, origin_third_recommend_square)
            if second_sign:
                fill_num += 1
            else:
                death_num += 1
    return fill_num, death_num, calculate_num


def judge_area_triple_sample_death(origin_area_info_list: list, origin_fill_position_dict, origin_triple_list):
    """
    判定当前三个块是否会导致盘面致死
    :param origin_area_info_list: 盘面类
    :param origin_fill_position_dict: 方块可放置字典
    :param origin_triple_list: 推荐方块类列表
    :return: 返回当前盘面存活数和致死数
    """

    # 分割获取三个推荐块，其中第一个为主块，二、三为丛块
    first_recommend_square, second_recommend_square, third_recommend_square = origin_triple_list
    total_fill_num = 0
    total_death_num = 0
    calculate_num = 0
    origin_area_info_list_tuple = get_list_tuple(origin_area_info_list)

    # 遍历第一个块的可放置位置
    for position_x, position_y in origin_fill_position_dict[first_recommend_square.block_num]:
        # 根据第一个块的可放置位置及当前盘面，返回添加方块后的二维数组(与初始数组一致)
        first_fill_area_list = get_modify_area_list_old(origin_area_info_list_tuple, first_recommend_square, position_x,
                                                        position_y)

        # 在第一个块已经确定的情况下，判断二、三个块能否放入盘面
        first_fill_num, first_death_num, temp_calculate_num = judge_area_double_sample_death(first_fill_area_list,
                                                                                             second_recommend_square,
                                                                                             third_recommend_square)
        calculate_num += temp_calculate_num

        # 当第二个块和第三个块不相等时，交换位置判断
        if third_recommend_square.block_num != second_recommend_square.block_num:
            second_fill_num, second_death_num, temp_calculate_num = judge_area_double_sample_death(first_fill_area_list,
                                                                                                   third_recommend_square,
                                                                                                   second_recommend_square)
            calculate_num += temp_calculate_num
        else:
            second_fill_num, second_death_num = first_fill_num, first_death_num

        if first_fill_num == 0 and second_fill_num == 0:
            # 第一种和第二种都没有可放入路径时，只增加一次死亡次数
            temp_fill_num = 0
            temp_death_num = 1
        else:
            temp_fill_num = first_fill_num + second_fill_num
            temp_death_num = first_death_num + second_death_num
        total_fill_num += temp_fill_num
        total_death_num += temp_death_num

    return total_fill_num, total_death_num, calculate_num


def calculate_area_recommend_death_percent(origin_area_info: BaseAreaInfo, origin_old_recommend_list):
    """
    判断盘面和推荐列表的死亡概率
    :param origin_area_info: 盘面类
    :param origin_old_recommend_list: 方块推荐列表
    :return: 返回盘面和推荐列表死亡概率
    """
    # try:
    # 获取get_fill_all_info_dict中的盘面可填充方块
    # fill_set为盘面可放置块集合，用于加快in的判断
    # config_square_dict为预设方块类，k为方块编号，v为编号对应的方块类
    temp_num = 0
    death_num = 0
    fill_list, fill_position_dict = get_fill_all_info_dict(origin_area_info.area_list)
    fill_set = set(fill_list)
    config_square_dict = get_square_number_dict()
    origin_recommend_list = []
    for origin_old_recommend_list_sample in origin_old_recommend_list:
        if int(origin_old_recommend_list_sample) < 10 and str(origin_old_recommend_list_sample)[0] != '0':
            origin_recommend_list.append('0{}'.format(str(origin_old_recommend_list_sample)))
        else:
            origin_recommend_list.append(str(origin_old_recommend_list_sample))

    # 根据推荐列表和可放置列表，设置可放入的推荐方块列表
    origin_recommend_fill_list = []
    for origin_recommend_list_sample in origin_recommend_list:
        if judge_simple_area_square_correct(fill_set, origin_recommend_list_sample):
            origin_recommend_fill_list.append(origin_recommend_list_sample)

    # 当前推荐列表中没有可放置方块，则直接结束
    if not origin_recommend_fill_list:
        # 没有可放入方块
        return 100, temp_num, death_num

    # 设置已判断的方块列表，用于跳过重复方块判定
    calculate_recommend_dict = {}
    calculate_recommend_duplicate_list = []
    for origin_recommend_fill_list_sample in origin_recommend_fill_list:
        # 判断推荐块和盘面直接死亡时的优化条件
        # if origin_recommend_fill_list_sample in judge_recommend_set:
        #     # 避免如['03', '03', '05']重复判断03两次
        #     continue
        # judge_recommend_set.add(origin_recommend_fill_list_sample)

        # 从可放入的推荐方块开始，构建临时方块类列表
        temp_triple_list = [config_square_dict[origin_recommend_fill_list_sample]]
        temp_sign = False
        for origin_recommend_list_sample in origin_recommend_list:
            if origin_recommend_list_sample != origin_recommend_fill_list_sample:
                # 编号和当前第一块不一致时，直接加入即可
                temp_triple_list.append(config_square_dict[origin_recommend_list_sample])
            else:
                if temp_sign:
                    # 第二次遇见第一块编号，添加即可
                    temp_triple_list.append(config_square_dict[origin_recommend_list_sample])
                else:
                    # 第一次遇见第一块编号，直接跳过
                    temp_sign = True
        target_recommend_str = ''
        for temp_triple_list_sample in temp_triple_list:
            target_recommend_str += temp_triple_list_sample.block_num
        if target_recommend_str in calculate_recommend_dict:
            calculate_recommend_duplicate_list.append(target_recommend_str)
        # 根据盘面信息、方块可放置字典、临时方块类列表，判断当前推荐列表是否导致盘面致死
        else:
            temp_fill_num, temp_death_num, temp_calculate_num = judge_area_triple_sample_death(
                origin_area_info.area_list, fill_position_dict, temp_triple_list)
            temp_num += temp_calculate_num
            death_num += temp_death_num
            calculate_recommend_dict[target_recommend_str] = tuple([temp_fill_num, temp_death_num])
    total_fill_num = 0
    total_death_num = 0
    for calculate_recommend_dict_key, calculate_recommend_dict_value in calculate_recommend_dict.items():
        total_fill_num += calculate_recommend_dict_value[0]
        total_death_num += calculate_recommend_dict_value[1]
    for calculate_recommend_duplicate_list_sample in calculate_recommend_duplicate_list:
        total_fill_num += calculate_recommend_dict[calculate_recommend_duplicate_list_sample][0]
        total_death_num += calculate_recommend_dict[calculate_recommend_duplicate_list_sample][1]
    return round(total_death_num / (total_death_num + total_fill_num) * 100,
                 4), total_death_num + total_fill_num, total_death_num


def get_death_percent(origin_str, origin_block_list):
    try:
        origin_list = eval(origin_str)
        convert_block_list = eval(origin_block_list)
        str_block_list = [str(c) if len(str(c)) >= 2 else '0{}'.format(c) for c in convert_block_list]
        round_area_info = BaseAreaInfo(origin_list)
        round_area_info.get_area_info_and_corner_list()
        calculate_death_percent, calculate_total_num, calculate_death_num = calculate_area_recommend_death_percent(
            round_area_info, str_block_list)
        calculate_revive_num = calculate_total_num - calculate_death_num
        return calculate_death_percent, float(calculate_death_num), float(calculate_revive_num)
    except:
        return -1.0, -1.0, -1.0


from pyspark.sql.types import DoubleType, StructType, StructField
from datetime import datetime


if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("block_blast_gp_block_action_round_di").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]
    schema = StructType([
        StructField("calculate_death_percent", DoubleType(), True),
        StructField("calculate_death_num", DoubleType(), True),
        StructField("calculate_revive_num", DoubleType(), True)
    ])
    spark.udf.register('get_death_percent', get_death_percent, schema)
    spark.udf.register('get_pos_step_info', get_pos_step_info)

    F_DT = datetime.strptime(DT, "%Y-%m-%d").strftime("%Y%m%d")
    now = datetime.now()
    current_time_str = now.strftime("%H%M%S")
    print(current_time_str)
    tmp_tbl_name = f"""temp_dws_block_blast_gp_block_action_round_di_pre_{F_DT}"""

    print(tmp_tbl_name)

    spark.sql('''set hive.exec.dynamic.partition.mode=nonstrict;''')
    exec_sql = '''
                  select 
                   device_id,
                   app_id,
                   install_datetime,
                   os,
                   ip,
                   country,
                   city,
                   distinct_id,
                   event_name,
                   min_event_timestamp, -- 轮开始时间戳 
                   min_event_time, -- 轮开始时间
                   max_event_timestamp,
                   max_event_datetime, -- 轮最后落块时间
                   game_id,
                   game_type,
                   round_id,
                   travel_id,
                   travel_lv,
                    matrix, -- 当前排面
                   clear_screen_cnt, -- 清屏次数
                   blast_cnt,--消除次数
                   blast_row_col_cnt, -- 消除行列数
                   is_combo_status_cnt, -- 在combo状态的步数
                   common_block_cnt, -- 块基础得分
                   round_score, -- 单轮得分
                   time_diff_in_seconds, -- 单轮耗时
                   matrix_complex_value,  -- 轮初始排面复杂度
                   rec_strategy,  -- 轮初始理论出块策略
                   rec_strategy_fact , -- 轮初始实际出块策略
                   index_pos_list, -- 轮内出块位置信息

                   block_step_cnt, -- 步数
       	           game_combo_cnt, -- 段数
       	           game_combo_step_cnt, -- combo 总长度
       	           game_combo_block_step_cnt, --combo内消除步数

                   multiple_block_step_cnt, -- 多消步数
                   game_combo_block_step_cnt_rate, -- combo消除比例 一局，在combo内消除的次数/combo总长度
                   game_combo_step_cnt_rate, -- combo浓度 一局，combo总长度/摆放块次数
                   avg_game_combo_step_cnt,-- combo平均段长度 一局，combo总长度/combo段数
                   block_step_cnt_rate,-- 消除比例 一局，消除次数/摆放块次数
                   multiple_block_step_cnt_rate,-- 多消比例
                   death_percent , -- 死亡率 取轮初始牌面和初始出块
                   death_percent_death_num , -- 死亡率 取轮初始牌面和初始出块
                   death_percent_revive_num , -- 死亡率 取轮初始牌面和初始出块 
                   block_list , -- 轮初始 预备方块列表
                   -- 阳角率
                   -- 阴角率
                   time_action_in_seconds, -- 单轮耗时
                   time_think_in_seconds, -- 单轮耗时
                   gain_score,
                   gain_score_per_done
                   ,clean_cnt
                   ,clean_cnt_avg
                   ,is_clean_screen
                   ,weight_avg
                   ,put_rate_avg
                   ,accumulate_clean_times
                   ,accumulate_clean_cnt
                   ,app_version
                   ,clean_screen_cnt
                   ,is_final_round
                   ,combo_cnt
                   ,revive_cnt 
                   ,color_list
                   ,network_type
                   ,game_way_type
                   ,user_waynum
                   ,active_ab_game_way_type
                   ,nowwayarr
                   ,latest_now_way
                   ,weight_list
                   ,latest_weight
                   ,session_id
                    ,combo5_cnt
                    ,combo10_cnt
                    ,combo15_cnt
                    ,common_block_cnt_list
                    ,fact_line_list
                    ,total_line_list
                    ,max_blast_row_col_cnt
                    ,block_index_id_list
                    ,design_position_list
                    ,position_list
                    ,first_block_time
                    ,revive_show_cnt 
                    ,revive_click_cnt
                    ,cost_time
                    ,down_reason
                   ,dt
         from           
       (
       select 
        device_id,
                   app_id,
                   install_datetime,
                   os,
                   ip,
                   country,
                   city,
                   distinct_id,
                   event_name,
                   min_event_timestamp, -- 轮开始时间戳 
                   min_event_time, -- 轮开始时间
                   max_event_timestamp,
                   max_event_datetime, -- 轮最后落块时间
                   game_id,
                   game_type,
                   round_id,
                   travel_id,
                   travel_lv,
                    matrix, -- 当前排面
                   clear_screen_cnt, -- 清屏次数
                   blast_cnt,--消除次数
                   blast_row_col_cnt, -- 消除行列数
                   is_combo_status_cnt, -- 在combo状态的步数
                   common_block_cnt, -- 块基础得分
                   round_score, -- 单轮得分
                   time_diff_in_seconds, -- 单轮耗时
                   matrix_complex_value,  -- 轮初始排面复杂度
                   rec_strategy,  -- 轮初始理论出块策略
                   rec_strategy_fact , -- 轮初始实际出块策略
                   index_pos_list, -- 轮内出块位置信息

                   block_step_cnt, -- 步数
       	           game_combo_cnt, -- 段数
       	           game_combo_step_cnt, -- combo 总长度
       	           game_combo_block_step_cnt, --combo内消除步数

                   multiple_block_step_cnt, -- 多消步数
                   game_combo_block_step_cnt_rate, -- combo消除比例 一局，在combo内消除的次数/combo总长度
                   game_combo_step_cnt_rate, -- combo浓度 一局，combo总长度/摆放块次数
                   avg_game_combo_step_cnt,-- combo平均段长度 一局，combo总长度/combo段数
                   block_step_cnt_rate,-- 消除比例 一局，消除次数/摆放块次数
                   multiple_block_step_cnt_rate,-- 多消比例
                   death_percent , -- 死亡率 取轮初始牌面和初始出块
                   death_percent_death_num , -- 死亡率 取轮初始牌面和初始出块
                   death_percent_revive_num , -- 死亡率 取轮初始牌面和初始出块 
                   block_list , -- 轮初始 预备方块列表
                   time_action_in_seconds, -- 单轮耗时
                   time_think_in_seconds, -- 单轮耗时
                   gain_score,
                   gain_score_per_done
                   ,clean_cnt
                   ,clean_cnt_avg
                   ,is_clean_screen
                   ,weight_avg
                   ,put_rate_avg
                   ,accumulate_clean_times
                   ,accumulate_clean_cnt
                   ,app_version
                   ,clean_screen_cnt
                   ,is_final_round
                   ,combo_cnt
                   ,color_list
                   ,network_type
                   ,cast(revive_cnt as int) as revive_cnt 
                   ,color_list
                   ,cast(game_way_type as int) as game_way_type
                   ,user_waynum
                   ,cast(active_ab_game_way_type as int) as active_ab_game_way_type
                   ,nowwayarr
                   ,latest_now_way
                   ,weight_list
                   ,latest_weight
                   ,session_id
                   ,combo5_cnt
                   ,combo10_cnt
                   ,combo15_cnt
                   ,common_block_cnt_list
                   ,fact_line_list
                   ,total_line_list
                   ,max_blast_row_col_cnt
                   ,block_index_id_list
                   ,design_position_list
                   ,position_list
                   ,first_block_time
                   ,revive_show_cnt 
                   ,revive_click_cnt
                   ,cost_time
                   ,down_reason
                   ,dt 
        from default.{tmp_tbl_name}) a 
            '''.format(tmp_tbl_name=tmp_tbl_name, DT=DT)
    print(exec_sql)
    df = spark.sql(exec_sql)
    df.createOrReplaceTempView("source_data")
    last_round_df = spark.sql(f"""
                      select 
                      distinct_id as solution_distinct_id
                      ,game_id as solution_game_id
                      ,game_type as solution_game_type
                      ,round_id as solution_round_id
                      ,matrix
                      ,block_list from source_data where (is_final_round = true  or revive_cnt >= 1) and game_type ='0'
              """)

    schema = StructType([StructField("result", IntegerType(), True), StructField("time_taken", FloatType(), True)])

    process_solution_udf = udf(lambda x, y, z: process_solution(x, y, z), schema)
    spark.udf.register("process_solution_udf", process_solution_udf)
    process_valid_udf = udf(lambda x, y: process_valid(x, y), IntegerType())
    spark.udf.register("process_valid_udf", process_valid_udf)

    last_round_df = last_round_df.repartition(2000)

    last_round_df = last_round_df.withColumn("is_valid", process_valid_udf(col("matrix"), col("block_list")))
    last_round_df = last_round_df.withColumn("solution_withtime",process_solution_udf(col("matrix"), col("block_list"), col("is_valid")))
    last_round_df = last_round_df.withColumn("is_solution", last_round_df["solution_withtime"]["result"]).withColumn(
        "solution_calc_time", last_round_df["solution_withtime"]["time_taken"])
    last_round_df.createOrReplaceTempView("solution")

    spark.sql(f"""
            insert overwrite table hungry_studio.dws_block_blast_gp_block_action_round_di
            select 
            device_id,
            app_id,
            install_datetime,
            os,
            ip,
            country,
            city,
            distinct_id,
            event_name,
            min_event_timestamp, -- 轮开始时间戳 
            min_event_time, -- 轮开始时间
            max_event_timestamp,
            max_event_datetime, -- 轮最后落块时间
            game_id,
            game_type,
            round_id,
            travel_id,
            travel_lv,
             a.matrix, -- 当前排面
            clear_screen_cnt, -- 清屏次数
            blast_cnt,--消除次数
            blast_row_col_cnt, -- 消除行列数
            is_combo_status_cnt, -- 在combo状态的步数
            common_block_cnt, -- 块基础得分
            round_score, -- 单轮得分
            time_diff_in_seconds, -- 单轮耗时
            matrix_complex_value,  -- 轮初始排面复杂度
            rec_strategy,  -- 轮初始理论出块策略
            rec_strategy_fact , -- 轮初始实际出块策略
            index_pos_list, -- 轮内出块位置信息

            block_step_cnt, -- 步数
	        game_combo_cnt, -- 段数
	        game_combo_step_cnt, -- combo 总长度
	        game_combo_block_step_cnt, --combo内消除步数

            multiple_block_step_cnt, -- 多消步数
            game_combo_block_step_cnt_rate, -- combo消除比例 一局，在combo内消除的次数/combo总长度
            game_combo_step_cnt_rate, -- combo浓度 一局，combo总长度/摆放块次数
            avg_game_combo_step_cnt,-- combo平均段长度 一局，combo总长度/combo段数
            block_step_cnt_rate,-- 消除比例 一局，消除次数/摆放块次数
            multiple_block_step_cnt_rate,-- 多消比例
            death_percent , -- 死亡率 取轮初始牌面和初始出块
            death_percent_death_num , -- 死亡率 取轮初始牌面和初始出块
            death_percent_revive_num , -- 死亡率 取轮初始牌面和初始出块 
            a.block_list , -- 轮初始 预备方块列表
            -- 阳角率
            -- 阴角率
            time_action_in_seconds, -- 单轮耗时
            time_think_in_seconds, -- 单轮耗时
            gain_score,
            gain_score_per_done
            ,clean_cnt
            ,clean_cnt_avg
            ,is_clean_screen
            ,weight_avg
            ,put_rate_avg
            ,accumulate_clean_times
            ,accumulate_clean_cnt
            ,app_version
            ,clean_screen_cnt
            ,is_final_round
            ,combo_cnt
            ,revive_cnt 
            ,color_list
            ,b.is_valid
            ,b.is_solution
            ,network_type
            ,game_way_type
            ,user_waynum
            ,active_ab_game_way_type
            ,nowwayarr
            ,latest_now_way
            ,weight_list
            ,latest_weight
            ,session_id
            ,combo5_cnt
            ,combo10_cnt
            ,combo15_cnt
            ,common_block_cnt_list
            ,fact_line_list
            ,total_line_list
            ,max_blast_row_col_cnt
            ,block_index_id_list
            ,design_position_list
            ,position_list
            ,first_block_time
            ,revive_show_cnt 
            ,revive_click_cnt
            ,cost_time
            ,down_reason
            ,dt
      from  source_data a left join solution b
            on a.distinct_id = b.solution_distinct_id 
            and a.game_id = b.solution_game_id 
            and a.round_id = b.solution_round_id
            and a.game_type = b.solution_game_type
            """)
    print('写入完成。。。。。')
    # 关闭 Spark 会话
    spark.stop()
