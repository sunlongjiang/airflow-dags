from pyspark.sql import SparkSession
import sys
import math
from collections import Counter

import copy
import json
import traceback

import sys

"""
CREATE EXTERNAL TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di(
  `device_id` string COMMENT '设备id',
  `app_id` string COMMENT '应用id',
  `install_datetime` timestamp COMMENT '安装时间',
  `os` string COMMENT '操作系统',
  `ip` string COMMENT 'ip',
  `country` string COMMENT '国家',
  `city` string COMMENT '城市',
  `uuid` string COMMENT '唯一键',
  `distinct_id` string COMMENT '用户id',
  `event_name` string COMMENT '事件名称',
  `event_timestamp` bigint COMMENT '事件上报时间戳',
  `event_datetime` timestamp COMMENT '事件上报时间',
  `game_id` string COMMENT '游戏-局id' ,
  `game_type` string COMMENT '游戏模式（0无尽 2关卡 6土耳其方块 10谜题 99新手引导）',
  `round_id` string COMMENT '局内累计轮数',
  `travel_id` string COMMENT '期数',
  `travel_lv` string COMMENT '该期关卡号',
  `matrix` string COMMENT '当前盘面数据-落块时',
  `position` string COMMENT '松开时对应的盘面坐标，左上角对齐',
  `clean` string COMMENT '消除信息（数字对应列，字母对应行）',
  `block_id` string COMMENT '块id',
  `index_id` string COMMENT '点击块在预备块中索引位置  0左1中2右',
  `rec_strategy` string COMMENT '理论推荐策略',
  `rec_strategy_fact` string COMMENT '实际推荐策略',
  `combo_cnt` string COMMENT '第几次连击',
  `gain_score` string COMMENT '获得分数',
  `gain_item` string COMMENT '获得物品。旅行模式中收集关卡记录。物品id：物品数量',
  `block_list` string COMMENT '预备方块列表',
  `lag_event_timestamp` bigint COMMENT '上一次落块（或开局）上报时间',
  `time_diff_in_seconds` double COMMENT '本次落块耗时',
rec_strategy_fact_most string comment '实际推荐策略-出现次数最多',
lag_matrix string comment '出块前排面信息',
is_clear_screen boolean comment '是否清屏',
is_blast boolean comment '是否存在消除',
blast_row_col_cnt bigint comment '消除行列数',
is_combo_status boolean comment '是否处于combo状态',
common_block_cnt int comment 'block属性信息-块数',
step_score int comment '单次得分',
block_index_id int comment '轮内放块顺序',
matrix_complex_value int comment '出块前排面复杂度',
block_line_percent  double COMMENT '落块贴边率',
corner_outside_percent double COMMENT '阳角率',
corner_inside_percent double COMMENT '阴角率'
)
  PARTITIONED by(dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://hungry-studio-data-warehouse/dwd/block_blast/gp/block_action_block_di'
  TBLPROPERTIES (
  'classification'='parquet')


ALTER TABLE hungry_studio.dwd_block_blast_ios_block_action_block_di ADD COLUMNS (time_action_in_seconds double COMMENT '落块-动作时间');
ALTER TABLE hungry_studio.dwd_block_blast_ios_block_action_block_di ADD COLUMNS (time_think_in_seconds double COMMENT '落块-思考时间');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (time_action_in_seconds double COMMENT '落块-动作时间');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (time_think_in_seconds double COMMENT '落块-思考时间');

-- add by zhaodawei at 2024.07.31
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di CHANGE COLUMN step_score step_score int COMMENT '单次得分【废弃】';
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di CHANGE COLUMN gain_score gain_score string COMMENT '本局截止到该块的累计得分';
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (gain_score_per_done int COMMENT '单块得分');

-- 20240826 liubing
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS ( cur_matrix_complex_value string comment '出块后的排面复杂度' )


-- 20240826  zhaodawei  

ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (max_round_id int COMMENT '单局最大轮数');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (is_final_round boolean COMMENT '此轮是不是最后一轮');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (is_lethal_block boolean COMMENT '是不是致死块');

ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (last_click_time bigint COMMENT '最后一次点击块的时间点');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (is_clean_screen int COMMENT '是否清屏（1=是，0=否）');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (weight float COMMENT '放置后盘面权重');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (put_rate float COMMENT '放置后贴边率');

ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (clean_times int COMMENT '消除次数');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (clean_cnt int COMMENT '消除行数');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (accumulate_clean_times int COMMENT '累计消除次数');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (accumulate_clean_cnt int COMMENT '累计消除行数');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (app_version string COMMENT 'app版本');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (ram string COMMENT '内存');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (disk string COMMENT '磁盘');



-- modified at 2024-10-08 by sunlj

ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (is_sdk_sample int comment '是否抽样');
ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (block_down_color int comment '块颜色');


ALTER TABLE hungry_studio.dwd_block_blast_gp_block_action_block_di ADD COLUMNS (network_type string comment '网络类型');
"""

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


def block_line_calculate(origin_square_info: BaseSquareInfo, origin_fill_list, temp_column_list, square_row_size,
                         square_column_size, temp_square_info_list_iter_i, temp_square_info_list_iter_j, target_x,
                         target_y, hard_line_sign, soft_line_sign, empty_line_sign):
    """
    方块贴边计算
    各边统计时，均为单类判定，即边为贴硬边/贴软边/不贴边/不形成边的一种
    :param origin_square_info: 方块类
    :param origin_fill_list: 原始盘面二维列表
    :param temp_column_list: 计数列表，用于统计方块边的贴合类别
    :param square_row_size: 方块行数
    :param square_column_size: 方块列数
    :param temp_square_info_list_iter_i: 当前判定块的块内行坐标
    :param temp_square_info_list_iter_j: 当前判定块的块内列坐标
    :param target_x: 当前判定块的盘面行坐标
    :param target_y: 当前判定块的盘面列坐标
    :param hard_line_sign: 硬边标记
    :param soft_line_sign: 软边标记
    :param empty_line_sign: 空边标记
    :return: 无返回值，通过逻辑和temp_column_list进行数据交互
    """
    if target_y == 0:
        # 左侧贴硬边
        temp_column_list.append(hard_line_sign)
    elif temp_square_info_list_iter_j != 0 and origin_square_info.block_list[temp_square_info_list_iter_i][
        temp_square_info_list_iter_j - 1] == origin_square_info.square_block_sign:
        # 左侧相连有实体
        pass
    elif origin_fill_list[target_x][target_y - 1] == origin_square_info.square_block_sign:
        # 左侧贴软边
        temp_column_list.append(soft_line_sign)
    else:
        temp_column_list.append(empty_line_sign)

    if target_x == 0:
        # 上侧贴硬边
        temp_column_list.append(hard_line_sign)
    elif temp_square_info_list_iter_i != 0 and len(
            origin_square_info.block_list[temp_square_info_list_iter_i - 1]) > temp_square_info_list_iter_j and \
            origin_square_info.block_list[temp_square_info_list_iter_i - 1][
                temp_square_info_list_iter_j] == origin_square_info.square_block_sign:
        pass
    elif origin_fill_list[target_x - 1][target_y] == origin_square_info.square_block_sign:
        # 上侧贴软边
        temp_column_list.append(soft_line_sign)
    else:
        temp_column_list.append(empty_line_sign)

    if target_y == target_width_length - 1:
        # 右侧贴硬边
        temp_column_list.append(hard_line_sign)
    elif temp_square_info_list_iter_j != square_column_size - 1 and \
            origin_square_info.block_list[temp_square_info_list_iter_i][
                temp_square_info_list_iter_j + 1] == origin_square_info.square_block_sign:
        pass
    elif origin_fill_list[target_x][target_y + 1] == origin_square_info.square_block_sign:
        # 右侧贴软边
        temp_column_list.append(soft_line_sign)
    else:
        temp_column_list.append(empty_line_sign)

    if target_x == target_line_length - 1:
        # 下侧贴硬边
        temp_column_list.append(hard_line_sign)
    elif temp_square_info_list_iter_i != square_row_size - 1 and len(
            origin_square_info.block_list[temp_square_info_list_iter_i + 1]) > temp_square_info_list_iter_j and \
            origin_square_info.block_list[temp_square_info_list_iter_i + 1][
                temp_square_info_list_iter_j] == origin_square_info.square_block_sign:
        pass
    elif origin_fill_list[target_x + 1][target_y] == origin_square_info.square_block_sign:
        # 下侧贴软边
        temp_column_list.append(soft_line_sign)
    else:
        temp_column_list.append(empty_line_sign)


def block_corner_inside_calculate(origin_square_info: BaseSquareInfo, origin_fill_list, inside_corner_list,
                                  square_row_size, square_column_size, temp_square_info_list_iter_i,
                                  temp_square_info_list_iter_j, target_x, target_y, corner_eliminate_sign,
                                  corner_not_eliminate_sign):
    """
    方块阴角消除计算
    各拐角统计时，均为单类判定，即角为消除/未消除之一
    :param origin_square_info: 方块类
    :param origin_fill_list: 原始盘面二维列表
    :param inside_corner_list: 计数列表，用于统计方块阴角消除的类别
    :param square_row_size: 方块行数
    :param square_column_size: 方块列数
    :param temp_square_info_list_iter_i: 当前判定块的块内行坐标
    :param temp_square_info_list_iter_j: 当前判定块的块内列坐标
    :param target_x: 当前判定块的盘面行坐标
    :param target_y: 当前判定块的盘面列坐标
    :param corner_eliminate_sign: 拐角消除标记
    :param corner_not_eliminate_sign: 拐角未消除标记
    :return: 无返回值，通过逻辑和inside_corner_list进行数据交互
    """
    if temp_square_info_list_iter_i == square_row_size - 1:
        # 最后一行，无需检查，写在这儿方便核验逻辑
        return
    if temp_square_info_list_iter_j != 0:
        # 当不是顶格
        if len(origin_square_info.block_list[temp_square_info_list_iter_i + 1]) > temp_square_info_list_iter_j - 1 and \
                origin_square_info.block_list[temp_square_info_list_iter_i + 1][
                    temp_square_info_list_iter_j - 1] == origin_square_info.square_block_sign:
            # 判断左下角是否有块
            if origin_square_info.block_list[temp_square_info_list_iter_i][
                temp_square_info_list_iter_j - 1] == origin_square_info.square_empty_sign:
                # 左侧为空，存在内角
                if origin_fill_list[target_x][target_y - 1] == origin_square_info.square_block_sign:
                    inside_corner_list.append(corner_eliminate_sign)
                else:
                    inside_corner_list.append(corner_not_eliminate_sign)
            if len(origin_square_info.block_list[temp_square_info_list_iter_i + 1]) <= temp_square_info_list_iter_j or \
                    origin_square_info.block_list[temp_square_info_list_iter_i + 1][
                        temp_square_info_list_iter_j] == origin_square_info.square_empty_sign:
                # 下侧为空，存在内角
                if origin_fill_list[target_x + 1][target_y] == origin_square_info.square_block_sign:
                    inside_corner_list.append(corner_eliminate_sign)
                else:
                    inside_corner_list.append(corner_not_eliminate_sign)

    if temp_square_info_list_iter_j != origin_square_info.block_width - 1:
        # 当不是最后一列
        if len(origin_square_info.block_list[temp_square_info_list_iter_i + 1]) > temp_square_info_list_iter_j + 1 and \
                origin_square_info.block_list[temp_square_info_list_iter_i + 1][
                    temp_square_info_list_iter_j + 1] == origin_square_info.square_block_sign:
            # 右下角有块
            if temp_square_info_list_iter_j == square_column_size - 1 or \
                    origin_square_info.block_list[temp_square_info_list_iter_i][
                        temp_square_info_list_iter_j + 1] == origin_square_info.square_empty_sign:
                # 右侧为空
                if origin_fill_list[target_x][target_y + 1] == origin_square_info.square_block_sign:
                    inside_corner_list.append(corner_eliminate_sign)
                else:
                    inside_corner_list.append(corner_not_eliminate_sign)
            if len(origin_square_info.block_list[temp_square_info_list_iter_i + 1]) <= temp_square_info_list_iter_j or \
                    origin_square_info.block_list[temp_square_info_list_iter_i + 1][
                        temp_square_info_list_iter_j] == origin_square_info.square_empty_sign:
                # 下侧为空
                if origin_fill_list[target_x + 1][target_y] == origin_square_info.square_block_sign:
                    inside_corner_list.append(corner_eliminate_sign)
                else:
                    inside_corner_list.append(corner_not_eliminate_sign)


def block_corner_outside_calculate(origin_square_info: BaseSquareInfo, origin_fill_list, outside_corner_list,
                                   square_row_size, square_column_size, temp_square_info_list_iter_i,
                                   temp_square_info_list_iter_j, target_x, target_y, corner_eliminate_sign,
                                   corner_not_eliminate_sign):
    """
    方块阳角消除计算
    各拐角统计时，均为单类判定，即角为消除/未消除之一
    :param origin_square_info: 方块类
    :param origin_fill_list: 原始盘面二维列表
    :param outside_corner_list: 计数列表，用于统计方块阳角消除的类别
    :param square_row_size: 方块行数
    :param square_column_size: 方块列数
    :param temp_square_info_list_iter_i: 当前判定块的块内行坐标
    :param temp_square_info_list_iter_j: 当前判定块的块内列坐标
    :param target_x: 当前判定块的盘面行坐标
    :param target_y: 当前判定块的盘面列坐标
    :param corner_eliminate_sign: 拐角消除标记
    :param corner_not_eliminate_sign: 拐角未消除标记
    :return: 无返回值，通过逻辑和outside_corner_list进行数据交互
    """
    if temp_square_info_list_iter_i != 0 and len(
            origin_square_info.block_list[temp_square_info_list_iter_i - 1]) > temp_square_info_list_iter_j and \
            origin_square_info.block_list[temp_square_info_list_iter_i - 1][
                temp_square_info_list_iter_j] == origin_square_info.square_block_sign:
        # 判断内部上面是否有边相连
        pass
    elif temp_square_info_list_iter_j != 0 and origin_square_info.block_list[temp_square_info_list_iter_i][
        temp_square_info_list_iter_j - 1] == origin_square_info.square_block_sign:
        # 判断内部左边是否有边相连
        pass
    elif target_x == 0 or target_y == 0:
        # 判断左边或者上边是否有硬边相连
        outside_corner_list.append(corner_eliminate_sign)
    elif origin_fill_list[target_x - 1][target_y] == origin_square_info.square_block_sign or origin_fill_list[target_x][
        target_y - 1] == origin_square_info.square_block_sign:
        # 判断外界左边或者上边是否有边相连
        outside_corner_list.append(corner_eliminate_sign)
    else:
        outside_corner_list.append(corner_not_eliminate_sign)

    if temp_square_info_list_iter_i != 0 and len(
            origin_square_info.block_list[temp_square_info_list_iter_i - 1]) > temp_square_info_list_iter_j and \
            origin_square_info.block_list[temp_square_info_list_iter_i - 1][
                temp_square_info_list_iter_j] == origin_square_info.square_block_sign:
        # 判断内部上面是否有边相连
        pass
    elif temp_square_info_list_iter_j != square_column_size - 1 and \
            origin_square_info.block_list[temp_square_info_list_iter_i][
                temp_square_info_list_iter_j + 1] == origin_square_info.square_block_sign:
        # 判断内部左边是否有边相连
        pass
    elif target_x == 0 or target_y == target_width_length - 1:
        # 判断右边或者上边是否有硬边相连
        outside_corner_list.append(corner_eliminate_sign)
    elif origin_fill_list[target_x - 1][target_y] == origin_square_info.square_block_sign or origin_fill_list[target_x][
        target_y + 1] == origin_square_info.square_block_sign:
        # 判断外界左边或者上边是否有边相连
        outside_corner_list.append(corner_eliminate_sign)
    else:
        outside_corner_list.append(corner_not_eliminate_sign)

    if temp_square_info_list_iter_i != len(origin_square_info.block_list) - 1 and len(
            origin_square_info.block_list[temp_square_info_list_iter_i + 1]) > temp_square_info_list_iter_j and \
            origin_square_info.block_list[temp_square_info_list_iter_i + 1][
                temp_square_info_list_iter_j] == origin_square_info.square_block_sign:
        # 判断内部上面是否有边相连
        pass
    elif temp_square_info_list_iter_j != square_column_size - 1 and \
            origin_square_info.block_list[temp_square_info_list_iter_i][
                temp_square_info_list_iter_j + 1] == origin_square_info.square_block_sign:
        # 判断内部左边是否有边相连
        pass
    elif target_x == target_line_length - 1 or target_y == target_width_length - 1:
        # 判断右边或者上边是否有硬边相连
        outside_corner_list.append(corner_eliminate_sign)
    elif origin_fill_list[target_x + 1][target_y] == origin_square_info.square_block_sign or origin_fill_list[target_x][
        target_y + 1] == origin_square_info.square_block_sign:
        # 判断外界左边或者上边是否有边相连
        outside_corner_list.append(corner_eliminate_sign)
    else:
        outside_corner_list.append(corner_not_eliminate_sign)

    if temp_square_info_list_iter_i != len(origin_square_info.block_list) - 1 and len(
            origin_square_info.block_list[temp_square_info_list_iter_i + 1]) > temp_square_info_list_iter_j and \
            origin_square_info.block_list[temp_square_info_list_iter_i + 1][
                temp_square_info_list_iter_j] == origin_square_info.square_block_sign:
        # 判断内部上面是否有边相连
        pass
    elif temp_square_info_list_iter_j != 0 and origin_square_info.block_list[temp_square_info_list_iter_i][
        temp_square_info_list_iter_j - 1] == origin_square_info.square_block_sign:
        # 判断内部左边是否有边相连
        pass
    elif target_x == target_line_length - 1 or target_y == 0:
        # 判断右边或者上边是否有硬边相连
        outside_corner_list.append(corner_eliminate_sign)
    elif origin_fill_list[target_x + 1][target_y] == origin_square_info.square_block_sign or origin_fill_list[target_x][
        target_y - 1] == origin_square_info.square_block_sign:
        # 判断外界左边或者上边是否有边相连
        outside_corner_list.append(corner_eliminate_sign)
    else:
        outside_corner_list.append(corner_not_eliminate_sign)


def block_preference_calculate(origin_area_info_list_tuple: list, origin_square_info: BaseSquareInfo, position_x,
                               position_y):
    """
    方块偏好计算，逐步调用边、阴阳角消除函数
    :param origin_area_info_list_tuple: 原始盘面二维列表
    :param origin_square_info: 当前方块类别
    :param position_x: 当前盘面行坐标
    :param position_y: 当前盘面列坐标
    :return: 返回方块边、阴阳角偏好列表
    """
    origin_fill_list = get_tuple_list(origin_area_info_list_tuple)

    if position_x + origin_square_info.block_length > target_line_length or position_y + origin_square_info.block_width - origin_square_info.block_shift > target_width_length or position_y - origin_square_info.block_shift < 0:
        # 超出边界
        return False

    outside_corner_list = []
    outside_corner_list_length = 12
    inside_corner_list = []
    inside_corner_list_length = 4
    temp_column_list = []
    temp_column_list_length = 12

    empty_line_sign = 0
    soft_line_sign = 1
    hard_line_sign = 2
    line_not_exist_sign = -1

    corner_eliminate_sign = 1
    corner_not_eliminate_sign = 0
    corner_not_exist_sign = -1
    square_row_size = len(origin_square_info.block_list)
    for temp_square_info_list_iter_i in range(square_row_size):
        square_column_size = len(origin_square_info.block_list[temp_square_info_list_iter_i])
        for temp_square_info_list_iter_j in range(square_column_size):
            if origin_square_info.block_list[temp_square_info_list_iter_i][
                temp_square_info_list_iter_j] == origin_square_info.square_empty_sign:
                # 空块跳过
                continue
            target_x = position_x + temp_square_info_list_iter_i
            target_y = position_y + temp_square_info_list_iter_j - origin_square_info.block_shift
            block_line_calculate(origin_square_info, origin_fill_list, temp_column_list, square_row_size,
                                 square_column_size, temp_square_info_list_iter_i, temp_square_info_list_iter_j,
                                 target_x, target_y, hard_line_sign, soft_line_sign, empty_line_sign)
            block_corner_outside_calculate(origin_square_info, origin_fill_list, outside_corner_list, square_row_size,
                                           square_column_size, temp_square_info_list_iter_i,
                                           temp_square_info_list_iter_j, target_x, target_y, corner_eliminate_sign,
                                           corner_not_eliminate_sign)
            block_corner_inside_calculate(origin_square_info, origin_fill_list, inside_corner_list, square_row_size,
                                          square_column_size, temp_square_info_list_iter_i,
                                          temp_square_info_list_iter_j, target_x, target_y, corner_eliminate_sign,
                                          corner_not_eliminate_sign)
    while len(outside_corner_list) < outside_corner_list_length:
        outside_corner_list.append(corner_not_exist_sign)
    while len(inside_corner_list) < inside_corner_list_length:
        inside_corner_list.append(corner_not_exist_sign)
    while len(temp_column_list) < temp_column_list_length:
        # 边列表数据补充
        temp_column_list.append(line_not_exist_sign)
    return temp_column_list, outside_corner_list, inside_corner_list


def get_line_percent(origin_list, block_id, origin_x, origin_y):
    try:
        temp_dict = get_square_number_dict()
        if len(block_id) == 1:
            block_id = '0{}'.format(block_id)
        else:
            block_id = str(block_id)
        if block_id not in temp_dict:
            line_percent = -1.0
            corner_outside_percent = -1.0
            corner_inside_percent = -1.0
        temp_block = temp_dict[block_id]
        round_area_info = BaseAreaInfo(eval(str(origin_list)))
        round_area_info.get_area_info_and_corner_list()
        # 获取盘面复杂度
        line_preference_list, corner_outside_list, corner_inside_list = block_preference_calculate(
            round_area_info.area_list, temp_block, origin_x, origin_y)
        line_percent = len([c for c in line_preference_list if c > 0]) / len(
            [c for c in line_preference_list if c >= 0])
        corner_outside_percent = len([c for c in corner_outside_list if c > 0]) / len(
            [c for c in corner_outside_list if c >= 0])
        corner_inside_list_convert = [c for c in corner_inside_list if c >= 0]
        if corner_inside_list_convert:
            corner_inside_percent = len([c for c in corner_inside_list if c > 0]) / len(
                [c for c in corner_inside_list if c >= 0])
        else:
            corner_inside_percent = -1.0
    except Exception as e:
        line_percent = -1.0
        corner_outside_percent = -1.0
        corner_inside_percent = -1.0
    return line_percent, corner_outside_percent, corner_inside_percent


target_square_number_dict = get_square_number_dict()
square_type_11_info = target_square_number_dict['11']
square_type_13_info = target_square_number_dict['13']
square_type_22_info = target_square_number_dict['22']


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


def get_area_value(area_info_area_list: list):
    """
    计算盘面复杂度
    :param area_info_area_list: 盘面二维列表
    :return: 复杂度计算结果
    """
    gong_num = 0
    row_num = 0
    col_num = 0
    col_weight_value = 0
    row_weight_value = 0
    empty_num = 0

    origin_square_empty_sign = square_type_11_info.square_empty_sign

    origin_fill_tuple = get_list_tuple(area_info_area_list)
    # 逐可填充点获取方块与盘面信息
    for iter_i in range(target_width_length):

        row_line_value = 0
        row_fill_value = 0
        for iter_j in range(target_line_length):
            if area_info_area_list[iter_i][iter_j] != origin_square_empty_sign:
                row_fill_value += 1
            else:
                empty_num += 1
                if fill_judge_dict(origin_fill_tuple, target_width_length, target_line_length, square_type_11_info,
                                   iter_i, iter_j):
                    row_num += 1
                if fill_judge_dict(origin_fill_tuple, target_width_length, target_line_length, square_type_13_info,
                                   iter_i, iter_j):
                    gong_num += 1
                if fill_judge_dict(origin_fill_tuple, target_width_length, target_line_length, square_type_22_info,
                                   iter_i, iter_j):
                    col_num += 1

                if iter_j == 0:
                    if area_info_area_list[iter_i][iter_j + 1] != origin_square_empty_sign:
                        row_line_value += 1
                elif iter_j == target_width_length - 1:
                    if area_info_area_list[iter_i][iter_j - 1] != origin_square_empty_sign:
                        row_line_value += 1
                else:
                    if area_info_area_list[iter_i][iter_j + 1] != origin_square_empty_sign:
                        row_line_value += 1
                    if area_info_area_list[iter_i][iter_j - 1] != origin_square_empty_sign:
                        row_line_value += 1
        row_weight_value += row_fill_value * row_line_value

    for iter_i in range(target_width_length):
        line_value = 0
        fill_value = 0
        for iter_j in range(target_line_length):
            if area_info_area_list[iter_j][iter_i] != origin_square_empty_sign:
                fill_value += 1
            else:
                if iter_j == 0:
                    if area_info_area_list[iter_j + 1][iter_i] != origin_square_empty_sign:
                        line_value += 1
                elif iter_j == target_width_length - 1:
                    if area_info_area_list[iter_j - 1][iter_i] != origin_square_empty_sign:
                        line_value += 1
                else:
                    if area_info_area_list[iter_j + 1][iter_i] != origin_square_empty_sign:
                        line_value += 1
                    if area_info_area_list[iter_j - 1][iter_i] != origin_square_empty_sign:
                        line_value += 1

        col_weight_value += fill_value * line_value
    base_weight_value = col_weight_value + row_weight_value
    if gong_num >= 3:
        clear_weight_value = gong_num + 20
    else:
        clear_weight_value = gong_num * 2 + row_num / 3 + col_num / 3 + 16
    temp_empty_value = (empty_num - 32) * (empty_num - 32)
    temp_weight_value = base_weight_value - clear_weight_value
    if empty_num >= 32:
        strength_weight_value = temp_weight_value - temp_empty_value / 5
    else:
        if gong_num > 0:
            strength_weight_value = temp_weight_value + temp_empty_value / 5
        else:
            strength_weight_value = temp_weight_value + temp_empty_value / 2
    # print(temp_weight_value, temp_empty_value)
    return math.floor(strength_weight_value + 246)


def get_area_complex_value(origin_list):
    try:
        round_area_info = BaseAreaInfo(eval(str(origin_list)))
        round_area_info.get_area_info_and_corner_list()
        # # # # 获取盘面复杂度
        usr_complex_value = get_area_value(round_area_info.area_list)
    except Exception as e:
        usr_complex_value = -1.0
    return usr_complex_value



def most_common_element(lst):
    # 使用 Counter 统计元素出现次数
    counter = Counter(lst)

    # 使用 most_common(1) 获取出现次数最多的元素
    most_common = counter.most_common(1)

    if most_common:
        return most_common[0][0]
    else:
        return None

import re
def is_clear_screen(matrix):
    if matrix is None:
        return False
    cleaned_matrix = matrix.replace("-1", "")
    return bool(re.search(r'\d', cleaned_matrix))

from pyspark.sql.types import DoubleType, StructType, StructField ,BooleanType
from datetime import datetime

if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName(
        "dws_block_blast_gp_block_action_di_pre_test").enableHiveSupport().getOrCreate()

    DT = sys.argv[1]
    HOUR = sys.argv[2]
    print("""开始执行最新逻辑""")
    # 注册udf函数
    spark.udf.register('most_common_element', most_common_element)
    spark.udf.register('get_area_complex_value', get_area_complex_value)
    schema = StructType([
        StructField("line_percent", DoubleType(), True),
        StructField("corner_outside_percent", DoubleType(), True),
        StructField("corner_inside_percent", DoubleType(), True)
    ])
    spark.udf.register('get_line_percent', get_line_percent, schema)
    spark.udf.register("is_clear_screen_udf", is_clear_screen, BooleanType())


    F_DT = datetime.strptime(DT, "%Y-%m-%d").strftime("%Y%m%d")
    now = datetime.now()
    current_time_str = now.strftime("%H%M%S")
    print(current_time_str)

    spark.sql('''set hive.exec.dynamic.partition.mode=nonstrict;''')
    tmp_tbl_name = f"""temp_dws_block_blast_gp_block_action_di_pre_test_0_{F_DT}_{HOUR}"""
    tbl_path = f"""temp_dws_block_blast_gp_block_action_di_pre_test_0_{F_DT}_{current_time_str}"""
    print(tmp_tbl_name)
    # 删除存在的临时表
    sql_delete_tmp_tbl = '''
        drop table if exists default.{tmp_tbl_name}
        '''.format(tmp_tbl_name=tmp_tbl_name)
    print(sql_delete_tmp_tbl)
    spark.sql(sql_delete_tmp_tbl).show()
    print('''执行删除存在的临时表 default.{tmp_tbl_name}完成'''.format(tmp_tbl_name=tmp_tbl_name))

    spark.sql('''
        CREATE TABLE default.{tmp_tbl_name}
        USING PARQUET
         location 's3://hungry-studio-tmp/{tbl_path}/'
    		select device_id
            ,app_id
            ,install_datetime
            ,os
            ,ip
            ,country_cn
            ,country 
            ,city_cn 
            ,city
            ,uuid
            ,distinct_id
            ,event_name
            ,event_timestamp
            ,event_datetime
            ,app_version
            ,ram
            ,disk
            ,network_type
            ,dt
            ,game_id
            ,game_type
            ,round_id
            ,travel_id
            ,travel_lv
            ,matrix
            ,block_id
            ,index_id
            ,rec_strategy
            ,rec_strategy_fact
            ,combo_cnt
            ,gain_score
            ,gain_item
            ,block_list
            ,last_click_time
            ,click_rank_per_round
            ,gain_score_per_done
            ,is_clean_screen
            ,weight
            ,put_rate
            ,userwaynum
            ,block_down_color
            ,session_id
    		,block_shape_list
    		,block_shape
    		,design_postion_upleft
            ,(ABS(HASH(distinct_id)) % 10) + 1 AS group_id
            ,from_json(properties, 'struct<clean: array<string>>').clean AS clean
            ,from_json(properties, 'struct<position: array<string>>').position AS position
    		 from hungry_studio.dwd_block_blast_gp_event_data_hi 
    		 lateral view json_tuple(
    				properties,
    				'game_id',
    				'game_type',
    				'round_id',
    				'travel_id',
    				'travel_lv',
    				'matrix',
    				'block_id',
    				'index_id',
    				'rec_strategy',
    				'rec_strategy_fact',
    				'combo_cnt',
    				'gain_score',
    				'gain_item',
    				'block_list',
    				'last_click_time',
    				'click_rank_per_round',
    				'gain_score_per_done',
    				'is_clean_screen',
    				'weight',
    				'put_rate',
    				'userwaynum',
    				'block_down_color',
    				'session_id',
    				'block_shape_list',
    		        'block_shape',
    		        'design_postion_upleft'
    			) t2 as game_id,
    			game_type,
    			round_id,
    			travel_id,
    			travel_lv,
    			matrix,
    			position,
    			block_id,
    			index_id,
    			rec_strategy,
    			rec_strategy_fact,
    			combo_cnt,
    			gain_score,
    			gain_item,
    			block_list,
    			last_click_time,
    			click_rank_per_round,
    			gain_score_per_done,
    			`is_clean_screen`,
    		    `weight`,
    		    `put_rate`,
    		    `userwaynum`,
    		    `block_down_color`,
    		    `session_id`,
    		    `block_shape_list`,
    		    `block_shape`,
    		    `design_postion_upleft`
    				where dt = '{DT}'
    			and event_name in('game_touchend_block_done', 'usr_data_game_start')
    		'''.format(tmp_tbl_name=tmp_tbl_name, DT=DT, tbl_path=tbl_path)).show()

    print('''执行创建并写入default.{tmp_tbl_name}完成,执行时间{DT}'''.format(tmp_tbl_name=tmp_tbl_name, DT=DT))
    source_data_sql =  '''
            SELECT 
                *,
            is_clear_screen_udf(matrix) AS is_clear_screen,
            IF(SIZE(clean) = 0, FALSE, TRUE) AS is_blast,
            IF(SIZE(clean) = 0, 0, SIZE(clean)) AS blast_row_col_cnt,
             CASE 
                    WHEN block_id IN (1, 2, 3, 37, 38) THEN 2
                    WHEN block_id IN (4, 5, 6, 15, 27, 28, 39, 40, 41) THEN 3
                    WHEN block_id IN (7, 8, 9, 10, 14, 16, 17, 18, 19, 20, 25, 26, 29, 30, 31, 32, 33, 34, 42) THEN 4
                    WHEN block_id IN (11, 12, 21, 22, 23, 24) THEN 5
                    WHEN block_id IN (35, 36) THEN 6
                    WHEN block_id = 13 THEN 9
                    ELSE NULL
                END AS common_block_cnt,
                lag(event_timestamp) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_event_timestamp,
                lag(matrix) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_matrix,
                lag(clean, 5) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_clean_5,
                lag(clean, 4) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_clean_4,
                lag(clean, 3) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_clean_3,
                lag(clean, 2) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_clean_2,
                lag(clean, 1) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_clean_1,
                lead(clean, 3) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lead_clean_3,
                lead(clean, 2) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lead_clean_2,
                lead(clean, 1) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lead_clean_1,
                lag(cast(combo_cnt as int), 1) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_combo_cnt_1,
                lag(cast(combo_cnt as int), 2) OVER (PARTITION BY distinct_id, game_id, game_type ORDER BY event_timestamp ASC) AS lag_combo_cnt_2
            FROM default.{tmp_tbl_name} where dt = '{DT}' 
            and group_id >= 1 and group_id <=4
        		'''.format(tmp_tbl_name=tmp_tbl_name, DT=DT)
    print(source_data_sql)
    df = spark.sql(source_data_sql)
    df.createOrReplaceTempView('source_data')
    spark.sql('''
        insert overwrite table hungry_studio.dwd_block_blast_gp_block_action_block_pre_new_di
        SELECT 
        device_id,
        app_id,
        install_datetime,
        os,
        ip,
        country_cn AS country,
        city_cn AS city,
        uuid,
        distinct_id,
        event_name,
        event_timestamp,
        event_datetime,
        game_id,
        game_type,
        round_id,
        travel_id,
        travel_lv,
        matrix,
        position,
        concat('[', concat_ws(',', clean), ']') as clean,
        block_id,
        index_id,
        rec_strategy,
        rec_strategy_fact,
        combo_cnt,
        gain_score,
        gain_item,
        block_list,
        lag_event_timestamp,
        (event_timestamp - lag_event_timestamp) / 1000 AS time_diff_in_seconds,
        most_common_element(rec_strategy_fact) AS rec_strategy_fact_most,
        lag_matrix,
        is_clear_screen,
        is_blast,
        blast_row_col_cnt,
       CASE 
            WHEN size(clean) > 0 THEN
                CASE 
                    WHEN (IF(size(lag_clean_3) > 0, 1, 0) + IF(size(lag_clean_2) > 0, 1, 0) + IF(size(lag_clean_1) > 0, 1, 0)) > 0 THEN TRUE
                    WHEN (IF(size(lag_clean_3) > 0, 1, 0) + IF(size(lag_clean_2) > 0, 1, 0) + IF(size(lag_clean_1) > 0, 1, 0)) = 0
                         AND combo_cnt = '-1'
                         AND (IF(size(lead_clean_3) > 0, 1, 0) + IF(size(lead_clean_2) > 0, 1, 0) + IF(size(lead_clean_1) > 0, 1, 0)) > 0 THEN TRUE
                    ELSE FALSE
                END
            ELSE
                CASE 
                    WHEN (IF(size(lag_clean_2) > 0, 1, 0) + IF(size(lag_clean_1) > 0, 1, 0)) = 0 THEN FALSE
                    WHEN (IF(size(lag_clean_2) > 0, 1, 0) + IF(size(lag_clean_1) > 0, 1, 0)) = 2 THEN TRUE
                    WHEN size(lag_clean_1) > 0
                         AND lag_combo_cnt_1 = -1
                         AND (IF(size(lead_clean_1) > 0, 1, 0) + IF(size(lead_clean_2) > 0, 1, 0)) >= 1 THEN TRUE
                    WHEN size(lag_clean_1) > 0
                         AND lag_combo_cnt_1 > -1
                         AND (IF(size(lag_clean_3) > 0, 1, 0) + IF(size(lag_clean_4) > 0, 1, 0)) >= 1 THEN TRUE
                    WHEN size(lag_clean_2) > 0
                         AND lag_combo_cnt_2 = -1
                         AND size(lead_clean_1) > 0 THEN TRUE
                    WHEN size(lag_clean_2) > 0
                         AND lag_combo_cnt_2 > -1
                         AND (IF(size(lag_clean_3) > 0, 1, 0) + IF(size(lag_clean_4) > 0, 1, 0) + IF(size(lag_clean_5) > 0, 1, 0)) >= 1 THEN TRUE
                    ELSE FALSE
                END
        END AS is_combo_status,
        common_block_cnt,
        CASE
            WHEN is_clear_screen = TRUE THEN 300
            WHEN is_blast = FALSE THEN common_block_cnt
            WHEN combo_cnt <> '-1' AND blast_row_col_cnt = 1 THEN (combo_cnt + 2) * 10 + common_block_cnt
            WHEN combo_cnt <> '-1' AND blast_row_col_cnt > 1 THEN (combo_cnt + 2) * blast_row_col_cnt * (blast_row_col_cnt - 1) * 10 + common_block_cnt
            ELSE blast_row_col_cnt * 10 + common_block_cnt
        END AS step_score,
        CAST(click_rank_per_round AS INT) AS block_index_id,
        CAST(get_area_complex_value(matrix) as int) AS matrix_complex_value,
        (event_timestamp - last_click_time) / 1000 AS time_action_in_seconds,
        (event_timestamp - lag_event_timestamp) / 1000 - (event_timestamp - last_click_time) / 1000 AS time_think_in_seconds,
        gain_score_per_done,
        is_clean_screen,
        last_click_time,
        weight,
        put_rate,
        userwaynum,
        IF(clean IS NULL OR size(clean) = 0, 0, 1) AS clean_times,
        IF(clean IS NULL OR size(clean) = 0, 0, size(clean)) AS clean_cnt,
        app_version,
        ram,
        disk,
        cast(get_area_complex_value(matrix) as int) as cur_matrix_complex_value,
        cast(block_down_color as int) block_down_color,
        network_type,
        dt,
        group_id
    FROM source_data 
    		''')
    print("执行写入成功")
# 关闭 Spark 会话
spark.stop()
