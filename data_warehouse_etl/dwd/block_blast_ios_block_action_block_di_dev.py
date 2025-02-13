import math
import copy

class BaseSignInfo:
    def __init__(self):
        self.square_block_sign = -1
        self.square_empty_sign = 1
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
    def get_area_info_and_corner_list(self):
        """
        获取盘面所有属性数据
        :return:
        """
        self.convert_list()
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
target_line_length = target_width_length = 8
square_type_11_info = SquareType11Info()
square_type_13_info = SquareType13Info()
square_type_22_info = SquareType22Info()
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
import time
if __name__ == "__main__":
    test = '''[[-1,-1,-1,-1,-1,-1,-1,-1],[-1,-1,-1,-1,-1,-1,-1,-1],[-1,-1,-1,-1,-1,-1,-1,-1],[-1,-1,-1,-1,-1,-1,-1,-1],[-1,-1,-1,-1,-1,-1,-1,-1],[-1,-1,-1,-1,-1,3,3,3],[-1,-1,-1,-1,-1,3,3,3],[-1,-1,-1,-1,-1,3,3,3]]'''
    print(f'''{test}测试成功''')
    start_time = time.time()
    print(get_area_complex_value(test))
    end_time = time.time()
    print(f"Execution time1: {end_time - start_time} seconds")

    print(f'''{test}测试成功''')
    start_time1 = time.time()
    print(get_area_complex_value(test))
    end_time1 = time.time()
    print(f"Execution time2: {end_time1 - start_time1} seconds")

