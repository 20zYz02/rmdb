/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    // 初始化rid_，找到第一个存放了记录的位置
    int page_no = file_handle_->get_file_hdr().first_free_page_no;
    RmPageHandle page_handle = file_handle_->fetch_page_handle(page_no);

    // 查找第一个非空闲的记录位置
    int slot_no = Bitmap::first_bit(true, page_handle.bitmap, file_handle_->get_file_hdr().num_records_per_page);

    if (slot_no != file_handle_->get_file_hdr().num_records_per_page) {
        rid_ = Rid{ page_no, slot_no };
    } else {
        // 如果找不到非空闲的记录位置，则初始化为无效的记录号
        rid_ = Rid{ -1, -1 };
    }
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    if (rid_.page_no == -1 || rid_.slot_no == -1) {
        return; // 如果当前位置已经无效，则直接返回
    }

    int page_no = rid_.page_no;
    int slot_no = rid_.slot_no;

    RmPageHandle page_handle = file_handle_->fetch_page_handle(page_no);

    // 查找下一个非空闲的记录位置
    slot_no = Bitmap::next_bit(true, page_handle.bitmap, file_handle_->get_file_hdr().num_records_per_page, slot_no);

    if (slot_no != file_handle_->get_file_hdr().num_records_per_page) {
        rid_ = Rid{ page_no, slot_no };
    } else {
        // 如果找不到下一个非空闲的记录位置，则将 rid_ 设为无效
        rid_ = Rid{ -1, -1 };
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    return (rid_.page_no == -1 || rid_.slot_no == -1);
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}