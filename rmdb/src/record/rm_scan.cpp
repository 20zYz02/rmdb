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
// 获取文件头信息
auto file_header = file_handle_->file_hdr_;

// 初始化页号和插槽号为无效值
page_id_t page_no = -1;
int slot_no = -1;

// 循环遍历文件的每一页，寻找第一个非空闲插槽
for (page_no = 1; page_no < file_header.num_pages; ++page_no) {
    auto page_handle = file_handle->fetch_page_handle(page_no); // 获取当前页的页面句柄
    int first_free_slot = Bitmap::first_bit(true, page_handle.bitmap, file_header.num_records_per_page);// 查找页面中第一个空闲插槽的位置
    file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    // 如果找到了空闲插槽，则记录页号和插槽号，并结束循环
    if (first_free_slot < file_header.num_records_per_page) {
        slot_no = first_free_slot;
        break;
    }
}
if (slot_no == -1) {
    page_no = -1;
}
rid_ = Rid{page_no, slot_no};

}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
auto hdr = file_handle_->file_hdr_;
int num_slot = hdr.num_records_per_page;

for (int page_no = rid_.page_no; page_no < hdr.num_pages; ++page_no) {
    auto page_handle = file_handle_->fetch_page_handle(page_no);
    int slot_no = Bitmap::next_bit(true, page_handle.bitmap, num_slot, rid_.slot_no); // 找到此page内第一个记录
    file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    if (slot_no < num_slot) {
        rid_ = {page_no, slot_no};
        return;
    }
    rid_.slot_no = -1;
}
rid_ = {-1, -1};

}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    return rid_.page_no == -1 && rid_.slot_no == -1;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}