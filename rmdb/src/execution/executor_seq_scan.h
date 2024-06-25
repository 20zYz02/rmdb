/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }
    std::string get_tab_name() override { return tab_name_; }
    
    Value get_record_value(const RmRecord &record, const TabCol &col) {
        Value val;
        auto col_meta = sm_manager_->db_.get_table(tab_name_).get_col(col.col_name)[0];

        val.type = col_meta.type;
        
        if (col_meta.type == TYPE_INT) {
            val.set_int(*(int *)(record.data + col_meta.offset));
        } else if (col_meta.type == TYPE_FLOAT) {
            val.set_float(*(float *)(record.data + col_meta.offset));
        } else if (col_meta.type == TYPE_STRING) {
            
            int offset = col_meta.offset;
            int len = col_meta.len;

            val.set_str(std::string(record.data + offset, len));
        } 
        else {
            throw TypeNotExistsError();
        }


        return val;
    }

    bool check_condition(const Condition &cond, const RmRecord &record){
        Value left = get_record_value(record, cond.lhs_col);
        Value right;

        if (cond.is_rhs_val) {
            right = cond.rhs_val;
        } else {
            right = get_record_value(record, cond.rhs_col);
        }
        int flag = 0;
        if (left.type == TYPE_INT && right.type == TYPE_INT) {
            if (left.int_val > right.int_val){
                flag = 1;
            } else if (left.int_val < right.int_val){
                flag = -1;
            } else {
                flag = 0;
            }
        } else if (left.type == TYPE_FLOAT && right.type == TYPE_FLOAT) {
            if (left.float_val > right.float_val){
                flag = 1;
            } else if (left.float_val < right.float_val){
                flag = -1;
            } else {
                flag = 0;
            }
        } else if (left.type == TYPE_STRING && right.type == TYPE_STRING) {
            flag = left.str_val.compare(right.str_val);

        } else if (left.type == TYPE_INT && right.type == TYPE_FLOAT) {
            if (left.int_val > right.float_val){
                flag = 1;
            } else if (left.int_val < right.float_val){
                flag = -1;
            } else {
                flag = 0;
            }
        } else if (left.type == TYPE_FLOAT && right.type == TYPE_INT) {
            if (left.float_val > right.int_val){
                flag = 1;
            } else if (left.float_val < right.int_val){
                flag = -1;
            } else {
                flag = 0;
            }
        } else {
            throw InternalError("Unexpected value pair field type");
        }
        switch (cond.op) {
            case OP_EQ:
                return flag == 0;
            case OP_NE:
                return flag != 0;
            case OP_LT:
                return flag < 0;
            case OP_LE:
                return flag <= 0;
            case OP_GT:
                return flag > 0;
            case OP_GE:
                return flag >= 0;
            default:
                throw InternalError("Unexpected cond.op field type");
        }
    }

    void beginTuple() override {
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end() && !check_eval()) {      // 跳过不满足条件的记录,找到第一条符合的
            scan_->next();
        }

    }
    bool check_eval(){
        auto rec = fh_->get_record(scan_->rid(), context_);
        bool is_valid = true;
        for(auto cond:conds_){
            if(!check_condition(cond,*rec)){
                is_valid = false;
                break;
            }
        }
        return is_valid;
    }

    void nextTuple() override {
        do {
            scan_->next();  
        } while (!is_end() && !check_eval());
    }

    std::unique_ptr<RmRecord> Next() override {
        if (scan_->is_end()) 
            return nullptr;
        else {
            return fh_->get_record(scan_->rid(), context_); 
        }  

    }

    Rid &rid() override { 
        rid_ = scan_->rid();   
        return rid_;
    }
};