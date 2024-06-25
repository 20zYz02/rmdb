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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

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
    std::unique_ptr<RmRecord> Next() override {
        for(auto rid_:rids_){
            auto record = fh_->get_record(rid_, context_);
            bool is_valid = true;
            for(auto cond:conds_){
                if(!check_condition(cond,*record)){
                    is_valid = false;
                    break;
                }
            }
            if(!is_valid){
                continue;
            }
            // 设置每个字段
            for (auto &set_clause : set_clauses_) {
                auto col = set_clause.lhs;
                auto val = set_clause.rhs;

                auto col_meta = sm_manager_->db_.get_table(set_clause.lhs.tab_name).get_col(col.col_name)[0];

                int offset = col_meta.offset;
                int len = col_meta.len;

                int val_len = 0;
                if (val.type == TYPE_INT) {
                    val_len = sizeof(int);
                } else if (val.type == TYPE_FLOAT) {
                    val_len = sizeof(float);
                } else if (val.type == TYPE_STRING) {
                    val_len = val.str_val.size();
                }
                val.init_raw(val_len);
                memcpy(record->data + offset, val.raw->data, len);
            }
            fh_->update_record(rid_, record.get()->data, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};