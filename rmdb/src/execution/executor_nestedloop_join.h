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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

    std::unique_ptr<RmRecord> cur_rec;          //当前所存记录   
    std::vector<std::unique_ptr<RmRecord>> left_rec;     //左表记录
    std::vector<std::unique_ptr<RmRecord>> right_rec;    //右表记录
    int lIdx,rIdx;
    int lrec_size;
    int rrec_size;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    void beginTuple() override {
        for (left_->beginTuple(); !left_->is_end(); left_->nextTuple()) {
            left_rec.push_back(left_->Next());
        }
        for (right_->beginTuple(); !right_->is_end(); right_->nextTuple()) {
            right_rec.push_back(right_->Next());
        }
        lrec_size = left_rec.size();
        rrec_size = right_rec.size();
        lIdx = 0;rIdx = 0;
        isend = (lIdx>=lrec_size)||(rIdx>=rrec_size);
        while (!isend && !check()) {     // 滑过不满足条件的记录
            rIdx++;
            if(rIdx>=rrec_size){
                rIdx = 0;
                lIdx++;
                if(lIdx>=lrec_size){
                    isend = true;
                }
            }
        }
        if (isend) {      // 没有任何满足条件的记录
            return;
        }
        cur_rec = std::make_unique<RmRecord>(len_);
        memcpy(cur_rec->data, left_rec[lIdx]->data, left_->tupleLen());
        memcpy(cur_rec->data + left_->tupleLen(), right_rec[rIdx]->data, right_->tupleLen());

    }

    Value get_record_value(const RmRecord &record, int offset, int len, ColType type){
        Value val;
        val.type = type;

        if (type == TYPE_INT) {
            val.set_int(*(int *)(record.data + offset));
        } else if (type == TYPE_FLOAT) {
            val.set_float(*(float *)(record.data + offset));
        } else if (type == TYPE_STRING) {
            val.set_str(std::string(record.data + offset, len));
        } else {
             throw TypeNotExistsError();
        }

        return val;
    }

    bool check_condition(Value left, Value right, CompOp op){
        int cp_res = 0;
        if (left.type == TYPE_INT && right.type == TYPE_INT) {
            if (left.int_val > right.int_val){
                cp_res = 1;
            } else if (left.int_val < right.int_val){
                cp_res = -1;
            } else {
                cp_res = 0;
            }
        } else if (left.type == TYPE_FLOAT && right.type == TYPE_FLOAT) {
            if (left.float_val > right.float_val){
                cp_res = 1;
            } else if (left.float_val < right.float_val){
                cp_res = -1;
            } else {
                cp_res = 0;
            }
        } else if (left.type == TYPE_STRING && right.type == TYPE_STRING) {

            std::cout<<"left.str_val: "<<left.str_val<<" right.str_val: "<<right.str_val<<std::endl;

            cp_res = left.str_val.compare(right.str_val);

        } 
        else if (left.type == TYPE_INT && right.type == TYPE_FLOAT) {
            if (left.int_val > right.float_val){
                cp_res = 1;
            } else if (left.int_val < right.float_val){
                cp_res = -1;
            } else {
                cp_res = 0;
            }
        } else if (left.type == TYPE_FLOAT && right.type == TYPE_INT) {
            if (left.float_val > right.int_val){
                cp_res = 1;
            } else if (left.float_val < right.int_val){
                cp_res = -1;
            } else {
                cp_res = 0;
            }
        } 
        else {
            throw InternalError("Unexpected value pair field type");
        }

        std::cout<<"compare result: "<<cp_res<<std::endl;

        switch (op) {
            case OP_EQ:
                return cp_res == 0;
            case OP_NE:
                return cp_res != 0;
            case OP_LT:
                return cp_res < 0;
            case OP_LE:
                return cp_res <= 0;
            case OP_GT:
                return cp_res > 0;
            case OP_GE:
                return cp_res >= 0;
            default:
                throw InternalError("Unexpected cond.op field type");
        }
    }
    bool check(){
        bool flag = true;

        for (auto &cond : fed_conds_){
            // 从左表中查出此字段信息
            int left_off=0, left_len = 0;
            ColType left_type;
            auto col_meta = left_.get()->cols();
            for (auto &lcol : col_meta){
                if (lcol.name.compare(cond.lhs_col.col_name)==0){
                    left_off = lcol.offset;
                    left_len = lcol.len;
                    left_type = lcol.type;
                }
            }

            int right_off=0, right_len = 0;
            ColType right_type;
            col_meta = right_.get()->cols();
            for (auto &rcol : col_meta){
                if (rcol.name.compare(cond.rhs_col.col_name)==0){
                    right_off = rcol.offset;
                    right_len = rcol.len;
                    right_type = rcol.type;
                }
            }

            Value left_val = get_record_value(*left_rec[lIdx].get(), left_off, left_len, left_type);
            Value right_val = get_record_value(*right_rec[rIdx].get(), right_off, right_len, right_type);
            if(!check_condition(left_val,right_val,cond.op)){
                flag = false;
                break;
            }
        }
        return flag;
    }

    void nextTuple() override {
        assert(!is_end());
        do {         // 跳过不满足条件的记录
            rIdx++;
            if(rIdx>=rrec_size){
                rIdx = 0;
                lIdx++;
                if(lIdx>=lrec_size){
                    isend = true;
                }
            }
        } while (!isend && !check());
        if (isend) {      
            return;
        }
       
        assert(cur_rec == nullptr);      
        cur_rec = std::make_unique<RmRecord>(len_);
        memcpy(cur_rec->data, left_rec[lIdx]->data, left_->tupleLen());
        memcpy(cur_rec->data + left_->tupleLen(), right_rec[rIdx]->data, right_->tupleLen());
    }

    bool is_end() const override {
        return isend;
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(cur_rec);
    }

    Rid &rid() override { return _abstract_rid; }
};
