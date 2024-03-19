#include "operators.h"
#include <cstdint>
#include <functional>
#include <iterator>
#include <mutex>
#include <thread>
#include <cassert>
#include <iostream>
#include <ostream>
#include <unistd.h>
#include <chrono>  

const constexpr unsigned NUM_THREAD = 8;

// Get materialized results
std::vector<uint64_t *> Operator::getResults() {
    std::vector<uint64_t *> result_vector;
    for (auto &c : tmp_results_) {
        result_vector.push_back(c.data());
    }
    return result_vector;
}

// Require a column and add it to results
bool Scan::require(SelectInfo info, unsigned num = 0) {
    if (info.binding != relation_binding_)
        return false;
    assert(info.col_id < relation_.columns().size());
    result_columns_.push_back(relation_.columns()[info.col_id]);
    select_to_result_col_id_[info] = result_columns_.size() - 1;
    required_select_num_[info] = num;
    return true;
}

// Run
void Scan::run() {
    // Nothing to do
    result_size_ = relation_.size();
}

// Get materialized results
std::vector<uint64_t *> Scan::getResults() {
    return result_columns_;
}

// Require a column and add it to results
bool FilterScan::require(SelectInfo info, unsigned num = 0) {
    if (info.binding != relation_binding_)
        return false;
    assert(info.col_id < relation_.columns().size());
    if (select_to_result_col_id_.find(info) == select_to_result_col_id_.end()) {
        // Add to results
        input_data_.push_back(relation_.columns()[info.col_id]);
        tmp_results_.emplace_back();
        unsigned colId = tmp_results_.size() - 1;
        select_to_result_col_id_[info] = colId;
        required_select_num_[info] = num;
    }
    return true;
}

// Copy to result
void FilterScan::copy2Result(uint64_t id) {
    for (unsigned cId = 0; cId < input_data_.size(); ++cId){
        tmp_results_[cId].push_back(input_data_[cId][id]);
    }
}

// Apply filter
bool FilterScan::applyFilter(uint64_t i, FilterInfo &f) {
    auto compare_col = relation_.columns()[f.filter_column.col_id];
    auto constant = f.constant;
    switch (f.comparison) {
    case FilterInfo::Comparison::Equal:
        return compare_col[i] == constant;
    case FilterInfo::Comparison::Greater:
        return compare_col[i] > constant;
    case FilterInfo::Comparison::Less:
        return compare_col[i] < constant;
    };
    return false;
}

// Run
void FilterScan::run() {
    for(auto &column : tmp_results_) column.reserve(relation_.size());
    for (uint64_t i = 0; i < relation_.size(); ++i) {
        bool pass = true;
        for (auto &f : filters_) {
            pass &= applyFilter(i, f);
        }
        if (pass){
            copy2Result(i);
        }
    }
    for(auto &column : tmp_results_) column.shrink_to_fit();
    result_size_ = tmp_results_.front().size();
}

// Run
void Join::run() {
    for (auto select_num : left_->required_select_num_) {
        if(select_num.first == p_info_.left){
            --select_num.second;
        }
        if(select_num.second > 0){
            required_select_num_[select_num.first] = select_num.second;
            requested_columns_left_.push_back(select_num.first);
            tmp_results_.emplace_back();
        }
    }
    for (auto select_num : right_->required_select_num_) {
        if(select_num.first == p_info_.right){
            --select_num.second;
        }
        if(select_num.second > 0){
            required_select_num_[select_num.first] = select_num.second;
            requested_columns_right_.push_back(select_num.first);
            tmp_results_.emplace_back();
        }
    }

    if (left_->result_size() > right_->result_size()) {
        std::swap(left_, right_);
        std::swap(p_info_.left, p_info_.right);
        std::swap(requested_columns_left_, requested_columns_right_);
    }
    auto left_input_data = left_->getResults();
    auto right_input_data = right_->getResults();

    // Resolve the input_ columns_
    unsigned res_col_id = 0;
    for (auto &info : requested_columns_left_) {
        copy_left_data_.push_back(left_input_data[left_->resolve(info)]);
        select_to_result_col_id_[info] = res_col_id++;
    }
    for (auto &info : requested_columns_right_) {
        copy_right_data_.push_back(right_input_data[right_->resolve(info)]);
        select_to_result_col_id_[info] = res_col_id++;
    }

    auto left_col_id = left_->resolve(p_info_.left);
    auto right_col_id = right_->resolve(p_info_.right);

    // Build phase
    auto left_key_column = left_input_data[left_col_id];
    hash_table_.reserve(left_->result_size() * 2);
    for (uint64_t i = 0, limit = i + left_->result_size(); i != limit; ++i) {
        hash_table_.emplace(left_key_column[i], i);
    }
    // Probe phase
    auto right_key_column = right_input_data[right_col_id];
    bool mutithread = left_->result_size() * right_->result_size() > 5000000 ? true : false;

    if(!mutithread){
        Probe(right_key_column, 0, right_->result_size(), tmp_results_);
        result_size_ = tmp_results_.front().size();
        return;
    }

    std::vector<std::thread> threads;
    std::vector<std::vector<std::vector<uint64_t>>> thread_results(NUM_THREAD-1, std::vector<std::vector<uint64_t>>(tmp_results_.size()));
    for(int i = 0; i < NUM_THREAD; ++i){
        uint64_t from = i * (right_->result_size()/NUM_THREAD);
        uint64_t to = i == NUM_THREAD-1 ? right_->result_size() : (i+1) * (right_->result_size()/NUM_THREAD);
        if(i == 0){
            threads.emplace_back(&Join::Probe, this, right_key_column, from, to, std::ref(tmp_results_));
        } else {
            threads.emplace_back(&Join::Probe, this, right_key_column, from, to, std::ref(thread_results[i-1]));
        }
    }
    
    threads[0].join();
    for(int i = 1; i < NUM_THREAD; ++i){
        threads[i].join();
        for(int column = 0; column<tmp_results_.size(); ++column){
            tmp_results_[column].insert(tmp_results_[column].end(), thread_results[i-1][column].begin(), thread_results[i-1][column].end());
        }
    }
    result_size_ = tmp_results_.front().size();
}

void Join::Probe(uint64_t *right_key_column, uint64_t from, uint64_t to, std::vector<std::vector<uint64_t>>& results){
    for(auto &column : results) column.reserve(to-from);
    for (uint64_t i = from, limit = to; i != limit; ++i) {
        auto rightKey = right_key_column[i];
        auto range = hash_table_.equal_range(rightKey);
        for (auto iter = range.first; iter != range.second; ++iter) {
            unsigned rel_col_id = 0;
            for (unsigned cId = 0; cId < copy_left_data_.size(); ++cId)
                results[rel_col_id++].push_back(copy_left_data_[cId][iter->second]);
            for (unsigned cId = 0; cId < copy_right_data_.size(); ++cId)
                results[rel_col_id++].push_back(copy_right_data_[cId][i]);
        }
    }
}

void Checksum::run() {
    auto results = input_->getResults();
    for (auto &sInfo : col_info_) {
        auto col_id = input_->resolve(sInfo);
        auto result_col = results[col_id];
        uint64_t sum = 0;
        result_size_ = input_->result_size();
        for (auto iter = result_col, limit = iter + input_->result_size();
                iter != limit;
                ++iter)
            sum += *iter;
        check_sums_.push_back(sum);
    }
}

void SelfJoin::copy2Result(uint64_t id) {
    for (unsigned cId = 0; cId < copy_data_.size(); ++cId)
        tmp_results_[cId].push_back(copy_data_[cId][id]);
}

// Run
void SelfJoin::run() {
    input_data_ = input_->getResults();
    for(auto select_num : input_->required_select_num_){
        if(select_num.first == p_info_.left || select_num.first == p_info_.right){
            --select_num.second;
        }
        if(select_num.second > 0){
            required_select_num_[select_num.first] = select_num.second;
            tmp_results_.emplace_back();
            required_IUs_.emplace(select_num.first);
        }
    }
    
    for (auto &iu : required_IUs_) {
        auto id = input_->resolve(iu);
        copy_data_.emplace_back(input_data_[id]);
        select_to_result_col_id_.emplace(iu, copy_data_.size() - 1);
    }

    auto left_col_id = input_->resolve(p_info_.left);
    auto right_col_id = input_->resolve(p_info_.right);

    auto left_col = input_data_[left_col_id];
    auto right_col = input_data_[right_col_id];
    for (uint64_t i = 0; i < input_->result_size(); ++i) {
        if (left_col[i] == right_col[i])
            copy2Result(i);
    }
    result_size_ = tmp_results_.front().size();
}

