#include "joiner.h"

#include <cassert>
#include <iostream>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <set>
#include <sstream>
#include <vector>
#include <map>
#include <algorithm>
#include <climits>
#include "parser.h"

// Loads a relation_ from disk
void Joiner::addRelation(const char *file_name) {
    relations_.emplace_back(file_name);
}

void Joiner::addRelation(Relation &&relation) {
    relations_.emplace_back(std::move(relation));
}

// Loads a relation from disk
const Relation &Joiner::getRelation(unsigned relation_id) {
    if (relation_id >= relations_.size()) {
        std::cerr << "Relation with id: " << relation_id << " does not exist"
                  << std::endl;
        throw;
    }
    return relations_[relation_id];
}


std::shared_ptr<Operator> Joiner::addScan(const SelectInfo &info,QueryInfo &query) {
    std::vector<FilterInfo> filters;
    for (auto &f : query.filters()) {
        if (f.filter_column.binding == info.binding) {
            filters.emplace_back(f);
        }
    }
    return !filters.empty() ?
           std::make_shared<FilterScan>(getRelation(info.rel_id), filters)
           : std::make_shared<Scan>(getRelation(info.rel_id),
                                    info.binding);
}

std::string Joiner::join(QueryInfo &query) {
    std::vector<std::map<SelectInfo, unsigned>> relation_bind_need_columns = query.need_columns();
    std::vector<std::shared_ptr<Operator>> relation_bind_op(relation_bind_need_columns.size());
    for (unsigned i = 0; i < relation_bind_op.size(); ++i){
        relation_bind_op[i] = addScan(relation_bind_need_columns[i].begin()->first, query);
        for(auto &s_info_num : relation_bind_need_columns[i]){
            relation_bind_op[i]->require(s_info_num.first, s_info_num.second);
        }
        relation_bind_op[i]->run();
    }

    auto predicates_copy = query.predicates();
    while(predicates_copy.size()){
        unsigned min_cost = UINT_MAX;
        auto min_predicate_ptr = predicates_copy.begin();
        for(auto predicate_ptr = predicates_copy.begin(); predicate_ptr != predicates_copy.end(); ++predicate_ptr){
            if(relation_bind_op[predicate_ptr->left.binding] == relation_bind_op[predicate_ptr->right.binding]){
                min_predicate_ptr = predicate_ptr;
                break;
            }
            unsigned cur_cost = relation_bind_op[predicate_ptr->left.binding]->result_size() + 
                                relation_bind_op[predicate_ptr->right.binding]->result_size();
            if(cur_cost < min_cost){
                min_predicate_ptr = predicate_ptr;
                min_cost = cur_cost;
            }
        }
        auto left_op = relation_bind_op[min_predicate_ptr->left.binding];
        auto right_op = relation_bind_op[min_predicate_ptr->right.binding];
        std::shared_ptr<Operator> new_join;
        if(left_op == right_op){
            new_join = std::make_shared<SelfJoin>(left_op, *min_predicate_ptr);
        } else {
            new_join = std::make_shared<Join>(left_op, right_op, *min_predicate_ptr);
        }
        new_join->run();
        for(auto &join_op : relation_bind_op){
            if(join_op == left_op || join_op == right_op){
                join_op = new_join;
            }
        }
        predicates_copy.erase(min_predicate_ptr);
    }

    Checksum checksum(relation_bind_op.front(), query.selections());
    checksum.run();
    std::stringstream out;
    auto &results = checksum.check_sums();
    for (unsigned i = 0; i < results.size(); ++i) {
        out << (checksum.result_size() == 0 ? "NULL" : std::to_string(results[i]));
        if (i < results.size() - 1)
            out << " ";
    }
    out << "\n";
    return out.str();
}

