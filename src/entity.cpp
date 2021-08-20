/**
 * Copyright 2018 VMware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "hotstuff/entity.h"
#include "hotstuff/hotstuff.h"

namespace hotstuff {

void OrderedList::serialize(DataStream &s) const {
    s << htole((uint32_t)cmds.size());
    for (const auto &cmd : cmds)
        s << cmd;
    for (const auto &timestamp : timestamps)
        s << timestamp;
}

void OrderedList::unserialize(DataStream &s, HotStuffCore *hsc) {
    uint32_t n;
    s >> n;
    cmds.resize(n);
    for (auto &cmd : cmds)
        s >> cmd;
    timestamps.resize(n);
    for (auto &timestamp : timestamps)
        s >> timestamp;
}

void Block::serialize(DataStream &s) const {
    s << htole((uint32_t)parent_hashes.size());
    for (const auto &hash: parent_hashes)
        s << hash;
    //  HOTSTUFF_LOG_PROTO("Proposed block size : %lu", cmds.size());
    // s << htole((uint32_t)cmds.size());
    // for (auto cmd: cmds)
    //     s << cmd;

    // serializing LeaderProposedOrderedList
    s << htole((uint32_t)proposed_orderedlist.cmds.size());
    for (auto cmd: proposed_orderedlist.cmds) {
        //s << htole((uint32_t)cmd_vec.size());
        //for (auto cmd: cmd_vec) {
            s << cmd;
        //}
    }

    s << *qc << htole((uint32_t)extra.size()) << extra;
}

void Block::unserialize(DataStream &s, HotStuffCore *hsc) {
    uint32_t n;
    s >> n;
    n = letoh(n);
    parent_hashes.resize(n);
    for (auto &hash: parent_hashes)
        s >> hash;
    // s >> n;
    // n = letoh(n);
    // cmds.resize(n);
    // for (auto &cmd: cmds)
    //     s >> cmd;
//    for (auto &cmd: cmds)
//        cmd = hsc->parse_cmd(s);

    // deserializing LeaderProposedOrderedList
    uint32_t tot_rank;
    s >> tot_rank;
    tot_rank = letoh(tot_rank);
    proposed_orderedlist.cmds.resize(tot_rank);
    for (auto &cmd: proposed_orderedlist.cmds) {
        //uint32_t num_cmds;
        //s >> num_cmds;
        //cmd_vec.resize(num_cmds);
        //for (auto &cmd: cmd_vec) {
            s >> cmd;
        //}
    }
    // printing the leader proosed orderedlist
    HOTSTUFF_LOG_INFO("Only use print_out here in unserialize...which is strange.");
    proposed_orderedlist.print_out();

    qc = hsc->parse_quorum_cert(s);
    s >> n;
    n = letoh(n);
    if (n == 0)
        extra.clear();
    else
    {
        auto base = s.get_data_inplace(n);
        extra = bytearray_t(base, base + n);
    }
    this->hash = salticidae::get_hash(*this);
}

bool Block::verify(const HotStuffCore *hsc) const {
    if (qc->get_obj_hash() == hsc->get_genesis()->get_hash())
        return true;
    return qc->verify(hsc->get_config());
}

promise_t Block::verify(const HotStuffCore *hsc, VeriPool &vpool) const {
    if (qc->get_obj_hash() == hsc->get_genesis()->get_hash())
        return promise_t([](promise_t &pm) { pm.resolve(true); });
    return qc->verify(hsc->get_config(), vpool);
}





void CommandTimestampStorage::add_command_to_storage(const uint256_t cmd_hash)
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    uint64_t timestamp_us = tv.tv_sec;
    timestamp_us *= 1000 * 1000;
    timestamp_us += tv.tv_usec;
    HOTSTUFF_LOG_INFO("(cmd,timestamp): (%s,%s)",get_hex10(cmd_hash).c_str(),boost::lexical_cast<std::string>(timestamp_us).c_str());
    cmd_ts_storage.insert(std::make_pair(cmd_hash, timestamp_us));
    available_cmd_hashes.push_back(cmd_hash);
    available_timestamps.push_back(timestamp_us);
    cmd_hashes.push_back(cmd_hash);
    timestamps.push_back(timestamp_us);
}

/** return true if it is a new command */
bool CommandTimestampStorage::is_new_command(const uint256_t &cmd_hash) const
{
    if (std::find(cmd_hashes.begin(), cmd_hashes.end(), cmd_hash) == cmd_hashes.end())
    {
        return true;
    }
    else
    {
        return false;
    }
}

/** Updating the available cmds and timestamps on receiving acceptable proposals.
  * This must be called after ensuring acceptable_fairness_check() outputs True.
  * The input to this function will be block->cmds. */
void CommandTimestampStorage::refresh_available_cmds(const std::vector<uint256_t> cmds)
{
    for (auto& cmd : cmds)
    {
        auto it = std::find(available_cmd_hashes.begin(), available_cmd_hashes.end(), cmd);
        if (it != available_cmd_hashes.end())
        {
            auto index = std::distance(available_cmd_hashes.begin(), it);
            // HOTSTUFF_LOG_PROTO("The command being erased is %s", get_hex10(available_cmd_hashes[index]).c_str());
            available_cmd_hashes.erase(available_cmd_hashes.begin() + index);
            available_timestamps.erase(available_timestamps.begin() + index);

        }
    }
}

/** Get a orderedlist on giving a vector of commands as input.
     * Called just before calling acceptable fairness check in order to get the 
     * corresponding timestamps of the commands in the proposed block.
     * Must be called after add_command_to_storage().*/
std::vector<uint64_t> CommandTimestampStorage::get_timestamps(const std::vector<uint256_t> &cmd_hashes_inquired) const
{
    std::vector<uint64_t> timestamps_list;
    for (auto& cmd_hash : cmd_hashes_inquired)
    {
        auto it = std::find(cmd_hashes.begin(), cmd_hashes.end(), cmd_hash);
        timestamps_list.push_back(timestamps[std::distance(cmd_hashes.begin(), it)]);
    }
    return timestamps_list;
}

std::vector<uint64_t> CommandTimestampStorage::get_timestamps_1(const LeaderProposedOrderedList &proposed_orderedlist_inquired) const
{
    std::vector<uint64_t> proposed_orderedlist_timestamp;
    for (auto &cmd_hash : proposed_orderedlist_inquired.cmds) 
    {
        // std::vector<uint64_t> timestamp_vec;
        // timestamp_vec.clear();
        //for (auto& cmd_hash: cmd_vec) 
        // {
            auto it = std::find(cmd_hashes.begin(), cmd_hashes.end(), cmd_hash);
            // timestamp_vec.push_back(timestamps[std::distance(cmd_hashes.begin(), it)]);
        // }
        // proposed_orderedlist_timestamp.push_back(timestamp_vec);
        proposed_orderedlist_timestamp.push_back(timestamps[std::distance(cmd_hashes.begin(), it)]);
    }
    return proposed_orderedlist_timestamp;
}

/** Get a new ordered list to send as part of the vote.
  * Basically has to repackage the available cmds and timestamps into ordered list
*/
orderedlist_t CommandTimestampStorage::get_orderedlist(const uint256_t &blk_hash, uint32_t blk_size)
{
    while (available_cmd_hashes.size() < 10)
    {
        // HOTSTUFF_LOG_INFO("Waiting!");
        HOTSTUFF_LOG_INFO("Number of available cmd hashes is: %d", available_cmd_hashes.size());
    }
    // HOTSTUFF_LOG_PROTO("Number of available cmd hashes is: %d", available_cmd_hashes.size());

    
    std::vector<uint256_t> proposed_available_cmd_hashes;
    std::vector<uint64_t> proposed_available_timestamps;
    for (auto i = 0; i < blk_size; i++)
    {
        proposed_available_cmd_hashes.push_back(available_cmd_hashes[i]);
        proposed_available_timestamps.push_back(available_timestamps[i]);
        HOTSTUFF_LOG_INFO("The cmd to be sent to the leader is: %s", get_hex(available_timestamps[i]).c_str());
        //HOTSTUFF_LOG_INFO("The timestamp to be sent to the leader is: %s", boost::lexical_cast<std::string>(available_timestamps[i]).c_str());
    }
    auto it = replica_preferred_ordering_cache.find(blk_hash);
    if (it != replica_preferred_ordering_cache.end()) 
    {
        replica_preferred_ordering_cache.erase(blk_hash);
    }

    //OrderedList temp = new OrderedList(proposed_available_cmd_hashes, proposed_available_timestamps);	
    //temp.printout();

    return replica_preferred_ordering_cache.insert(std::make_pair(blk_hash,  new OrderedList(proposed_available_cmd_hashes, proposed_available_timestamps))).first->second;
}

void OrderedListStorage::add_ordered_list(const uint256_t block_hash, const OrderedList preferred_orderedlist, bool leader, size_t num_peers)
{
    HOTSTUFF_LOG_INFO("Adding orderedlist to the storage at the leader!");
    preferred_orderedlist.printout();
    // HOTSTUFF_LOG_INFO("The block hash is: %s", get_hex10(block_hash).c_str());
    // size_t num_faulty = num_peers / 3;
    // HOTSTUFF_LOG_PROTO("Number of faulty is: %lu", num_faulty);
    // size_t test_num = num_peers + 1 - num_faulty
    auto it = ordered_list_cache.find(block_hash);
    if (it == ordered_list_cache.end())
    {
        HOTSTUFF_LOG_PROTO("It is a new addition!");
        std::vector<OrderedList> temp{preferred_orderedlist};
        ordered_list_cache.insert(std::make_pair(block_hash, temp));
        if(leader ==true) {HOTSTUFF_LOG_PROTO("It is leader");}
    }
    else
    {
        HOTSTUFF_LOG_PROTO("It is a new addition to existing record!");
        if (leader == true) 
        {
            it->second.insert(it->second.begin(), preferred_orderedlist);
            HOTSTUFF_LOG_PROTO("It is leader");
        }
        else 
        {
            // for now the assumption is that all replicas are honest
            it->second.push_back(preferred_orderedlist);
            HOTSTUFF_LOG_PROTO("It is replica");
            
        }
        // HOTSTUFF_LOG_PROTO("Size of cache for this index is %lu", it->second.size());
    }
}

std::vector<OrderedList> OrderedListStorage::get_set_of_orderedlists(const uint256_t block_hash) const
{
    if(ordered_list_cache.find(block_hash)->second.size() == 0){
        throw std::runtime_error("Empty orderedlist...");
    }
    return ordered_list_cache.find(block_hash)->second;
}

std::vector<uint256_t> OrderedListStorage::get_all_block_hashes() const {
    std::vector<uint256_t> block_hashes;
    for (auto kv : ordered_list_cache)
    {
        block_hashes.push_back(kv.first);
    }
    return block_hashes;
}

std::vector<uint256_t> OrderedListStorage::get_cmds_for_first_one(const uint256_t block_hash) const {
    return ordered_list_cache.find(block_hash)->second[0].extract_cmds();
}
std::vector<uint64_t> OrderedListStorage::get_timestamps_for_first_one(const uint256_t block_hash) const
{
    return ordered_list_cache.find(block_hash)->second[0].extract_timestamps();
}
std::vector<uint256_t> OrderedListStorage::get_cmds_for_second_one(const uint256_t block_hash) const
{
    return ordered_list_cache.find(block_hash)->second[1].extract_cmds();
}
std::vector<uint64_t> OrderedListStorage::get_timestamps_for_second_one(const uint256_t block_hash) const
{
    return ordered_list_cache.find(block_hash)->second[1].extract_timestamps();
}
}
