/**
 * Copyright 2018 VMware
 * Copyright 2018 Ted Yin
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
#include <cassert>
#include <random>
#include <signal.h>
#include <sys/time.h>

#include "hotstuff/hotstuff.h"
#include "hotstuff/client.h"
#include "hotstuff/liveness.h"
#include "hotstuff/aequitas.h"
#define max_num_all_txn 5005 //5005
using salticidae::static_pointer_cast;

#define LOG_INFO HOTSTUFF_LOG_INFO
#define LOG_DEBUG HOTSTUFF_LOG_DEBUG
#define LOG_WARN HOTSTUFF_LOG_WARN

namespace hotstuff {

const opcode_t MsgPropose::opcode;
MsgPropose::MsgPropose(const Proposal &proposal) { serialized << proposal; }
void MsgPropose::postponed_parse(HotStuffCore *hsc) {
    proposal.hsc = hsc;
    serialized >> proposal;
}

const opcode_t MsgVote::opcode;
MsgVote::MsgVote(const Vote &vote) { serialized << vote; }
void MsgVote::postponed_parse(HotStuffCore *hsc) {
    vote.hsc = hsc;
    serialized >> vote;
}

const opcode_t MsgReqBlock::opcode;
MsgReqBlock::MsgReqBlock(const std::vector<uint256_t> &blk_hashes) {
    serialized << htole((uint32_t)blk_hashes.size());
    for (const auto &h: blk_hashes)
        serialized << h;
}

MsgReqBlock::MsgReqBlock(DataStream &&s) {
    uint32_t size;
    s >> size;
    size = letoh(size);
    blk_hashes.resize(size);
    for (auto &h: blk_hashes) s >> h;
}

const opcode_t MsgRespBlock::opcode;
MsgRespBlock::MsgRespBlock(const std::vector<block_t> &blks) {
    serialized << htole((uint32_t)blks.size());
    for (auto blk: blks) serialized << *blk;
}

void MsgRespBlock::postponed_parse(HotStuffCore *hsc) {
    uint32_t size;
    serialized >> size;
    size = letoh(size);
    blks.resize(size);
    for (auto &blk: blks)
    {
        Block _blk;
        _blk.unserialize(serialized, hsc);
        blk = hsc->storage->add_blk(std::move(_blk), hsc->get_config());
    }
}

// TODO: improve this function
void HotStuffBase::exec_command(uint256_t cmd_hash, commit_cb_t callback) {
    cmd_pending.enqueue(std::make_pair(cmd_hash, callback));
}

void HotStuffBase::on_fetch_blk(const block_t &blk) {
#ifdef HOTSTUFF_BLK_PROFILE
    blk_profiler.get_tx(blk->get_hash());
#endif
    LOG_DEBUG("fetched %.10s", get_hex(blk->get_hash()).c_str());
    part_fetched++;
    fetched++;
    //for (auto cmd: blk->get_cmds()) on_fetch_cmd(cmd);
    const uint256_t &blk_hash = blk->get_hash();
    auto it = blk_fetch_waiting.find(blk_hash);
    if (it != blk_fetch_waiting.end())
    {
        it->second.resolve(blk);
        blk_fetch_waiting.erase(it);
    }
}

bool HotStuffBase::on_deliver_blk(const block_t &blk) {
    const uint256_t &blk_hash = blk->get_hash();
    bool valid;
    /* sanity check: all parents must be delivered */
    for (const auto &p: blk->get_parent_hashes())
        assert(storage->is_blk_delivered(p));
    if ((valid = HotStuffCore::on_deliver_blk(blk)))
    {
        LOG_DEBUG("block %.10s delivered",
                get_hex(blk_hash).c_str());
        part_parent_size += blk->get_parent_hashes().size();
        part_delivered++;
        delivered++;
    }
    else
    {
        LOG_WARN("dropping invalid block");
    }

    bool res = true;
    auto it = blk_delivery_waiting.find(blk_hash);
    if (it != blk_delivery_waiting.end())
    {
        auto &pm = it->second;
        if (valid)
        {
            pm.elapsed.stop(false);
            auto sec = pm.elapsed.elapsed_sec;
            part_delivery_time += sec;
            part_delivery_time_min = std::min(part_delivery_time_min, sec);
            part_delivery_time_max = std::max(part_delivery_time_max, sec);

            pm.resolve(blk);
        }
        else
        {
            pm.reject(blk);
            res = false;
            // TODO: do we need to also free it from storage?
        }
        blk_delivery_waiting.erase(it);
    }
    return res;
}

promise_t HotStuffBase::async_fetch_blk(const uint256_t &blk_hash,
                                        const PeerId *replica,
                                        bool fetch_now) {
    if (storage->is_blk_fetched(blk_hash))
        return promise_t([this, &blk_hash](promise_t pm){
            pm.resolve(storage->find_blk(blk_hash));
        });
    auto it = blk_fetch_waiting.find(blk_hash);
    if (it == blk_fetch_waiting.end())
    {
#ifdef HOTSTUFF_BLK_PROFILE
        blk_profiler.rec_tx(blk_hash, false);
#endif
        it = blk_fetch_waiting.insert(
            std::make_pair(
                blk_hash,
                BlockFetchContext(blk_hash, this))).first;
    }
    if (replica != nullptr)
        it->second.add_replica(*replica, fetch_now);
    return static_cast<promise_t &>(it->second);
}

promise_t HotStuffBase::async_deliver_blk(const uint256_t &blk_hash,
                                        const PeerId &replica) {
    if (storage->is_blk_delivered(blk_hash))
        return promise_t([this, &blk_hash](promise_t pm) {
            pm.resolve(storage->find_blk(blk_hash));
        });
    auto it = blk_delivery_waiting.find(blk_hash);
    if (it != blk_delivery_waiting.end())
        return static_cast<promise_t &>(it->second);
    BlockDeliveryContext pm{[](promise_t){}};
    it = blk_delivery_waiting.insert(std::make_pair(blk_hash, pm)).first;
    /* otherwise the on_deliver_batch will resolve */
    async_fetch_blk(blk_hash, &replica).then([this, replica](block_t blk) {
        /* qc_ref should be fetched */
        std::vector<promise_t> pms;
        const auto &qc = blk->get_qc();
        assert(qc);
        if (blk == get_genesis())
            pms.push_back(promise_t([](promise_t &pm){ pm.resolve(true); }));
        else
            pms.push_back(blk->verify(this, vpool));
        pms.push_back(async_fetch_blk(qc->get_obj_hash(), &replica));
        /* the parents should be delivered */
        for (const auto &phash: blk->get_parent_hashes())
            pms.push_back(async_deliver_blk(phash, replica));
        promise::all(pms).then([this, blk](const promise::values_t values) {
            auto ret = promise::any_cast<bool>(values[0]) && this->on_deliver_blk(blk);
            if (!ret)
                HOTSTUFF_LOG_WARN("verification failed during async delivery");
        });
    });
    return static_cast<promise_t &>(pm);
}

void HotStuffBase::propose_handler(MsgPropose &&msg, const Net::conn_t &conn) {
    const PeerId &peer = conn->get_peer_id();
    if (peer.is_null()) return;
    msg.postponed_parse(this);
    auto &prop = msg.proposal;
    block_t blk = prop.blk;
    if (!blk) return;
    promise::all(std::vector<promise_t>{
        async_deliver_blk(blk->get_hash(), peer)
    }).then([this, prop = std::move(prop)]() {
        on_receive_proposal(prop);
    });
}

void HotStuffBase::vote_handler(MsgVote &&msg, const Net::conn_t &conn) {
    const auto &peer = conn->get_peer_id();
    if (peer.is_null()) return;
    msg.postponed_parse(this);
    //auto &vote = msg.vote;
    RcObj<Vote> v(new Vote(std::move(msg.vote)));
    promise::all(std::vector<promise_t>{
        async_deliver_blk(v->blk_hash, peer),
        v->verify(vpool),
    }).then([this, v=std::move(v)](const promise::values_t values) {
        if (!promise::any_cast<bool>(values[1]))
            LOG_WARN("invalid vote from %d", v->voter);
        else
            on_receive_vote(*v);
    });
}

void HotStuffBase::req_blk_handler(MsgReqBlock &&msg, const Net::conn_t &conn) {
    const PeerId replica = conn->get_peer_id();
    if (replica.is_null()) return;
    auto &blk_hashes = msg.blk_hashes;
    std::vector<promise_t> pms;
    for (const auto &h: blk_hashes)
        pms.push_back(async_fetch_blk(h, nullptr));
    promise::all(pms).then([replica, this](const promise::values_t values) {
        std::vector<block_t> blks;
        for (auto &v: values)
        {
            auto blk = promise::any_cast<block_t>(v);
            blks.push_back(blk);
        }
        pn.send_msg(MsgRespBlock(blks), replica);
    });
}

void HotStuffBase::resp_blk_handler(MsgRespBlock &&msg, const Net::conn_t &) {
    msg.postponed_parse(this);
    for (const auto &blk: msg.blks)
        if (blk) on_fetch_blk(blk);
}

bool HotStuffBase::conn_handler(const salticidae::ConnPool::conn_t &conn, bool connected) {
    if (connected)
    {
        if (!pn.enable_tls) return true;
        auto cert = conn->get_peer_cert();
        //SALTICIDAE_LOG_INFO("%s", salticidae::get_hash(cert->get_der()).to_hex().c_str());
        return valid_tls_certs.count(salticidae::get_hash(cert->get_der()));
    }
    return true;
}

void HotStuffBase::print_stat() const {
    LOG_INFO("===== begin stats =====");
    LOG_INFO("-------- queues -------");
    LOG_INFO("blk_fetch_waiting: %lu", blk_fetch_waiting.size());
    LOG_INFO("blk_delivery_waiting: %lu", blk_delivery_waiting.size());
    LOG_INFO("decision_waiting: %lu", decision_waiting.size());
    LOG_INFO("-------- misc ---------");
    LOG_INFO("fetched: %lu", fetched);
    LOG_INFO("delivered: %lu", delivered);
    LOG_INFO("cmd_cache: %lu", storage->get_cmd_cache_size());
    LOG_INFO("blk_cache: %lu", storage->get_blk_cache_size());
    LOG_INFO("------ misc (10s) -----");
    LOG_INFO("fetched: %lu", part_fetched);
    LOG_INFO("delivered: %lu", part_delivered);
    LOG_INFO("decided: %lu", part_decided);
    LOG_INFO("gened: %lu", part_gened);
    LOG_INFO("avg. parent_size: %.3f",
            part_delivered ? part_parent_size / double(part_delivered) : 0);
    LOG_INFO("delivery time: %.3f avg, %.3f min, %.3f max",
            part_delivered ? part_delivery_time / double(part_delivered) : 0,
            part_delivery_time_min == double_inf ? 0 : part_delivery_time_min,
            part_delivery_time_max);

    part_parent_size = 0;
    part_fetched = 0;
    part_delivered = 0;
    part_decided = 0;
    part_gened = 0;
    part_delivery_time = 0;
    part_delivery_time_min = double_inf;
    part_delivery_time_max = 0;
#ifdef HOTSTUFF_MSG_STAT
    LOG_INFO("--- replica msg. (10s) ---");
    size_t _nsent = 0;
    size_t _nrecv = 0;
    for (const auto &replica: peers)
    {
        auto conn = pn.get_peer_conn(replica);
        if (conn == nullptr) continue;
        size_t ns = conn->get_nsent();
        size_t nr = conn->get_nrecv();
        size_t nsb = conn->get_nsentb();
        size_t nrb = conn->get_nrecvb();
        conn->clear_msgstat();
        LOG_INFO("%s: %u(%u), %u(%u), %u",
            get_hex10(replica).c_str(), ns, nsb, nr, nrb, part_fetched_replica[replica]);
        _nsent += ns;
        _nrecv += nr;
        part_fetched_replica[replica] = 0;
    }
    nsent += _nsent;
    nrecv += _nrecv;
    LOG_INFO("sent: %lu", _nsent);
    LOG_INFO("recv: %lu", _nrecv);
    LOG_INFO("--- replica msg. total ---");
    LOG_INFO("sent: %lu", nsent);
    LOG_INFO("recv: %lu", nrecv);
#endif
    LOG_INFO("====== end stats ======");
}

HotStuffBase::HotStuffBase(uint32_t blk_size,
                    ReplicaID rid,
                    privkey_bt &&priv_key,
                    NetAddr listen_addr,
                    pacemaker_bt pmaker,
                    EventContext ec,
                    size_t nworker,
                    const Net::Config &netconfig):
        HotStuffCore(rid, std::move(priv_key)),
        listen_addr(listen_addr),
        blk_size(blk_size),
        ec(ec),
        tcall(ec),
        vpool(ec, nworker),
        pn(ec, netconfig),
        pmaker(std::move(pmaker)),

        fetched(0), delivered(0),
        nsent(0), nrecv(0),
        part_parent_size(0),
        part_fetched(0),
        part_delivered(0),
        part_decided(0),
        part_gened(0),
        part_delivery_time(0),
        part_delivery_time_min(double_inf),
        part_delivery_time_max(0)
{
    /* register the handlers for msg from replicas */
    pn.reg_handler(salticidae::generic_bind(&HotStuffBase::propose_handler, this, _1, _2));
    pn.reg_handler(salticidae::generic_bind(&HotStuffBase::vote_handler, this, _1, _2));
    pn.reg_handler(salticidae::generic_bind(&HotStuffBase::req_blk_handler, this, _1, _2));
    pn.reg_handler(salticidae::generic_bind(&HotStuffBase::resp_blk_handler, this, _1, _2));
    pn.reg_conn_handler(salticidae::generic_bind(&HotStuffBase::conn_handler, this, _1, _2));
    pn.reg_error_handler([](const std::exception_ptr _err, bool fatal, int32_t async_id) {
        try {
            std::rethrow_exception(_err);
        } catch (const std::exception &err) {
            HOTSTUFF_LOG_WARN("network async error: %s\n", err.what());
        }
    });
    // HOTSTUFF_LOG_INFO("===========checkpoint0=============");
    pn.start();
    pn.listen(listen_addr);
}

void HotStuffBase::do_broadcast_proposal(const Proposal &prop) {
    pn.multicast_msg(MsgPropose(prop), peers);
    // leader's addition of orderedlist should be done here.
    command_timestamp_storage->refresh_available_cmds(prop.blk->get_proposed_orderedlist().convert_to_vec());
    orderedlist_t self_orderedlist = command_timestamp_storage->get_orderedlist(prop.blk->get_hash(), blk_size);
    orderedlist_storage->add_ordered_list(prop.blk->get_hash(), *self_orderedlist, true, num_peers());
}

void HotStuffBase::do_vote(ReplicaID last_proposer, const Vote &dummy_vote)
{
    // HOTSTUFF_LOG_PROTO("Inside do vote!");
    // WARN - Here the data inside replica_preferred_orderedlist is not returned on calling extract_cmds()
    //std::vector<uint256_t> test_cmds = vote.replica_preferred_orderedlist->extract_cmds();
    //HOTSTUFF_LOG_PROTO("The size inside do_vote before pmaker is: %lu", test_cmds.size());
    pmaker->beat_resp(last_proposer)
        .then([this, dummy_vote](ReplicaID proposer) {
            if (proposer == get_id())
            {
                throw HotStuffError("unreachable line");
                //on_receive_vote(vote);
            }
            else
            {
                //HOTSTUFF_LOG_PROTO("Replica id is: %d",vote.voter);
                //ReplicaID voterID = std::move(vote.voter);
                //HOTSTUFF_LOG_PROTO("Replica id test is: %d", voterID);
                uint256_t vote_blk_hash = std::move(dummy_vote.blk_hash);
                //HOTSTUFF_LOG_PROTO("Block hash is: %s", get_hex10(blk_hash_test).c_str());
                //HOTSTUFF_LOG_PROTO("Part cert is: %s", get_hex10(vote.cert->get_obj_hash()).c_str());
                //part_cert_bt certificate = create_part_cert(*priv_key, blk_hash_test);
                //HOTSTUFF_LOG_PROTO("Part test cert is: %s", get_hex10(certificate->get_obj_hash()).c_str());
                // WARN - Here the data inside replica_preferred_orderedlist is not returned on calling extract_cmds()
                //orderedlist_t replica_orderedlist = command_timestamp_storage->get_orderedlist(blk_hash_test);
                Vote vote = Vote(std::move(dummy_vote.voter), vote_blk_hash, create_part_cert(*priv_key, vote_blk_hash), command_timestamp_storage->get_orderedlist(vote_blk_hash, blk_size), this);
                //vote.preferredOrderedListPrintout();
		//HOTSTUFF_LOG_PROTO("The size inside do_vote  after pmakeris: %lu", vote_test.replica_preferred_orderedlist->extract_cmds().size());
                //for(auto &ts: vote_test.replica_preferred_orderedlist->extract_timestamps()) {
                //    HOTSTUFF_LOG_PROTO("The ts sent is: %s", boost::lexical_cast<std::string>(ts).c_str());
                //}
                // HOTSTUFF_LOG_PROTO("The size inside do_vote  after pmakeris: %lu", vote.replica_preferred_orderedlist->extract_cmds().size());
                pn.send_msg(MsgVote(vote), get_config().get_peer_id(proposer));
            }
        });
}

void HotStuffBase::do_consensus(const block_t &blk) {
    pmaker->on_consensus(blk);
}

void HotStuffBase::do_decide(Finality &&fin) {
    part_decided++;
    state_machine_execute(fin);
    auto it = decision_waiting.find(fin.cmd_hash);
    if (it != decision_waiting.end())
    {
        it->second(std::move(fin));
        decision_waiting.erase(it);
    }
}

HotStuffBase::~HotStuffBase() {}

//proposed_orderlist[0] is the orderlist of the leader before the leader receive other replicas' ordered list
//return a vector, which will be a list of orderedlist
//"timestamps" in these returned orderedlist are useless, cmds in one orderedlist should be in one block
LeaderProposedOrderedList HotStuffBase::aequitas_order(Aequitas::TopologyGraph<max_num_all_txn> &G, 
                                        std::vector<OrderedList> &proposed_orderlist,
                                        int n_f)
// hotstuff::LeaderProposedOrderedList aequitas_order(Aequitas::TopologyGraph<max_num_all_txn> &G, 
                                        // std::vector<hotstuff::OrderedList> &proposed_orderlist,
                                        // int n_f)
{
    /*
    HOTSTUFF_LOG_INFO("Begin printing replica's orderlist.");
    for(int i = 0; i < proposed_orderlist.size(); i++)
    {
        proposed_orderlist[i].printout();
    }
    HOTSTUFF_LOG_INFO("Finish printing replica's orderlist.");
    */
    int n_replica = proposed_orderlist.size();
    if(n_replica == 0) 
        throw std::runtime_error("the number of replica is 0.");
    
    //sort all the cmds
    for (int i = 0; i < n_replica; i++) proposed_orderlist[i].sort_cmds();

    //map the cmd to a number
    int distinct_cmd = -1, distinct_cmd_r = -2;

    
    // int matric_before[max_num_all_txn][max_num_all_txn];
    std::vector< std::vector<int> > matric_before;
    // std::memset(matric_before, 0, sizeof(matric_before));
    matric_before.resize(max_num_all_txn, std::vector<int>(max_num_all_txn));
    // std::cout << "matric_before has been resized..." << std::endl;
    for(int pr = 0; pr < n_replica; pr++)
    {
        
        int n_cmds = proposed_orderlist[pr].cmds.size();
        if(n_cmds == 0 || n_cmds != proposed_orderlist[pr].timestamps.size())
            throw std::runtime_error("no cmds to be ordered or cmds not in the right form.");

        // std::cout << "dealing with replica " << pr << " n_cmds = " << n_cmds  << std::endl;


        for (int i = 0; i < n_cmds; i++)
        {
            uint256_t cmd_i = proposed_orderlist[pr].cmds[i];
            // std::cout << "cmd_i = " << cmd_i << " ";
            if ( HotStuffCore::map_cmd.find(cmd_i) ==  HotStuffCore::map_cmd.end())
            {
                 HotStuffCore::num_of_all_cmds++;
                 HotStuffCore::map_cmd[cmd_i].cmd_et.start();
                 //map_cmd will directly map the content of the cmd to the property it has
                 HotStuffCore::map_cmd[cmd_i].id = HotStuffCore::num_of_all_cmds;
                 HotStuffCore::map_cmd[cmd_i].is_solid = HotStuffCore::map_cmd[cmd_i].appear = 1;
                 HotStuffCore::map_cmd[cmd_i].belonged_graph = HotStuffCore::round_number;

                 if( HotStuffCore::map_cmd[cmd_i].appear >= 2 * n_f + 1)
                         HotStuffCore::map_cmd[cmd_i].is_solid = 3;
                 else if( HotStuffCore::map_cmd[cmd_i].appear >= n_f + 1)
                         HotStuffCore::map_cmd[cmd_i].is_solid = 2;

                 //Attention: cmd_content is 0-base, use cmd_content[id-1].
                 //cmd_content is saving the concrete command content for each coming cmd
                 HotStuffCore::cmd_content.push_back(cmd_i);

                if(distinct_cmd == -1) distinct_cmd =  HotStuffCore::num_of_all_cmds;
                distinct_cmd_r =  HotStuffCore::num_of_all_cmds;
            } else {
                if( HotStuffCore::round_number ==  HotStuffCore::map_cmd[cmd_i].belonged_graph) {
                     HotStuffCore::map_cmd[cmd_i].appear++;
                    if( HotStuffCore::map_cmd[cmd_i].appear >= 2 * n_f + 1)
                         HotStuffCore::map_cmd[cmd_i].is_solid = 3;
                    else if( HotStuffCore::map_cmd[cmd_i].appear >= n_f + 1)
                         HotStuffCore::map_cmd[cmd_i].is_solid = 2;
                }
            }
        }

        // std::cout << " num_of_all_cmds = " << HotStuffCore::num_of_all_cmds << std::endl;
        // std::cout << "initialization of setting property for cmds is done..." << std::endl;

        //make sure that in every proposed orderlist, one transaction will not appear more than once
        for (int i = 0; i < n_cmds; i++)
        {
            for (int j = i + 1; j < n_cmds; j++)
            {
                // std::cout << "i = " << i << " j = " << j << std::endl;
                hotstuff::cmd_with_type curi =  HotStuffCore::map_cmd[proposed_orderlist[pr].cmds[i]];
                hotstuff::cmd_with_type curj =  HotStuffCore::map_cmd[proposed_orderlist[pr].cmds[j]];

                if(curi.belonged_graph != curj.belonged_graph) continue;

                if(curi.belonged_graph ==  HotStuffCore::round_number)
                {
                    if(curi.is_solid >= 2 && curj.is_solid >= 2)
                    {
                        int ii = curi.id,  jj = curj.id;
                        if(ii == jj) continue;
                        matric_before[ii][jj] += 1;
                    }
                } else
                {
                    int ii = curi.id,  jj = curj.id;
                    if(ii == jj) continue;
                    if( HotStuffCore::allG[curi.belonged_graph].edge[ii][jj] == 1
                        ||  HotStuffCore::allG[curi.belonged_graph].edge[jj][ii] == 1) continue;
                    matric_before[ii][jj] += 1;
                }
            }
        }

        // std::cout << "initialization done..." << std::endl;

        //addedge for previous G.
        for (int i = 0; i < n_cmds; i++)
        {
            for (int j = i + 1; j < n_cmds; j++)
            {
                hotstuff::cmd_with_type curi =  HotStuffCore::map_cmd[proposed_orderlist[pr].cmds[i]];
                hotstuff::cmd_with_type curj =  HotStuffCore::map_cmd[proposed_orderlist[pr].cmds[j]];

                if(curi.belonged_graph != curj.belonged_graph) continue;

                if(curi.belonged_graph !=  HotStuffCore::round_number)
                {
                    int ii = curi.id, jj = curj.id;
                    if(ii == jj) continue;
                    if( HotStuffCore::allG[curi.belonged_graph].edge[ii][jj] == 1
                        ||  HotStuffCore::allG[curi.belonged_graph].edge[jj][ii] == 1) continue;
                    if(matric_before[ii][jj] >= matric_before[jj][ii] 
                        && matric_before[ii][jj] >= n_f + 1)
                        {
                            // std::cout << "Addedge for graph " << curi.belonged_graph << std::endl;
                            HotStuffCore::allG[curi.belonged_graph].addedge(ii, jj);
                        }
                    else if(matric_before[jj][ii] > matric_before[ii][jj] 
                        && matric_before[jj][ii] >= n_f + 1)
                        {
                            // std::cout << "Addedge for graph " << curi.belonged_graph << std::endl;
                            HotStuffCore::allG[curi.belonged_graph].addedge(jj, ii);
                        }
                         
                }
            }
        }
        // std::cout << "addedge done..." << std::endl;
       
    }

    // decide how many cmds are is_solid for this G
    G.num_solid = 0;
    for(int i = distinct_cmd; i <= distinct_cmd_r; i++)
    {
        if(HotStuffCore::map_cmd[HotStuffCore::cmd_content[i - 1]].is_solid >= 2)
        {
            // std::cout<<i<<" is solid_cmd, solid = " 
            //     << HotStuffCore::map_cmd[HotStuffCore::cmd_content[i - 1]].is_solid 
            //     << " appear = " << HotStuffCore::map_cmd[HotStuffCore::cmd_content[i - 1]].appear 
            //     << " n_f+1 = " << n_f + 1 << std::endl;
            G.num_solid = G.num_solid + 1;
        }
    }

    // std::cout << "begin to add edge for new G..." << std::endl;
    //decide whether we should add an edge from cmd_j to cmd_i for current G
    for(int i = distinct_cmd; i <= distinct_cmd_r; i++)
        for(int j = i + 1; j <= distinct_cmd_r; j++)
        {
            // if(i == j) continue;
            // k1 = matric_before[i][j], k2 = matric_before[j][i]
            // if(max(k1,k2)>=n_faulty+1) addedge for the larger one
            // std::cout << "i = " << i << " j = " << j << std::endl;
            // std::cout << "matric_before[i][j] = " << matric_before[i][j]
                //  << " matric_before[j][i] = " << matric_before[j][i] << std::endl;
            if(matric_before[i][j] >= matric_before[j][i]
                && matric_before[i][j] >= n_f + 1)
                {
                    // std::cout<<"matric_before[i][j]="<<matric_before[i][j]<<" "
                        // <<"matric_before[j][i]="<<matric_before[j][i]<<std::endl;
                    // std::cout << "Addedge for graph " << HotStuffCore::round_number << std::endl;
                    G.addedge(i, j);
                }
            else if(matric_before[j][i] > matric_before[i][j]
                && matric_before[j][i] >= n_f + 1)
                {
                    // std::cout<<"matric_before[j][i]="<<matric_before[j][i]<<" "
                        // <<"matric_before[i][j]="<<matric_before[i][j]<<std::endl;
                    // std::cout << "Addedge for graph " << HotStuffCore::round_number << std::endl;
                    G.addedge(j, i);
                }
                //TODO: remember to mark those transactions appears more than n_f+1, and regardless those without mark
        }    
    // std::cout << "addedge for new G is done..." << std::endl;
    G.distinct_cmd = distinct_cmd; // The range of nodes in current G: [distinct_cmd, distinct_cmd_r]
    G.distinct_cmd_r = distinct_cmd_r;


    std::vector<uint256_t> final_ordered_cmds; final_ordered_cmds.clear();
    LOG_INFO("round_number = %d, passed_number = %d\n", HotStuffCore::round_number, HotStuffCore::passed_round);
    // std::cout<< "round_number = " << HotStuffCore::round_number 
        // << " passed_number = " << HotStuffCore::passed_round << std::endl;
    for(int cur_round =  HotStuffCore::round_number - 1; cur_round >  HotStuffCore::passed_round; cur_round--)
    {
        // std::cout<<"cur round = " << cur_round << " " << HotStuffCore::allG[cur_round].is_tournament() <<std::endl;
        if( HotStuffCore::allG[cur_round].is_tournament())
        {
            // std::cout<<"Yeah encounter a tournament"<<std::endl;
            for(int cur =  HotStuffCore::passed_round + 1; cur <= cur_round; cur++)
            {
                // std::cout << "cur = " << cur << std::endl;
                std::vector<uint256_t> cur_ordered_cmds =  HotStuffCore::allG[cur - 1].finalize( HotStuffCore::cmd_content);
                HOTSTUFF_LOG_INFO("avg_circle_size = %lf, max_circle_size = %d, min_circle_size = %d", 
                    allG[cur - 1].avg_circle_size, allG[cur - 1].max_circle_size, allG[cur - 1].min_circle_size);
                for(int i = 0; i < cur_ordered_cmds.size(); i++)
                {
                    final_ordered_cmds.push_back(cur_ordered_cmds[i]);
                    //output the time for one cmd transfer from dotted transaction to solid transaction 
                    char fmt[64];
                    HotStuffCore::map_cmd[cur_ordered_cmds[i]].cmd_et.stop();
                    struct timeval tv;
                    gettimeofday(&tv, nullptr);
                    struct tm *tmp = localtime(&tv.tv_sec);
                    strftime(fmt, sizeof fmt, "%Y-%m-%d %H:%M:%S.%%06u [hotstuff cmd info] %%.6f\n", tmp);
                    fprintf(stderr, fmt, 
                        tv.tv_usec, HotStuffCore::map_cmd[cur_ordered_cmds[i]].cmd_et.elapsed_sec);
                        
                }
                    
            }
             HotStuffCore::passed_round = cur_round;
            break;
        } else {
            // std::cout << "cur_round = " << cur_round << " " << HotStuffCore::allG[cur_round].num_of_edges << std::endl;
        }
    }

    // std::cout << "Finish checking tournament at this round." << std::endl;
    // std::cout << "G.is_tournament = " << G.is_tournament() << std::endl;
    if((HotStuffCore::passed_round ==  HotStuffCore::round_number - 1) && G.is_tournament())
    {
        // std::cout << "first line, ready! " << std::endl;
        std::vector<uint256_t> cur_ordered_cmds = G.finalize( HotStuffCore::cmd_content);
        HOTSTUFF_LOG_INFO("avg_circle_size = %lf, max_circle_size = %d, min_circle_size = %d", 
                    G.avg_circle_size, G.max_circle_size, G.min_circle_size);
        // std::cout << "second line, ready! " << std::endl;
        for(int i = 0; i < cur_ordered_cmds.size(); i++)
        {
            final_ordered_cmds.push_back(cur_ordered_cmds[i]);
            //output the time for one cmd transfer from dotted transaction to solid transaction 
            char fmt[64];
            HotStuffCore::map_cmd[cur_ordered_cmds[i]].cmd_et.stop();
            struct timeval tv;
            gettimeofday(&tv, nullptr);
            struct tm *tmp = localtime(&tv.tv_sec);
            strftime(fmt, sizeof fmt, "%Y-%m-%d %H:%M:%S.%%06u [hotstuff cmd info] %%.6f\n", tmp);
            fprintf(stderr, fmt, 
                tv.tv_usec, HotStuffCore::map_cmd[cur_ordered_cmds[i]].cmd_et.elapsed_sec);
        }
                
         HotStuffCore::passed_round =  HotStuffCore::round_number;
    }

    // std::cout << "Going to return the final proposed orderlist." << std::endl;

    hotstuff::LeaderProposedOrderedList final_ordered_vector(final_ordered_cmds);
    return final_ordered_vector;
    
}



void HotStuffBase::start(
        std::vector<std::tuple<NetAddr, pubkey_bt, uint256_t>> &&replicas,
        bool ec_loop) {
    // HOTSTUFF_LOG_INFO("===========checkpoint1=============");
    for (size_t i = 0; i < replicas.size(); i++)
    {
        auto &addr = std::get<0>(replicas[i]);
        auto cert_hash = std::move(std::get<2>(replicas[i]));
        valid_tls_certs.insert(cert_hash);
        auto peer = pn.enable_tls ? salticidae::PeerId(cert_hash) : salticidae::PeerId(addr);
        HotStuffCore::add_replica(i, peer, std::move(std::get<1>(replicas[i])));
        if (addr != listen_addr)
        {
            peers.push_back(peer);
            pn.add_peer(peer);
            pn.set_peer_addr(peer, addr);
            pn.conn_peer(peer);
        }
    }

    /* ((n - 1) + 1 - 1) / 3 */
    uint32_t nfaulty = peers.size() / 3;
    if (nfaulty == 0)
        LOG_WARN("too few replicas in the system to tolerate any failure");
    on_init(nfaulty);
    pmaker->init(this);
    if (ec_loop)
        ec.dispatch();
    // HOTSTUFF_LOG_INFO("===========checkpoint2=============");
    cmd_pending.reg_handler(ec, [this](cmd_queue_t &q) {
        // HOTSTUFF_LOG_INFO("===========checkpoint3=============");
        std::pair<uint256_t, commit_cb_t> e;
        while (q.try_dequeue(e))
        {
            // HOTSTUFF_LOG_INFO("===========checkpoint4=============");
            ReplicaID proposer = pmaker->get_proposer();

            const auto &cmd_hash = e.first;
            auto it = decision_waiting.find(cmd_hash);
            if (it == decision_waiting.end())
                it = decision_waiting.insert(std::make_pair(cmd_hash, e.second)).first;
            else
                e.second(Finality(id, 0, 0, 0, cmd_hash, uint256_t()));
            if (proposer != get_id())
                continue;
            cmd_pending_buffer.push_back(cmd_hash);
            if (cmd_pending_buffer.size() >= blk_size)
            {
                // std::vector<uint256_t> cmds;
                // for (uint32_t i = 0; i < blk_size; i++)
                // {
                //     cmds.push_back(cmd_pending_buffer.front());
                //     // HOTSTUFF_LOG_PROTO("command being included in proposal to be sent is: %s", get_hex10(cmd_pending_buffer.front()).c_str());
                //     cmd_pending_buffer.erase(cmd_pending_buffer.begin());
                // }

                
                pmaker->beat().then([this](ReplicaID proposer) {
                    
                    // The following part is for Aequitas
                    /*
                    uint256_t block_hash = pmaker->get_parents()[0]->get_hash();
                    // HOTSTUFF_LOG_INFO("The parent_block is: %s", get_hex10(block_hash).c_str());
                    LeaderProposedOrderedList proposed_orderedlist;
                    if (block_hash != this->get_genesis_hash())
                    {
                        // applying Aequitas to get the proposed ordering
                        Aequitas::TopologyGraph<max_num_all_txn> G;
                        salticidae::ElapsedTime computation_et;
                        computation_et.start();
                        std::vector<OrderedList> replica_orderedlists =
                                this->orderedlist_storage->get_set_of_orderedlists(block_hash);

                        HotStuffCore::round_number += 1;
                        proposed_orderedlist = HotStuffBase::aequitas_order(G, replica_orderedlists, 1);
                        HotStuffCore::allG.push_back(G);
                        
                        for (auto cmd: proposed_orderedlist.convert_to_vec()) 
                        {
                            auto it = std::find(cmd_pending_buffer.begin(), cmd_pending_buffer.end(), cmd);
                            auto index = std::distance(cmd_pending_buffer.begin(), it);
                            cmd_pending_buffer.erase(cmd_pending_buffer.begin() + index);
                        }
                        proposed_orderedlist.print_out();
                        //output the elapse time for computation
                        computation_et.stop();
                        struct timeval tv;
                        gettimeofday(&tv, nullptr);
                        std::pair<struct timeval, double> computation_elapsed 
                                    = std::make_pair(tv, computation_et.elapsed_sec);
                        char fmt[64];
                        struct tm *tmp = localtime(&tv.tv_sec);
                        strftime(fmt, sizeof fmt, "%Y-%m-%d %H:%M:%S.%%06u [hotstuff computation info] %%.6f\n", tmp);
                        fprintf(stderr, fmt, computation_elapsed.first.tv_usec, computation_elapsed.second);
                        
                    }
                    else 
                    {
                        // beginning proposal, no need for aequitas
                        HOTSTUFF_LOG_INFO("It is a beginning proposal!");
			            std::vector<uint256_t> proposed_ranked_cmds;
                        for (uint32_t i = 0; i < blk_size; i++)
                        {
                            proposed_ranked_cmds.push_back(cmd_pending_buffer.front());
                            cmd_pending_buffer.erase(cmd_pending_buffer.begin());
                        }
                        // std::vector<std::vector<uint256_t>> proposed_ranked_cmds;
                        // for(auto cmd: cmds) {
                            // proposed_ranked_cmds.push_back(cmd);
                        // }
                        proposed_orderedlist = LeaderProposedOrderedList(proposed_ranked_cmds);
                        proposed_orderedlist.print_out();
                    }
                   */
                    // The following part is for original-libhotstuff
                    LeaderProposedOrderedList proposed_orderedlist;
                    HOTSTUFF_LOG_INFO("Begin for proposal!");
                    std::vector<uint256_t> proposed_ranked_cmds;
                    for (uint32_t i = 0; i < blk_size; i++)
                    {
                        proposed_ranked_cmds.push_back(cmd_pending_buffer.front());
                        cmd_pending_buffer.erase(cmd_pending_buffer.begin());
                    }
                    // std::vector<std::vector<uint256_t>> proposed_ranked_cmds;
                    // for(auto cmd: cmds) {
                        // proposed_ranked_cmds.push_back(cmd);
                    // }
                    proposed_orderedlist = LeaderProposedOrderedList(proposed_ranked_cmds);
                    proposed_orderedlist.print_out();


                    if (proposer == get_id()){
			HOTSTUFF_LOG_INFO("The proposer id is: %d", get_id());
                        on_propose(pmaker->get_parents(), proposed_orderedlist);
		    }
                });
                return true;
            }
        }
        return false;
    });
}

}
