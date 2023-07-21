/* -*- c-basic-offset: 2; -*- */

#include <butil/endpoint.h>
#include <brpc/controller.h>       // brpc::Controller
#include <brpc/server.h>           // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile

namespace bknr {

  using std::string;


  class BknrStateMachine;

  typedef void transaction_callback(BknrStateMachine* fsm, int callback_handle, bool success,
                                    const char* status_message);

  class BknrClosure : public braft::Closure {
  public:
    BknrClosure(BknrStateMachine* fsm,
                int callbackHandle,
                transaction_callback* callback)
      : _fsm(fsm),
        _callback(callback),
        _callbackHandle(callbackHandle)
    {}

    void Run();
private:
    BknrStateMachine* _fsm;
    transaction_callback* _callback;
    int _callbackHandle;
  };

  class BknrStateMachine : public braft::StateMachine {
  public:
    BknrStateMachine () : _node(NULL) {
    }

    void on_apply(::braft::Iterator& iter) {
    }

    int start(string ip,
              int port, string config,
              int election_timeout_ms,
              int snapshot_interval,
              string data_path,
              string group) {
      butil::ip_t ipt;
      butil::str2ip(ip.c_str(), &ipt);
      butil::EndPoint addr(ipt, port);
      braft::NodeOptions node_options;
      if (node_options.initial_conf.parse_from(config) != 0) {
            LOG(ERROR) << "Fail to parse configuration `" << config << '\'';
            return -1;
        }
        node_options.election_timeout_ms = election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = snapshot_interval;
        std::string prefix = "local://" + data_path;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = false;
        braft::Node* node = new braft::Node(group, braft::PeerId(addr));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        return 0;
    }

    void shutdown() {
      if (_node) {
        _node->shutdown(NULL);
      }
    }

    void join() {
      if (_node) {
        _node->join();
      }
    }

    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }

    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    butil::atomic<int64_t> _leader_term;

    void apply(const char* data, int data_len, transaction_callback* callback, int callbackHandle) {
      // Serialize request to the replicated write-ahead-log so that all the
      // peers in the group receive this request as well.
      // Notice that _value can't be modified in this routine otherwise it
      // will be inconsistent with others in this group.

      // Serialize request to IOBuf
      const int64_t term = _leader_term.load(butil::memory_order_relaxed);
      /*
        This logic was copied from examples, but our logic for
        switching servers is different.
        if (term < 0) { return
        redirect(response); } */
      butil::IOBuf log;
      log.append(data, data_len);

      // Apply this log as a braft::Task
      braft::Task task;
      task.data = &log;
      // This callback would be iovoked when the task actually excuted or
      // fail
      task.done = new BknrClosure(this, callbackHandle, callback);
      if (true /*FLAGS_check_term*/) {
        // ABA problem can be avoid if expected_term is set
        task.expected_term = term;
      }
      // Now the task is applied to the group, waiting for the result.
      return _node->apply(task);
    }

  public:
    brpc::Server _server;
    braft::Node* _node;
  private:
  };

  void BknrClosure::Run() {
    if (status().ok()) {
      (*_callback)(_fsm,
                _callbackHandle,
                status().ok(),
                status().error_cstr());
    }
  }

  extern "C" {
    BknrStateMachine* make_bknr_state_machine() {
      return new BknrStateMachine();
    }

    void destroy_bknr_state_machine(BknrStateMachine* fsm) {
      delete fsm;
    }

    int start_bknr_state_machine(BknrStateMachine* fsm,
                                 const char* ip,
                                 int port, const char* config,
                                 int election_timeout_ms,
                                 int snapshot_interval,
                                 const char* data_path,
                                 const char* group) {
      if (braft::add_service(&(fsm->_server), port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
      }

      if (fsm->_server.Start(port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
      }

      return fsm->start(ip, port, config,
                        election_timeout_ms,
                        snapshot_interval,
                        data_path,
                        group);
    }

    void stop_bknr_state_machine(BknrStateMachine* fsm) {
      fsm->shutdown();
      fsm->join();
      fsm->_server.Stop(0);
    }

    bool bknr_is_leader(BknrStateMachine* fsm) {
      return fsm->_node->is_leader();
    }


    void bknr_apply_transaction(BknrStateMachine* fsm, const char* data, int data_len, transaction_callback* callback,
                                int callbackHandle) {
      fsm->apply(data, data_len, callback,
                 callbackHandle);
    }
  }
}
