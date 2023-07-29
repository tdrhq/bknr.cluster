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

  typedef void transaction_callback(BknrStateMachine* fsm, int callback_handle, int success,
                                    const char* status_message);
  typedef int LispCallback;

  class BknrClosure;

  typedef void InvokeClosure(BknrClosure* closure,
                             int ok,
                             const char* err);

  typedef void DeleteClosure(BknrClosure* closure);

  class BknrClosure : public braft::Closure {
  public:
    BknrClosure(InvokeClosure* invoke_closure,
                DeleteClosure* delete_closure)
      : _invoke_closure(invoke_closure),
        _delete_closure(delete_closure)
      {}

    ~BknrClosure() {
      LOG(INFO) << "Deleting closure!";
      (*_delete_closure)(this);
    }

    void Run();
public:
    InvokeClosure* _invoke_closure;
    DeleteClosure* _delete_closure;
  };

  typedef int OnApplyCallback(BknrStateMachine* fsm,
                              butil::IOBuf* data,
                              int datalen,
                              /* we need this to respond with the result */
                              BknrClosure *closure);

  typedef void OnSnapshotSave(BknrStateMachine* fsm,
                              braft::SnapshotWriter* writer,
                              braft::Closure* done);

  typedef int OnSnapshotLoad(BknrStateMachine* fsm,
                             braft::SnapshotReader* reader);

  typedef void OnLeaderStart(BknrStateMachine* fsm);
  typedef void OnLeaderStop(BknrStateMachine* fsm);

  class BknrStateMachine : public braft::StateMachine {
    OnApplyCallback* _on_apply_callback;
    OnSnapshotSave* _on_snapshot_save;
    OnSnapshotLoad* _on_snapshot_load;
    OnLeaderStart* _on_leader_start;
    OnLeaderStop* _on_leader_stop;

  public:
    BknrStateMachine (
      OnApplyCallback* on_apply_callback,
      OnSnapshotSave* on_snapshot_save,
      OnSnapshotLoad* on_snapshot_load,
      OnLeaderStart* on_leader_start,
      OnLeaderStop* on_leader_stop
      ) : _on_apply_callback(on_apply_callback),
          _on_snapshot_save(on_snapshot_save),
          _on_snapshot_load(on_snapshot_load),
          _on_leader_start(on_leader_start),
          _on_leader_stop(on_leader_stop),
          _node(NULL) {
    }

    void on_apply(::braft::Iterator& iter) {
      for (; iter.valid(); iter.next()) {
        braft::AsyncClosureGuard done_guard(iter.done());

        butil::IOBuf data = iter.data();

        int len = data.length();
        LOG(INFO) << "Calling OnApplyCallback";

        BknrClosure* c = NULL;
        if (iter.done()) {
          c = dynamic_cast<BknrClosure*>(iter.done());
        }

        int status = (*_on_apply_callback)(this, &data, len, c);
        if (!status) {
          iter.done()->status().set_error(1, "Failed to apply transaction");
        }
      }
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

    void on_snapshot_save(braft::SnapshotWriter* writer,
                          braft::Closure* done) {
      (*_on_snapshot_save)(this, writer, done);
    }

    int on_snapshot_load(braft::SnapshotReader* reader) {
      return (*_on_snapshot_load)(this, reader);
    }

    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
        (*_on_leader_start)(this);
    }

    void on_leader_stop(const butil::Status& status) {
        (*_on_leader_stop)(this);
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    butil::atomic<int64_t> _leader_term;

    void apply(const char* data, int data_len, BknrClosure* closure) {
      // Serialize request to the replicated write-ahead-log so that all the
      // peers in the group receive this request as well.
      // Notice that _value can't be modified in this routine otherwise it
      // will be inconsistent with others in this group.

      // Serialize request to IOBuf
      //const int64_t term = _leader_term.load(butil::memory_order_relaxed);
      /*
        This logic was copied from examples, but our logic for
        switching servers is different.
        if (term < 0) { return
        redirect(response); } */
      const int64_t term = _leader_term.load(butil::memory_order_relaxed);


      butil::IOBuf log;
      log.append(data, data_len);

      // Apply this log as a braft::Task
      braft::Task task;
      task.data = &log;
      // This callback would be iovoked when the task actually excuted or
      // fail
      task.done = closure;

      if (term < 0) {
              brpc::ClosureGuard closure_guard(task.done);
              LOG(ERROR) << "Attempting to to apply task when we're not a leader";
              task.done->status().set_error(1, "Attempting to apply task when we're not a leader");
              return;
      }


      // ABA problem can be avoid if expected_term is set
      task.expected_term = term;

      // Now the task is applied to the group, waiting for the result.
      return _node->apply(task);
    }

  public:
    brpc::Server _server;
    braft::Node* _node;
  private:
  };

  void BknrClosure::Run() {
    // If the status was okay, then we would've called the callback in
    // on_apply (see the lisp code). But we need to propagate errors
    // back too.
    (*_invoke_closure)(this,
                       status().ok(),
                       status().error_cstr());
    delete this;
  }

  extern "C" {
    BknrStateMachine* make_bknr_state_machine(OnApplyCallback* on_apply_callback,
                                              OnSnapshotSave* on_snapshot_save,
                                              OnSnapshotLoad* on_snapshot_load,
                                              OnLeaderStart* on_leader_start,
                                              OnLeaderStop* on_leader_stop) {
      return new BknrStateMachine(on_apply_callback,
                                  on_snapshot_save,
                                  on_snapshot_load,
                                  on_leader_start,
                                  on_leader_stop);
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

    int bknr_is_leader(BknrStateMachine* fsm) {
      return fsm->_node->is_leader();
    }


    void bknr_apply_transaction(BknrStateMachine* fsm, const char* data, int data_len, BknrClosure* closure) {
      fsm->apply(data, data_len, closure);
    }

    void bknr_iobuf_copy_to(butil::IOBuf* buf, void* arr, int len) {
      LOG(INFO) << "Copying IOBuf";
      buf->copy_to(arr, len);
    }

    void bknr_closure_run(google::protobuf::Closure *closure) {
      brpc::ClosureGuard guard(closure);
    }

    void bknr_closure_set_error(braft::Closure *closure, int error, const char* msg) {
      closure->status().set_error(error, msg);
    }

    void bknr_set_log_level(int level) {
      ::logging::SetMinLogLevel(level);
    }

    BknrClosure* bknr_make_closure(InvokeClosure* invokeClosure, DeleteClosure* deleteClosure) {
      return new BknrClosure(invokeClosure, deleteClosure);
    }

    void bknr_snapshot(BknrStateMachine* fsm, braft::Closure* done) {
      fsm->_node->snapshot(done);
    }

    char* return_cstr(const std::string& path) {
      char *res = (char*) malloc(path.length() + 10);
      path.copy(res, path.length());
      res[path.length()] = '\0';
      return res;
    }

    char*  bknr_snapshot_writer_get_path(braft::SnapshotWriter* snapshot_writer) {
      return return_cstr(snapshot_writer->get_path());
    }

    int bknr_snapshot_writer_add_file(braft::SnapshotWriter* snapshot_writer, const char* file) {
      LOG(ERROR) << "Adding snapshot file: " << file;
      return snapshot_writer->add_file(file);
    }

    char* bknr_snapshot_reader_get_path(braft::SnapshotReader* snapshot_reader) {
      return return_cstr(snapshot_reader->get_path());
    }
  }
}
