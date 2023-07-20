/* -*- c-basic-offset: 2; -*- */

#include <brpc/controller.h>       // brpc::Controller
#include <brpc/server.h>           // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile

namespace bknr {
  class BknrStateMachine : public braft::StateMachine {
    int lispHandle;
  public:
    BknrStateMachine (int _lispHandle) : lispHandle(_lispHandle) {
    }

    void on_apply(::braft::Iterator& iter) {
    }
  };







  extern "C" {
    BknrStateMachine* make_bknr_state_machine(int lispHandle) {
      return new BknrStateMachine(lispHandle);
    }
  }
}
