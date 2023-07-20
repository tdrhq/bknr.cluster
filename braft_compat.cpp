/* -*- c-basic-offset: 2; -*- */

#include <brpc/controller.h>       // brpc::Controller
#include <brpc/server.h>           // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile

namespace bknr {

  typedef int lisp_state_machine;

  class BknrStateMachine : public braft::StateMachine {
    lisp_state_machine lispHandle;
  public:
    BknrStateMachine (lisp_state_machine  _lispHandle) : lispHandle(_lispHandle) {
    }

    void on_apply(::braft::Iterator& iter) {
    }
  };

  extern "C" {
    BknrStateMachine* make_bknr_state_machine(lisp_state_machine lispHandle) {
      return new BknrStateMachine(lispHandle);
    }
  }
}
