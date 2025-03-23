// Copyright (c) 2023-present, arana-db Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#pragma once

#include <cstdint>

namespace net {

class NetOptions {
 public:
  NetOptions() = default;
  ~NetOptions() = default;

  void SetThreadNum(int8_t number) { thread_num_ = number; }

  int8_t GetThreadNum() const { return thread_num_; }

  void SetRwSeparation(bool spearation = true) { rw_separation_ = spearation; }

  bool GetRwSeparation() const { return rw_separation_; }

  void SetMaxClients(uint32_t maxClients) { max_clients_ = maxClients; }

  uint32_t GetMaxClients() const { return max_clients_; }
  void SetOpTcpKeepAlive(uint32_t tcpKeepAlive) { tcp_keepalive_timeout_ = tcpKeepAlive; }

  uint32_t GetOpTcpKeepAlive() const { return tcp_keepalive_timeout_; }

 private:
  bool rw_separation_ = true;  // Whether to separate read and write

  int8_t thread_num_ = 1;                 // The number of threads
  uint32_t max_clients_ = 1;              // The maximum number of connections(default 40000)
  uint32_t tcp_keepalive_timeout_ = 300;  // The timeout of the keepalive connection in seconds
};

}  // namespace net
