/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <memory>

#include "socket_addr.h"

namespace net {

class NetEvent;

// Auxiliary structure
struct Connection {
  explicit Connection(std::unique_ptr<NetEvent> net_event) : net_event_(std::move(net_event)) {}
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;
  ~Connection() = default;

  std::unique_ptr<NetEvent> net_event_;

  SocketAddr addr_;

  uint64_t conn_id_ = 0;

  int fd_ = 0;
};

}  // namespace net