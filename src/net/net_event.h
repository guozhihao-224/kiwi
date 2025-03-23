/*
 * Copyright (c) 2023-present, arana-db Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <atomic>

#include "callback_function.h"
#include "connection.h"

namespace net {

// For human readability
enum : std::int8_t {
  NE_ERROR = -1,
  NE_CLOSE = -2,
  NE_WAIT = -3,
  NE_OK = 0,
};

enum class NetListen : std::uint8_t {
  OK = 0,
  OPEN_ERROR,
  BIND_ERROR,
  LISTEN_ERROR,
};

class BaseEvent;

// abstraction of all networks
class NetEvent {
 public:
  explicit NetEvent(int fd) : fd_(fd) {}

  virtual ~NetEvent() = default;

  // Initialize the event
  virtual int Init() = 0;

  // Handle read event when the connection is readable and the data can be read
  virtual int OnReadable(Connection *conn, std::string *readBuff) = 0;

  // Handle write event when the connection is writable and the data can be sent
  virtual int OnWritable(Connection *conn, BaseEvent *event) = 0;

  virtual void OnError() = 0;

  // Send data
  virtual void SendPacket(std::string &&msg, std::function<void()> addWriteFlag) = 0;

  virtual void Close() = 0;

  int Fd() const { return fd_.load(); }

 protected:
  std::atomic<int> fd_ = 0;
};

}  // namespace net
