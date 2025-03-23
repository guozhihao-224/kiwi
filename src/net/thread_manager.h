/*
 * Copyright (c) 2023-present, arana-db Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "callback_function.h"
#include "config.h"
#include "connection.h"
#include "io_thread.h"
#include "log.h"
#include "net_options.h"

#include "log.h"

#if defined(HAVE_EPOLL)

#  include "epoll_event.h"

#elif defined(HAVE_KQUEUE)

#  include "kqueue_event.h"

#endif

namespace net {

extern uint64_t getConnId();

template <typename T>
requires HasSetFdFunction<T>
class ThreadManager {
 public:
  explicit ThreadManager(int8_t index, NetOptions &net_options) : index_(index), net_options_(net_options) {}

  ~ThreadManager();

  // set new connect create before callback function
  void SetOnInit(const OnInit<T> &func) { on_init_ = func; }

  // set new connect create callback function
  void SetOnCreate(const OnCreate<T> &func) { on_create_ = func; }

  void SetOnConnect(const OnCreate<T> &func) { on_connect_ = func; }

  // set read message callback function
  void SetOnMessage(const OnMessage<T> &func) { on_message_ = func; }

  // set close connect callback function
  void SetOnClose(const OnClose<T> &func) { on_close_ = func; }

  // Start the thread and initialize the event
  bool Start(const std::vector<std::shared_ptr<ListenSocket>> &listen_sockets, const std::shared_ptr<Timer> &timer);

  // Stop the thread
  void Stop();

  // Create a new connection callback function
  void OnNetEventCreate(const std::shared_ptr<Connection> &conn);

  // Read message callback function
  void OnNetEventMessage(uint64_t conn_id, std::string &&read_data);

  // Close connection callback function
  void OnNetEventClose(uint64_t conn_id, std::string &&err);

  // Server actively closes the connection
  void CloseConnection(uint64_t conn_id);

  void TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> net_event);

  void TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> net_event, OnCreate<T> on_connect);

  void Wait();

  // Send message to the client
  void SendPacket(const T &conn, std::string &&msg);

 private:
  // Create read thread
  bool CreateReadThread(const std::vector<std::shared_ptr<ListenSocket>> &listen_sockets,
                        const std::shared_ptr<Timer> &timer);

  // Create write thread if rwSeparation_ is true
  bool CreateWriteThread();

  uint64_t DoTCPConnect(T &t, int fd, const std::shared_ptr<Connection> &conn);

  uint32_t get_client_count() const { return client_count_.load(); }

  void client_count_decrement() { client_count_.fetch_sub(1, std::memory_order_seq_cst); }

 private:
  const int8_t index_ = 0;            // The index of the thread
  uint32_t tcp_keep_alive_ = 300;     // The timeout of the keepalive connection in seconds
  std::atomic<bool> running_ = true;  // Whether the thread is running

  NetOptions net_options_;

  inline static std::atomic<uint32_t> client_count_{0};

  std::unique_ptr<IOThread> read_thread_;   // Read thread
  std::unique_ptr<IOThread> write_thread_;  // Write thread

  std::unordered_map<uint64_t, std::pair<T, std::shared_ptr<Connection>>>
      connections_;  // All connections for the current thread

  std::shared_mutex mutex_;

  OnInit<T> on_init_;

  OnCreate<T> on_create_;

  OnCreate<T> on_connect_;

  OnMessage<T> on_message_;

  OnClose<T> on_close_;
};

template <typename T>
requires HasSetFdFunction<T>
ThreadManager<T>::~ThreadManager() {
  Stop();
}

template <typename T>
requires HasSetFdFunction<T>
bool ThreadManager<T>::Start(const std::vector<std::shared_ptr<ListenSocket>> &listen_sockets,
                             const std::shared_ptr<Timer> &timer) {
  if (!CreateReadThread(listen_sockets, timer)) {
    return false;
  }
  if (net_options_.GetRwSeparation()) {
    return CreateWriteThread();
  }
  return true;
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::Stop() {
  bool expected = true;
  if (running_.compare_exchange_strong(expected, false)) {
    read_thread_->Stop();
    if (net_options_.GetRwSeparation()) {
      write_thread_->Stop();
    }
  }
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::OnNetEventCreate(const std::shared_ptr<Connection> &conn) {
  uint32_t expected = get_client_count();
  conn->fd_;
  if (!client_count_.compare_exchange_strong(expected, expected + 1, std::memory_order_seq_cst,
                                             std::memory_order_seq_cst) ||
      expected >= net_options_.GetMaxClients()) {
    INFO("Max client connections, refuse new connection fd:{}", conn->fd_);
    std::string response = "-ERR max clients reached\r\n";
    ssize_t sent = ::send(conn->fd_, response.c_str(), response.size(), 0);
    if (sent < 0) {
      ERROR("Failed to send error response to fd: %d, errno: %d", conn->fd_, errno);
    }
    if (::close(conn->fd_) < 0) {
      ERROR("Failed to close fd: %d, errno: %d", conn->fd_, errno);
    }
    return;
  }

  T t;
  on_init_(&t);
  auto conn_id = getConnId();
  if constexpr (IsPointer_v<T>) {
    t->SetConnId(conn_id);
    t->SetThreadIndex(index_);
  } else {
    t.SetConnId(conn_id);
    t.SetThreadIndex(index_);
  }

  {
    std::lock_guard lock(mutex_);
    connections_.emplace(conn_id, std::make_pair(t, conn));
  }
  read_thread_->AddNewEvent(conn.get(), BaseEvent::EVENT_READ);
  if (write_thread_) {
    write_thread_->AddNewEvent(conn.get(), BaseEvent::EVENT_NULL);  // add null event to write_thread epoll
  }

  conn->conn_id_ = conn_id;
  on_create_(conn_id, t, conn->addr_);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::OnNetEventMessage(uint64_t conn_id, std::string &&read_data) {
  T t;
  {
    std::shared_lock lock(mutex_);
    auto iter = connections_.find(conn_id);
    if (iter == connections_.end()) {
      ERROR("OnNetEventMessage conn_id:{} not found", conn_id);
      return;
    }
    t = iter->second.first;
  }
  on_message_(std::move(read_data), t);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::OnNetEventClose(uint64_t conn_id, std::string &&err) {
  std::lock_guard lock(mutex_);
  auto iter = connections_.find(conn_id);
  if (iter == connections_.end()) {
    ERROR("OnNetEventClose conn_id:{} not found", conn_id);
    return;
  }
  const int fd = iter->second.second->fd_;

  read_thread_->CloseConnection(fd);
  if (net_options_.GetRwSeparation()) {
    write_thread_->CloseConnection(fd);
  }

  iter->second.second->net_event_->Close();  // close socket
  on_close_(iter->second.first, std::move(err));
  connections_.erase(iter);
  client_count_decrement();
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::CloseConnection(uint64_t conn_id) {
  OnNetEventClose(conn_id, "");
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> net_event) {
  auto newConn = std::make_shared<Connection>(std::move(net_event));
  newConn->addr_ = addr;
  T t;
  on_init_(&t);
  auto connId = DoTCPConnect(t, newConn->net_event_->Fd(), newConn);
  on_connect_(connId, t, addr);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> net_event, OnCreate<T> on_connect) {
  auto new_conn = std::make_shared<Connection>(std::move(net_event));
  new_conn->addr_ = addr;
  T t;
  on_init_(&t);
  auto conn_id = DoTCPConnect(t, new_conn->net_event_->Fd(), new_conn);
  on_connect(conn_id, t, addr);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::Wait() {
  read_thread_->Wait();
  if (net_options_.GetRwSeparation()) {
    write_thread_->Wait();
  }
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::SendPacket(const T &conn, std::string &&msg) {
  std::shared_lock lock(mutex_);
  uint64_t conn_id = 0;
  if constexpr (IsPointer_v<T>) {
    conn_id = conn->GetConnId();
  } else {
    conn_id = conn.GetConnId();
  }
  std::shared_ptr<Connection> conn_ptr;
  {
    auto iter = connections_.find(conn_id);
    if (iter == connections_.end()) {
      return;
    }
    conn_ptr = iter->second.second;
  }

  conn_ptr->net_event_->SendPacket(std::move(msg), [&]() {
    if (net_options_.GetRwSeparation()) {
      write_thread_->SetWriteEvent(conn_ptr.get());
    } else {
      read_thread_->SetWriteEvent(conn_ptr.get());
    }
  });
}

template <typename T>
requires HasSetFdFunction<T>
bool ThreadManager<T>::CreateReadThread(const std::vector<std::shared_ptr<ListenSocket>> &listen_sockets,
                                        const std::shared_ptr<Timer> &timer) {
  std::shared_ptr<BaseEvent> event;
  int8_t event_mode = BaseEvent::EVENT_MODE_READ;
  if (!net_options_.GetRwSeparation()) {
    event_mode |= BaseEvent::EVENT_MODE_WRITE;
  }

#if defined(HAVE_EPOLL)
  event = std::make_shared<EpollEvent>(listen_sockets, event_mode);
#elif defined(HAVE_KQUEUE)
  event = std::make_shared<KqueueEvent>(listen_sockets, event_mode);
#endif

  event->AddTimer(timer);

  event->SetOnCreate([this](const std::shared_ptr<Connection> &conn) { OnNetEventCreate(conn); });

  event->SetOnMessage(
      [this](uint64_t conn_id, std::string &&read_data) { OnNetEventMessage(conn_id, std::move(read_data)); });

  event->SetOnClose([this](uint64_t conn_id, std::string &&err) { OnNetEventClose(conn_id, std::move(err)); });

  read_thread_ = std::make_unique<IOThread>(event);
  return read_thread_->Run();
}

template <typename T>
requires HasSetFdFunction<T>
bool ThreadManager<T>::CreateWriteThread() {
  std::shared_ptr<BaseEvent> event;

#if defined(HAVE_EPOLL)
  event = std::make_shared<EpollEvent>(std::vector<std::shared_ptr<ListenSocket>>(), BaseEvent::EVENT_MODE_WRITE);
#elif defined(HAVE_KQUEUE)
  event = std::make_shared<KqueueEvent>(std::vector<std::shared_ptr<ListenSocket>>(), BaseEvent::EVENT_MODE_WRITE);
#endif

  event->SetOnClose([this](uint64_t conn_id, std::string &&msg) { OnNetEventClose(conn_id, std::move(msg)); });

  write_thread_ = std::make_unique<IOThread>(event);
  return write_thread_->Run();
}

template <typename T>
requires HasSetFdFunction<T>
uint64_t ThreadManager<T>::DoTCPConnect(T &t, int fd, const std::shared_ptr<Connection> &conn) {
  auto conn_id = getConnId();
  if constexpr (IsPointer_v<T>) {
    t->SetConnId(conn_id);
    t->SetThreadIndex(index_);
  } else {
    t.SetConnId(conn_id);
    t.SetThreadIndex(index_);
  }

  conn->fd_ = fd;
  conn->conn_id_ = conn_id;

  {
    std::lock_guard lock(mutex_);
    connections_.emplace(conn_id, std::make_pair(t, conn));
  }

  read_thread_->AddNewEvent(conn.get(), BaseEvent::EVENT_READ);
  if (write_thread_) {
    write_thread_->AddNewEvent(conn.get(), BaseEvent::EVENT_NULL);  // add null event to write_thread epoll
  }
  return conn_id;
}

}  // namespace net
