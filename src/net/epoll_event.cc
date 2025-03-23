/*
 * Copyright (c) 2023-present, arana-db Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "epoll_event.h"

#ifdef HAVE_EPOLL

#  include "log.h"

namespace net {

const int BaseEvent::EVENT_READ = EPOLLIN;
const int BaseEvent::EVENT_WRITE = EPOLLOUT;
const int BaseEvent::EVENT_ERROR = EPOLLERR;
const int BaseEvent::EVENT_HUB = EPOLLHUP;
const int BaseEvent::EVENT_NULL = 0;

bool EpollEvent::Init() {
  evFd_ = epoll_create1(0);
  if (evFd_ == -1) {  // If the epoll creation fails, return false
    ERROR("epoll_create1 error errno:{}", errno);
    return false;
  }
  if (mode_ & EVENT_MODE_READ) {  // Add the listen socket to epoll for read
    for (auto &s : listen_sockets_) {
      AddEvent(s->Fd(), EVENT_READ);
    }
  }
  if (pipe(pipeFd_) == -1) {
    ERROR("pipe error errno:{}", errno);
    return false;
  }

  AddEvent(pipeFd_[0], EVENT_READ);

  return true;
}

void EpollEvent::AddEvent(int fd, int mask) const {
  epoll_event ev{};
  ev.events = mask;
  ev.data.fd = fd;
  if (epoll_ctl(EvFd(), EPOLL_CTL_ADD, fd, &ev) == -1) {
    ERROR("AddEvent EvFd:{},fd:{}, epoll add error errno:{}", EvFd(), fd, errno);
  }
}

void EpollEvent::AddEvent(Connection *conn, int mask) {
  epoll_event ev{};
  ev.events = mask;
  ev.data.ptr = conn;
  if (epoll_ctl(EvFd(), EPOLL_CTL_ADD, conn->fd_, &ev) == -1) {
    ERROR("AddEvent id:{},EvFd:{},fd:{}, epoll AddEvent error errno:{}", conn->conn_id_, EvFd(), conn->fd_, errno);
  }
}

void EpollEvent::DelEvent(int fd) { epoll_ctl(EvFd(), EPOLL_CTL_DEL, fd, nullptr); }

void EpollEvent::EventPoll() {
  if (mode_ & EVENT_MODE_READ) {  // If it is a read multiplex, call EventRead
    EventRead();
  } else {  // If it is a write multiplex, call EventWrite
    EventWrite();
  }
}

void EpollEvent::AddWriteEvent(Connection *conn) {
  epoll_event ev{};
  ev.events = EVENT_WRITE;
  ev.data.ptr = conn;
  if (mode_ & EVENT_MODE_READ) {  // If it is a read multiplex, modify the event
    ev.events |= EVENT_READ;
    if (epoll_ctl(EvFd(), EPOLL_CTL_MOD, conn->fd_, &ev) == -1) {
      ERROR("AddWriteEvent id:{},EvFd:{},fd:{}, epoll add RW error errno:{}", conn->conn_id_, EvFd(), conn->fd_, errno);
    }
  } else {  // If it is a write multiplex, add the event
    if (epoll_ctl(EvFd(), EPOLL_CTL_MOD, conn->fd_, &ev) == -1) {
      ERROR("AddWriteEvent id:{},EvFd:{},fd:{}, epoll add W error errno:{}", conn->conn_id_, EvFd(), conn->fd_, errno);
    }
  }
}

void EpollEvent::DelWriteEvent(Connection *conn) {
  epoll_event ev{};
  ev.data.ptr = conn;
  if (mode_ & EVENT_MODE_READ) {  // If it is a read multiplex, modify the event to rea
    ev.events = EVENT_READ;
  } else {
    ev.events = EVENT_NULL;  // If it is a only write fd,set null event
  }
  if (epoll_ctl(EvFd(), EPOLL_CTL_MOD, conn->fd_, &ev) == -1) {
    ERROR("DelWriteEvent id:{},EvFd:{},fd:{}, EPOLL_CTL_MOD error errno:{}", conn->conn_id_, EvFd(), conn->fd_, errno);
  }
}

void EpollEvent::EventRead() {
  epoll_event events[eventsSize];
  int waitInterval = -1;
  if (timer_) {
    waitInterval = static_cast<int>(timer_->Interval());
  }
  while (running_.load()) {
    int nfds = epoll_wait(EvFd(), events, eventsSize, waitInterval);
    for (int i = 0; i < nfds; ++i) {
      if ((events[i].events & EVENT_HUB) || (events[i].events & EVENT_ERROR)) {
        // If the event is an error event, call DoError
        DoError(events[i], "");
        continue;
      }
      if (events[i].data.fd == pipeFd_[0]) {
        continue;
      }
      Connection *conn = nullptr;
      if (events[i].events & EVENT_READ) {
        // If the event is less than the listen socket, it is a new connection
        // If getListenSocket is nullptr, it means the event is not a listen socket
        auto listen = getListenSocket(events[i].data.fd);
        if (!listen) {
          conn = static_cast<Connection *>(events[i].data.ptr);
        }
        DoRead(events[i], conn, listen);
      }

      if ((mode_ & EVENT_MODE_WRITE) && events[i].events & EVENT_WRITE) {
        conn = static_cast<Connection *>(events[i].data.ptr);
        if (!conn) {  // If the connection is empty, call DoError
          DoError(events[i], "connection is null");
          continue;
        }
        // If the event is a write event, call DoWrite
        DoWrite(events[i], conn);
      }
    }
    if (timer_) {
      timer_->OnTimer();
    }
  }
}

void EpollEvent::EventWrite() {
  epoll_event events[eventsSize];
  while (running_.load()) {
    int nfds = epoll_wait(EvFd(), events, eventsSize, -1);
    for (int i = 0; i < nfds; ++i) {
      if ((events[i].events & EVENT_HUB) || (events[i].events & EVENT_ERROR)) {
        DoError(events[i], "");
      }
      auto conn = static_cast<Connection *>(events[i].data.ptr);
      if (!conn) {
        DoError(events[i], "connection is null");
        continue;
      }
      if (events[i].events & EVENT_WRITE) {
        DoWrite(events[i], conn);
      }
    }
  }
}

void EpollEvent::DoRead(const epoll_event &event, Connection *conn, const std::shared_ptr<ListenSocket> &listen) {
  if (listen) {
    auto newConn = std::make_shared<Connection>(nullptr);
    auto connFd = listen->OnReadable(newConn.get(), nullptr);
    if (connFd < 0) {
      DoError(event, "accept error");
      return;
    }
    onCreate_(newConn);
    return;
  }
  if (conn) {
    std::string readBuff;
    int ret = conn->net_event_->OnReadable(conn, &readBuff);
    if (ret == NE_ERROR) {
      DoError(event, "read error,errno: " + std::to_string(errno));
      return;
    }
    if (ret == NE_CLOSE) {
      DoError(event, "");
      return;
    }
    onMessage_(conn->conn_id_, std::move(readBuff));
  } else {
    DoError(event, "connection is null");
  }
}

void EpollEvent::DoWrite(const epoll_event &event, Connection *conn) {
  auto ret = conn->net_event_->OnWritable(conn, this);
  if (ret == NE_ERROR) {
    DoError(event, "write error,errno: " + std::to_string(errno));
  }
}

void EpollEvent::DoError(const epoll_event &event, std::string &&err) {
  auto conn = static_cast<Connection *>(event.data.ptr);
  if (!conn) {
    ERROR("DoError conn is null");
    return;
  }
  onClose_(conn->conn_id_, std::move(err));
}

}  // namespace net
#endif
