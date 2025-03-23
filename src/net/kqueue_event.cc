/*
 * Copyright (c) 2023-present, arana-db Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "kqueue_event.h"

#ifdef HAVE_KQUEUE
#  include "log.h"

namespace net {

const int BaseEvent::EVENT_READ = EVFILT_READ;
const int BaseEvent::EVENT_WRITE = EVFILT_WRITE;
const int BaseEvent::EVENT_ERROR = EV_ERROR;
const int BaseEvent::EVENT_HUB = EV_EOF;
const int BaseEvent::EVENT_NULL = EVFILT_USER;

bool KqueueEvent::Init() {
  evFd_ = kqueue();
  if (evFd_ == -1) {
    ERROR("kqueue error:{}", errno);
    return false;
  }
  if (mode_ & EVENT_MODE_READ) {
    for (auto &listenSocket : listen_sockets_) {
      AddEvent(listenSocket->Fd(), EVENT_READ);
    }
  }
  if (pipe(pipeFd_) == -1) {
    ERROR("pipe error:{}", errno);
    return false;
  }

  AddEvent(pipeFd_[0], EVENT_READ);
  return true;
}

void KqueueEvent::AddEvent(int fd, int mask) const {
  struct kevent change;
  EV_SET(&change, fd, mask, EV_ADD, 0, 0, nullptr);
  if (kevent(EvFd(), &change, 1, nullptr, 0, nullptr) == -1) {
    ERROR("KqueueEvent AddEvent EvFd:{},fd:{}, epoll add error errno:{}", EvFd(), fd, errno);
  }
}

void KqueueEvent::AddEvent(Connection *conn, int mask) {
  struct kevent change;
  EV_SET(&change, conn->fd_, mask, EV_ADD, 0, 0, conn);
  if (kevent(EvFd(), &change, 1, nullptr, 0, nullptr) == -1) {
    ERROR("KqueueEvent AddEvent id:{},EvFd:{}，fd:{}, kevent error:{}", conn->conn_id_, EvFd(), conn->fd_, errno);
  }
}

void KqueueEvent::DelEvent(int fd) {
  if (mode_ & EVENT_MODE_READ) {
    struct kevent change;
    EV_SET(&change, fd, EVENT_READ, EV_DELETE, 0, 0, nullptr);
    if (kevent(EvFd(), &change, 1, nullptr, 0, nullptr) == -1) {
      ERROR("KqueueEvent Del read Event EvFd:{}，fd:{}, kevent error:{}", EvFd(), fd, errno);
    }
  }
  if (mode_ & EVENT_MODE_WRITE) {
    struct kevent change;
    EV_SET(&change, fd, EVENT_WRITE, EV_DELETE, 0, 0, nullptr);
    if (kevent(EvFd(), &change, 1, nullptr, 0, nullptr) == -1) {
      if (errno != ENOENT) {  // If the event does not exist, it will return ENOENT
        ERROR("KqueueEvent Del write Event EvFd:{}，fd:{}, kevent error:{}", EvFd(), fd, errno);
      }
    }
  }
}

void KqueueEvent::AddWriteEvent(Connection *conn) { AddEvent(conn, EVENT_WRITE); }

void KqueueEvent::DelWriteEvent(Connection *conn) {
  struct kevent change;
  EV_SET(&change, conn->fd_, EVENT_WRITE, EV_DELETE, 0, 0, nullptr);
  if (kevent(EvFd(), &change, 1, nullptr, 0, nullptr) == -1) {
    ERROR("KqueueEvent Del write Event id:{},EvFd:{}，fd:{}, kevent error:{}", conn->conn_id_, EvFd(), conn->fd_,
          errno);
  }
}

void KqueueEvent::EventPoll() {
  if (mode_ & EVENT_MODE_READ) {
    EventRead();
  } else {
    EventWrite();
  }
}

void KqueueEvent::EventRead() {
  struct kevent events[eventsSize];
  struct timespec *pTimeout = nullptr;
  struct timespec timeout {};
  if (timer_) {
    pTimeout = &timeout;
    int waitInterval = static_cast<int>(timer_->Interval());
    timeout.tv_sec = waitInterval / 1000;
    timeout.tv_nsec = (waitInterval % 1000) * 1000000;
  }

  while (running_.load()) {
    int nev = kevent(EvFd(), nullptr, 0, events, eventsSize, pTimeout);
    for (int i = 0; i < nev; ++i) {
      if ((events[i].flags & EVENT_HUB) || (events[i].flags & EVENT_ERROR)) {
        DoError(events[i], "");
        continue;
      }
      if (events[i].ident == pipeFd_[0]) {
        continue;
      }
      Connection *conn = nullptr;
      if (events[i].filter == EVENT_READ) {
        auto listen = getListenSocket(events[i].ident);
        if (!listen) {
          conn = static_cast<Connection *>(events[i].udata);
        }
        DoRead(events[i], conn, listen);
      } else if ((mode_ & EVENT_MODE_WRITE) && events[i].filter == EVENT_WRITE) {
        conn = static_cast<Connection *>(events[i].udata);
        if (!conn) {
          DoError(events[i], "write conn is null");
          continue;
        }
        DoWrite(events[i], conn);
      }
    }
    if (timer_) {
      timer_->OnTimer();
    }
  }
}

void KqueueEvent::EventWrite() {
  struct kevent events[eventsSize];
  while (running_.load()) {
    int nev = kevent(EvFd(), nullptr, 0, events, eventsSize, nullptr);
    for (int i = 0; i < nev; ++i) {
      if ((events[i].flags & EVENT_HUB) || (events[i].flags & EVENT_ERROR)) {
        DoError(events[i], "EventWrite error");
        continue;
      }
      auto conn = static_cast<Connection *>(events[i].udata);
      if (!conn) {
        DoError(events[i], "write conn is null");
        continue;
      }
      if (events[i].filter == EVENT_WRITE) {
        DoWrite(events[i], conn);
      }
    }
  }
}

void KqueueEvent::DoRead(const struct kevent &event, Connection *conn, const std::shared_ptr<ListenSocket> &listen) {
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
    DoError(event, "DoRead error");
  }
}

void KqueueEvent::DoWrite(const struct kevent &event, Connection *conn) {
  auto ret = conn->net_event_->OnWritable(conn, this);
  if (ret == NE_ERROR) {
    DoError(event, "DoWrite error,errno: " + std::to_string(errno));
    return;
  }
}

void KqueueEvent::DoError(const struct kevent &event, std::string &&err) {
  auto conn = static_cast<Connection *>(event.udata);
  if (!conn) {
    ERROR("DoError conn is null");
    return;
  }
  onClose_(conn->conn_id_, std::move(err));
}

}  // namespace net

#endif
