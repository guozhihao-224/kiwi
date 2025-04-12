// Copyright (c) 2023-present, arana-db Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#pragma once

#include <atomic>

#include "common.h"
#include "net/net_options.h"

namespace kiwi {

class Options : public net::NetOptions {
 public:
  Options() = default;
  ~Options() = default;

  void SetConfigName(const PString& cfg_file) { cfg_file_ = cfg_file; }

  const PString& GetConfigName() const { return cfg_file_; }

  void SetLogLevel(const PString& log_level) { log_level_ = log_level; }

  const PString& GetLogLevel() const { return log_level_; }

  void SetRedisCompatibleMode(bool mode) { redis_compatible_mode = mode; }

  bool GetRedisCompatibleMode() const { return redis_compatible_mode; }

  void SetIps(const std::vector<PString>& ips) { ips_ = ips; }

  const std::vector<PString>& GetIps() const { return ips_; }

  void SetUseRaft(const PString& use) { use_raft = use; }

  const PString& GetUseRaft() const { return use_raft; }

  void SetRaftIp(const PString& ip) { raft_ip = ip; }

  const PString& GetRaftIp() const { return raft_ip; }

 private:
  PString cfg_file_;
  PString log_level_;

  std::atomic<bool> redis_compatible_mode = false;
  std::vector<PString> ips_;
  PString use_raft;
  PString raft_ip;
};

}  // namespace kiwi
