// Copyright (c) 2023-present, arana-db Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

/*
  Stub main() routine for the kiwi executable.

  This does some essential startup tasks for kiwi, and then dispatches to the proper FooMain() routine for the
  incarnation.
 */

#include <getopt.h>
#include <sys/fcntl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iostream>

#include "client.h"
#include "client_map.h"
#include "config.h"
#include "gflags/gflags.h"
#include "helper.h"
#include "kiwi.h"
#include "kiwi_logo.h"
#include "options.h"
#include "raft/raft.h"
#include "slow_log.h"
#include "std/log.h"
#include "std/std_util.h"
#include "store.h"

// g_kiwi is a global abstraction of the server-side process
std::unique_ptr<KiwiDB> g_kiwi;

using namespace kiwi;

/*
 * set up a handler to be called if the kiwi crashes
 * with a fatal signal or exception.
 */
static void IntSigHandle(const int sig) {
  INFO("Catch Signal {}, cleanup...", sig);
  g_kiwi->Stop();
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

const uint32_t KiwiDB::kRunidSize = 40;

static void WarnDefaultConfig() {
  std::cerr << "*********************************************************\n";
  std::cerr << "* Warning: Use the default configuration to start Kiwi. *\n";
  std::cerr << "*********************************************************\n";
}

static void Usage() {
  std::cerr << "kiwi is the kiwi server.\n";
  std::cerr << "\n";
  std::cerr << "Usage:\n";
  std::cerr << "  kiwi [--config] [/path/to/kiwi.conf] [options]\n";
  std::cerr << "\n";
  std::cerr << "Options:\n";
  std::cerr << "  -v, --Version                   output version information, then exit\n";
  std::cerr << "  -h, --usage                     output help message\n";
  std::cerr << "  -p PORT, --port PORT            Set the port to listen on\n";
  std::cerr << "  -l LEVEL, --loglevel LEVEL      Set the log level (e.g., debug, verbose, notice, warning)\n";
  std::cerr << "  -s ADDRESS, --slaveof ADDRESS   Set the slave address (e.g., 127.0.0.1:6380)\n";
  std::cerr << "  -c, --redis-compatible-mode     Enable Redis compatibility mode\n";
  std::cerr << "  --use-raft                      Whether to use Raft [yes or no]\n";
  std::cerr << "  --raft-ip                       Raft IP address\n";
  std::cerr << "  --ips                           List of IP addresses [x.x.x.x ::x::x::x::x ...]\n";
  std::cerr << "  --config                        Path to the configuration file\n";
  std::cerr << "Examples:\n";
  std::cerr << "  kiwi --usage\n";
  std::cerr << "  kiwi --Version\n";
  std::cerr << "  kiwi [--config] /path/kiwi.conf\n";
  std::cerr << "  kiwi [--config] /path/kiwi.conf\n";
  std::cerr << "  kiwi [--config] /path/kiwi.conf --loglevel verbose\n";
  std::cerr << "  kiwi --port 7777\n";
  std::cerr << "  kiwi --port 7777 --slaveof 127.0.0.1:8888\n";
  std::cerr << "  kiwi [--config] /path/kiwi.conf --use_raft [yes or no]\n";
  std::cerr << "  kiwi [--config] /path/kiwi.conf --ips [x.x.x.x ::x::x::x::x ...]\n";
  std::cerr << "  kiwi [--config] /path/kiwi.conf --raft_ip x.x.x.x\n";
}

static void version() {
  std::cerr << "kiwi Server version: " << KIWI_VERSION << " bits=" << (sizeof(void*) == 8 ? 64 : 32) << '\n';
  std::cerr << "kiwi Server Build Type: " << KIWI_BUILD_TYPE << '\n';
  std::cerr << "kiwi Server Build Date: " << KIWI_BUILD_DATE << '\n';
  std::cerr << "kiwi Server Build GIT SHA: " << KIWI_GIT_COMMIT_ID << '\n';
}

DEFINE_string(config, "", "Path to the configuration file");
DEFINE_string(use_raft, "", "Whether to use Raft [yes or no]");
DEFINE_string(ips, "", "List of IP addresses [x.x.x.x ::x::x::x::x ...]");
DEFINE_string(raft_ip, "", "Raft IP address");
DEFINE_uint32(port, 0, "Port number");
DEFINE_string(loglevel, "", "Log level");
DEFINE_bool(redis_compatible_mode, false, "Enable Redis compatible mode");
DEFINE_string(slaveof, "", "Set as a slave of another instance");
DEFINE_bool(usage, false, "Show usage information");
DEFINE_bool(Version, false, "Show version information");

static inline std::vector<std::string> SplitIPs(const std::string& ips, const std::string& sep) {
  std::vector<std::string> ipList;
  size_t start = 0;
  size_t end = ips.find(sep);
  while (end != std::string::npos) {
    ipList.push_back(ips.substr(start, end - start));
    start = end + sep.size();
    end = ips.find(sep, start);
  }
  ipList.emplace_back(ips.substr(start, end));
  return ipList;
}

static inline void PrintParsedFlags() {
  std::cout << "Parsed command-line flags:\n";
  std::cout << "  --config: " << FLAGS_config << "\n";
  std::cout << "  --use-raft: " << FLAGS_use_raft << "\n";
  std::cout << "  --ips: " << FLAGS_ips << "\n";
  std::cout << "  --raft-ip: " << FLAGS_raft_ip << "\n";
  std::cout << "  --port: " << FLAGS_port << "\n";
  std::cout << "  --loglevel: " << FLAGS_loglevel << "\n";
  std::cout << "  --redis-compatible-mode: " << (FLAGS_redis_compatible_mode ? "true" : "false") << "\n";
  std::cout << "  --slaveof: " << FLAGS_slaveof << "\n";
  std::cout << "  --usage: " << (FLAGS_usage ? "true" : "false") << "\n";
  std::cout << "  --Version: " << (FLAGS_Version ? "true" : "false") << "\n";
}

bool KiwiDB::ParseArgs(int argc, char* argv[]) {
  PString conf_file;
  if (argc > 1 && !std::string(argv[1]).starts_with('-')) {
    conf_file = argv[1];

    for (int i = 1; i < argc; ++i) {
      argv[i] = argv[i + 1];
    }
    argc--;
  }

  gflags::ParseCommandLineNonHelpFlags(&argc, &argv, true);

  if (FLAGS_usage) {
    return false;
  }

  if (FLAGS_Version) {
    version();
    exit(EXIT_SUCCESS);
  }

  // Overwrite the config
  if (!FLAGS_config.empty()) {
    conf_file = FLAGS_config;
  }

  if (!conf_file.empty()) {
    namespace fs = std::filesystem;
    std::filesystem::path config_path(conf_file);
    if (fs::is_regular_file(config_path) &&
        (fs::status(config_path).permissions() & fs::perms::owner_read) != fs::perms::none) {
      options_.SetConfigName(conf_file);
      std::cerr << "Configuration file path: [" << conf_file << "]\n";
    } else {
      std::cerr << "Configuration file [" << conf_file << "]: " << strerror(errno) << "\n";
      return false;
    }
  } else {
    WarnDefaultConfig();
  }

  if (FLAGS_port > 0) {
    if (FLAGS_port <= 1234) {
      std::cerr << "You should have root privileges but now NOT support."
                << "\n";
      return false;
    }
    port_ = static_cast<uint16_t>(FLAGS_port);
  }

  if (!FLAGS_loglevel.empty()) {
    if (FLAGS_loglevel != "debug" && FLAGS_loglevel != "verbose" && FLAGS_loglevel != "notice" &&
        FLAGS_loglevel != "warning") {
      std::cerr << "You must be choose one of [debug, verbose, notice, warning]."
                << "\n";
      return false;
    }
    options_.SetLogLevel(FLAGS_loglevel);
  }

  if (!FLAGS_slaveof.empty()) {
    char str[256];
    if (sscanf(FLAGS_slaveof.c_str(), "%[^:]:%hu", str, &master_port_) != 2) {
      std::cerr << "Invalid slaveof format.\n";
      return false;
    }
    master_ = str;
  }

  if (FLAGS_redis_compatible_mode) {
    options_.SetRedisCompatibleMode(FLAGS_redis_compatible_mode);
  }

  if (!FLAGS_use_raft.empty()) {
    std::transform(FLAGS_use_raft.cbegin(), FLAGS_use_raft.cend(), FLAGS_use_raft.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (FLAGS_use_raft != "yes" && FLAGS_use_raft != "no") {
      std::cerr << "You should decide use-raft = yes or no"
                << "\n";
      return false;
    }
    options_.SetUseRaft(FLAGS_use_raft);
  }

  if (!FLAGS_raft_ip.empty()) {
    options_.SetRaftIp(FLAGS_raft_ip);
  }

  if (!FLAGS_ips.empty()) {
    auto ips = SplitIPs(FLAGS_ips, " ");
    options_.SetIps(ips);
  }

  return true;
}

void KiwiDB::OnNewConnection(uint64_t connId, std::shared_ptr<kiwi::PClient>& client, const net::SocketAddr& addr) {
  INFO("New connection from {}:{}", addr.GetIP(), addr.GetPort());
  client->SetSocketAddr(addr);
  client->OnConnect();
  // add new PClient to clients
  ClientMap::getInstance().AddClient(client->GetUniqueID(), client);
}

void KiwiDB::ScanEvictedBlockedConnsOfBlrpop() {
  std::vector<kiwi::BlockKey> keys_need_remove;

  std::lock_guard<std::shared_mutex> map_lock(block_mtx_);
  auto& key_to_blocked_conns = g_kiwi->GetMapFromKeyToConns();
  for (auto& it : key_to_blocked_conns) {
    auto& conns_list = it.second;
    for (auto conn_node = conns_list->begin(); conn_node != conns_list->end();) {
      auto conn_ptr = conn_node->GetBlockedClient();
      if (conn_node->GetBlockedClient()->State() == ClientState::kClosed) {
        conn_node = conns_list->erase(conn_node);
        CleanBlockedNodes(conn_ptr);
      } else if (conn_node->IsExpired()) {
        conn_ptr->ReplyNull();
        conn_ptr->SendPacket();
        conn_node = conns_list->erase(conn_node);
        CleanBlockedNodes(conn_ptr);
      } else {
        ++conn_node;
      }
    }
    if (conns_list->empty()) {
      keys_need_remove.push_back(it.first);
    }
  }

  for (auto& remove_key : keys_need_remove) {
    key_to_blocked_conns.erase(remove_key);
  }
}

void KiwiDB::CleanBlockedNodes(const std::shared_ptr<kiwi::PClient>& client) {
  std::vector<kiwi::BlockKey> blocked_keys;
  for (const auto& key : client->Keys()) {
    blocked_keys.emplace_back(client->GetCurrentDB(), key);
  }
  auto& key_to_blocked_conns = g_kiwi->GetMapFromKeyToConns();
  for (auto& blocked_key : blocked_keys) {
    const auto& it = key_to_blocked_conns.find(blocked_key);
    if (it != key_to_blocked_conns.end()) {
      auto& conns_list = it->second;
      for (auto conn_node = conns_list->begin(); conn_node != conns_list->end(); ++conn_node) {
        if (conn_node->GetBlockedClient()->GetConnId() == client->GetConnId()) {
          conns_list->erase(conn_node);
          break;
        }
      }
    }
  }
}

bool KiwiDB::Init() {
  char runid[kRunidSize + 1] = "";
  getRandomHexChars(runid, kRunidSize);
  g_config.Set("runid", {runid, kRunidSize}, true);

  if (port_ != 0) {
    g_config.Set("port", std::to_string(port_), true);
  }

  if (!options_.GetLogLevel().empty()) {
    g_config.Set("log-level", options_.GetLogLevel(), true);
  }

  if (options_.GetRedisCompatibleMode()) {
    g_config.Set("redis_compatible_mode", std::to_string(options_.GetRedisCompatibleMode()), true);
  }

  if (!options_.GetUseRaft().empty()) {
    g_config.Set("use-raft", options_.GetUseRaft(), true);
  }

  if (!options_.GetRaftIp().empty()) {
    g_config.Set("raft-ip", options_.GetRaftIp(), true);
  }

  auto num = g_config.worker_threads_num + g_config.slave_threads_num;
  options_.SetThreadNum(num);

  options_.SetMaxClients(g_config.max_clients);

  // now we only use fast cmd thread pool
  auto status = cmd_threads_.Init(g_config.fast_cmd_threads_num, 1, "kiwi-cmd");
  if (!status.ok()) {
    ERROR("init cmd thread pool failed: {}", status.ToString());
    return false;
  }

  STORE_INST.Init(g_config.databases);

  PSlowLog::Instance().SetThreshold(g_config.slow_log_time);
  PSlowLog::Instance().SetLogLimit(static_cast<std::size_t>(g_config.slow_log_max_len));

  // master ip
  if (!g_config.master_ip.empty()) {
    PREPL.SetMasterAddr(g_config.master_ip.c_str(), g_config.master_port);
  }

  auto tcpKeepAlive = g_config.tcp_keepalive;
  options_.SetOpTcpKeepAlive(tcpKeepAlive);

  options_.SetRwSeparation(true);

  event_server_ = std::make_unique<net::EventServer<std::shared_ptr<PClient>>>(options_);

  if (!options_.GetIps().empty()) {
    g_config.ips = options_.GetIps();
  }

  for (const auto& ip : g_config.ips) {
    net::SocketAddr addr(ip, g_config.port);
    INFO("Add listen addr: {}, port: {}", ip, g_config.port);
    event_server_->AddListenAddr(addr);
  }

  event_server_->SetOnInit([](std::shared_ptr<PClient>* client) { *client = std::make_shared<PClient>(); });

  event_server_->SetOnCreate([](uint64_t connID, std::shared_ptr<PClient>& client, const net::SocketAddr& addr) {
    client->SetSocketAddr(addr);
    client->OnConnect();
    ClientMap::getInstance().AddClient(client->GetUniqueID(), client);
    INFO("New connection connID fd:{} IP:{} port:{}", connID, addr.GetIP(), addr.GetPort());
  });

  event_server_->SetOnMessage([](std::string&& msg, std::shared_ptr<PClient>& t) { t->HandlePacket(std::move(msg)); });

  event_server_->SetOnClose([](std::shared_ptr<PClient>& client, std::string&& msg) {
    INFO("Close connection id:{} msg:{}", client->GetConnId(), msg);
    client->OnClose();
    ClientMap::getInstance().RemoveClientById(client->GetUniqueID());
  });

  event_server_->InitTimer(10);

  auto timerTask = std::make_shared<net::CommonTimerTask>(1000);
  timerTask->SetCallback([]() { PREPL.Cron(); });
  event_server_->AddTimerTask(timerTask);

  auto BLRPopTimerTask = std::make_shared<net::CommonTimerTask>(250);
  BLRPopTimerTask->SetCallback(std::bind(&KiwiDB::ScanEvictedBlockedConnsOfBlrpop, this));
  event_server_->AddTimerTask(BLRPopTimerTask);

  time(&start_time_s_);

  return true;
}

void KiwiDB::Run() {
  auto [ret, err] = event_server_->StartServer();
  if (!ret) {
    ERROR("start server failed: {}", err);
    return;
  }

  cmd_threads_.Start();
  event_server_->Wait();
  INFO("server exit running");
}

void KiwiDB::Stop() {
  kiwi::RAFT_INST.ShutDown();
  kiwi::RAFT_INST.Join();
  kiwi::RAFT_INST.Clear();
  cmd_threads_.Stop();
  event_server_->StopServer();
}

void KiwiDB::TCPConnect(
    const net::SocketAddr& addr,
    const std::function<void(uint64_t, std::shared_ptr<kiwi::PClient>&, const net::SocketAddr&)>& onConnect,
    const std::function<void(std::string)>& cb) {
  INFO("Connect to {}:{}", addr.GetIP(), addr.GetPort());
  event_server_->TCPConnect(addr, onConnect, cb);
}

static void InitLogs() {
  logger::Init("logs/kiwi_server.log");

#if BUILD_DEBUG
  spdlog::set_level(spdlog::level::debug);
#else
  spdlog::set_level(spdlog::level::info);
#endif
}

static int InitLimit() {
  rlimit limit;
  rlim_t maxfiles = g_config.max_clients;
  if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
    WARN("getrlimit error: {}", strerror(errno));
  } else if (limit.rlim_cur < maxfiles) {
    rlim_t old_limit = limit.rlim_cur;
    limit.rlim_cur = maxfiles;
    limit.rlim_max = maxfiles;
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) {
      WARN("your 'limit -n' of {} is not enough for kiwi to start. kiwi has successfully reconfig it to {}", old_limit,
           limit.rlim_cur);
    } else {
      ERROR(
          "your 'limit -n ' of {} is not enough for kiwi to start."
          " kiwi can not reconfig it({}), do it by yourself",
          old_limit, strerror(errno));
      return -1;
    }
  }

  return 0;
}

static void daemonize() {
  if (fork()) {
    // parent exits
    exit(0);
  }
  // create a new session
  setsid();
}

static void closeStd() {
  int fd;
  fd = open("/dev/null", O_RDWR, 0);
  if (fd != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    close(fd);
  }
}

// Any kiwi server process begins execution here.
int main(int argc, char* argv[]) {
  g_kiwi = std::make_unique<KiwiDB>();
  if (!g_kiwi->ParseArgs(argc, argv)) {
    Usage();
    return -1;
  }
#if BUILD_DEBUG
  PrintParsedFlags();
#endif  //! BUILD_DEBUG

  if (!g_kiwi->GetConfigName().empty()) {
    if (!g_config.LoadFromFile(g_kiwi->GetConfigName())) {
      std::cerr << "Load config file [" << g_kiwi->GetConfigName() << "] failed!\n";
      return -1;
    }
  }

  if (g_config.daemonize) {
    daemonize();
  }

  kstd::InitRandom();
  SignalSetup();
  InitLogs();
  InitLimit();

  if (g_config.daemonize) {
    closeStd();
  }

  if (g_kiwi->Init()) {
    // output logo to console
    char logo[1024] = "";
    snprintf(logo, sizeof logo - 1, kiwiLogo, KIWI_VERSION, static_cast<int>(sizeof(void*)) * 8,
             static_cast<int>(g_config.port));
    std::cout << logo;
    g_kiwi->Run();
  }

  // When kiwi exit, flush log
  spdlog::get(logger::Logger::Instance().Name())->flush();
  return 0;
}
