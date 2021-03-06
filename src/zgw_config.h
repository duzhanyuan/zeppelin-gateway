#ifndef ZGW_CONFIG_H
#define ZGW_CONFIG_H

#include <string>

#include "slash/include/base_conf.h"

struct ZgwConfig {
  explicit ZgwConfig(std::string path);
  ~ZgwConfig();

  int LoadConf();
  void Dump();

  slash::BaseConf *b_conf;

  std::vector<std::string> zp_meta_ip_ports;
  std::string server_ip;
  int server_port;
  int admin_port;
  bool daemonize;
  int minloglevel;
  int cron_interval;
  int worker_num;

  std::string log_path;
  std::string pid_file;
};

#endif
