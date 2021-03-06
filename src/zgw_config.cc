#include "src/zgw_config.h"

#include <iostream>

#include "slash/include/slash_string.h"
#include "src/zgw_const.h"

ZgwConfig::ZgwConfig(std::string path)
      : server_ip("0.0.0.0"),
        server_port(8099),
        admin_port(8199),
        daemonize(false),
        minloglevel(0),
        worker_num(2),
        log_path("./log"),
        pid_file(kZgwPidFile) {
  b_conf = new slash::BaseConf(path);
}

ZgwConfig::~ZgwConfig() {
  delete b_conf;
}

int ZgwConfig::LoadConf() {
  if (b_conf->LoadConf() != 0)
    return -1;

  std::string zp_meta_addr;
  b_conf->GetConfStr("zp_meta_addr", &zp_meta_addr);
  slash::StringSplit(zp_meta_addr, '/', zp_meta_ip_ports);
  b_conf->GetConfStr("server_ip", &server_ip);
  b_conf->GetConfInt("server_port", &server_port);
  b_conf->GetConfInt("admin_port", &admin_port);
  b_conf->GetConfBool("daemonize", &daemonize);
  b_conf->GetConfInt("minloglevel", &minloglevel);
  b_conf->GetConfInt("worker_num", &worker_num);
  b_conf->GetConfStr("log_path", &log_path);
  b_conf->GetConfStr("pid_file", &pid_file);

  return 0;
}

void ZgwConfig::Dump() {
  b_conf->DumpConf();
}
