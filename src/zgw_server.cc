#include "src/zgw_server.h"

#include <glog/logging.h>
#include "slash/include/slash_mutex.h"
#include "slash/include/env.h"
#include "src/zgw_monitor.h"

extern ZgwConfig* g_zgw_conf;

int ZgwThreadEnvHandle::SetEnv(void** env) const {
  libzgw::ZgwStore* store;
  Status s = libzgw::ZgwStore::Open(g_zgw_conf->zp_meta_ip_ports, &store);
  if (!s.ok()) {
    LOG(FATAL) << "Can not open ZgwStore: " << s.ToString();
    return -1;
  }
  *env = static_cast<void*>(store);
  return 0;
}

ZgwServer::ZgwServer()
    : ip_(g_zgw_conf->server_ip),
      should_exit_(false),
      worker_num_(g_zgw_conf->worker_num),
      port_(g_zgw_conf->server_port),
      admin_port_(g_zgw_conf->admin_port) {
  if (worker_num_ > kMaxWorkerThread) {
    LOG(WARNING) << "Exceed max worker thread num: " << kMaxWorkerThread;
    worker_num_ = kMaxWorkerThread;
  }

  ZgwThreadEnvHandle* thandle = new ZgwThreadEnvHandle();

  // gateway server thread
  conn_factory_ = new ZgwConnFactory();
  std::set<std::string> ips;
  ips.insert(ip_);
  zgw_dispatch_thread_ = pink::NewDispatchThread(ips, port_,
                                                 worker_num_, conn_factory_,
                                                 0, nullptr, thandle);
  zgw_dispatch_thread_->set_thread_name("ZgwDispatchThread");

  // gateway admin thread
  admin_conn_factory_ = new AdminConnFactory();
  ZgwMonitorHandle* mhandle = new ZgwMonitorHandle();
  zgw_admin_thread_ = pink::NewHolyThread(admin_port_, admin_conn_factory_,
                                          kZgwMonitorInterval, mhandle, thandle);
  zgw_admin_thread_->set_thread_name("ZgwAdminThread");
  mhandle->SetServerThread(zgw_admin_thread_);
  zgw_monitor_ = new ZgwMonitor();

  buckets_list_ = new libzgw::ListMap(libzgw::ListMap::kBuckets);
  objects_list_ = new libzgw::ListMap(libzgw::ListMap::kObjects);
}

ZgwServer::~ZgwServer() {
  delete zgw_dispatch_thread_;
  delete zgw_admin_thread_;
  delete conn_factory_;
  delete admin_conn_factory_;
  delete zgw_monitor_;

  LOG(INFO) << "ZgwServerThread " << pthread_self() << " exit!!!";
}

void ZgwServer::Exit() {
  zgw_dispatch_thread_->StopThread();
  zgw_admin_thread_->StopThread();
  should_exit_.store(true);
}

Status ZgwServer::Start() {
  Status s;
  LOG(INFO) << "Waiting for ZgwServerThread Init, maybe "<< worker_num_ * 10 << "s";
  
  if (zgw_dispatch_thread_->StartThread() != 0) {
    return Status::Corruption("Launch DispatchThread failed");
  }
  if (zgw_admin_thread_->StartThread() != 0) {
    return Status::Corruption("Launch AdminThread failed");
  }

  LOG(INFO) << "ZgwServerThread Init Success!";

  while (running()) {
    // DoTimingTask
    slash::SleepForMicroseconds(kZgwCronInterval);
  }

  return Status::OK();
}
