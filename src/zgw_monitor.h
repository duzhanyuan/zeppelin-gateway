#ifndef ZGW_MONITOR_H
#define ZGW_MONITOR_H

#include <atomic>
#include <string>
#include <map>
#include <cstdint>

#include "slash/include/slash_status.h"
#include "slash/include/env.h"
#include "src/libzgw/zgw_store.h"
#include "src/zgw_server.h"

class ZgwMonitor {
 public:
  ZgwMonitor();

  void AddBucketVol(const std::string& bucket_name, uint64_t size);
  void DelBucketVol(const std::string& bucket_name, uint64_t size);
  void AddBucketTraffic(const std::string& bucket_name, uint64_t size);
  void AddClusterTraffic(uint64_t size);
  void SetClusterVol(uint64_t meta_vol, uint64_t data_vol) {
    zgw_meta_volumn_ = meta_vol;
    zgw_data_volumn_ = data_vol;
  }

  void AddRequest();
  void AddApiRequest(S3Interface iface, uint64_t error_code);
  void UpdateUpPartTime(double time);
  void AddQueryNum();
  uint64_t qps();

  std::string GetFormatInfo();

  std::string MetaKey() const;
  std::string MetaValue() const;
  Status ParseMetaValue(std::string* value);

  bool ShouldUpdate();
  void Reset();
  void BucketVolUpdate() {
    bucket_update_time_ = slash::NowMicros();
  }
  void ClearBucketVol() {
    bucket_update_time_ = 0;
    bucket_volumn_.clear();
  }
  bool initialed;
  bool need_update_bucket_vol;

 private:
  std::atomic<uint64_t> cluster_volumn_;   // Used
  std::atomic<uint64_t> zgw_meta_volumn_;
  std::atomic<uint64_t> zgw_data_volumn_;
  std::atomic<uint64_t> cluster_traffic_;
  std::map<std::string, uint64_t> bucket_volumn_;
  std::map<std::string, uint64_t> bucket_traffic_;
  std::map<S3Interface, uint64_t> api_request_count_;
  std::map<S3Interface, uint64_t> api_err4xx_count_;
  std::map<S3Interface, uint64_t> api_err5xx_count_;
  std::atomic<uint64_t> request_count_;
  std::atomic<uint64_t> upload_part_time_;
  std::atomic<uint64_t> bucket_update_time_;

  // QPS
  uint64_t last_query_num_;
  std::atomic<uint64_t> cur_query_num_;
  uint64_t last_time_us_;
};

class ZgwMonitorHandle : public pink::ServerHandle {
 public:
  // Load monitor info from zp; flush to zp.
  virtual void CronHandle() const override {
    MaybeUpdateMonitor();
  }

  void SetServerThread(pink::ServerThread* sthread) {
    sthread_ = sthread;
  }

 private:
  pink::ServerThread* sthread_;
  mutable ZgwMonitor* zgw_monitor_;

  void MaybeUpdateMonitor() const;
  void UpdateBucketVol() const;
  void UpdateClusterVol() const;
};

#endif
