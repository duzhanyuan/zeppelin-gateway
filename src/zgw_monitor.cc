#include "src/zgw_monitor.h"

#include <string>

#include <glog/logging.h>
#include "src/zgw_config.h"

using slash::Status;

extern ZgwServer* g_zgw_server;
extern ZgwConfig* g_zgw_conf;

static const std::string kMonitorInfoPrefix = "__zgw_monitorinfo_";

ZgwMonitor::ZgwMonitor()
      : initialed(false),
        need_update_bucket_vol (false),
        zgw_meta_volumn_(0),
        zgw_data_volumn_(0),
        cluster_traffic_(0),
        request_count_(0),
        upload_part_time_(0),
        bucket_update_time_(0),
        last_query_num_(0),
        cur_query_num_(0) {
  last_time_us_ = slash::NowMicros();
}

void ZgwMonitor::AddBucketVol(const std::string& bucket_name, uint64_t size) {
  if (bucket_volumn_.count(bucket_name)) {
    bucket_volumn_[bucket_name] += size;
  } else {
    bucket_volumn_[bucket_name] = size;
  }
  uint64_t final_size = bucket_volumn_[bucket_name];
  if (final_size == 0) {
    bucket_volumn_.erase(bucket_name);
  }
}

void ZgwMonitor::AddBucketTraffic(const std::string& bucket_name, uint64_t size) {
  if (bucket_traffic_.count(bucket_name)) {
    bucket_traffic_[bucket_name] += size;
  } else {
    bucket_traffic_[bucket_name] = size;
  }
}

void ZgwMonitor::AddClusterTraffic(uint64_t size) {
  if (cluster_traffic_.load() == 0) {
    cluster_traffic_ = size;
  } else {
    cluster_traffic_ += size;
  }
}

void ZgwMonitor::DelBucketVol(const std::string& bucket_name, uint64_t size) {
  if (bucket_volumn_.count(bucket_name)) {
    auto& vol = bucket_volumn_[bucket_name];
    if (vol < size) {
      vol = 0;
    } else {
      vol -= size;
    }

    if (vol == 0) {
      bucket_volumn_.erase(bucket_name);
    }
  }
}

void ZgwMonitor::AddApiRequest(S3Interface iface, uint64_t error_code) {
  if (error_code == 0) {
    if (api_request_count_.count(iface)) {
      api_request_count_[iface] += 1;
    } else {
      api_request_count_[iface] = 1;
    }
  } else if(error_code < 500) {
    if (api_err4xx_count_.count(iface)) {
      api_err4xx_count_[iface] += 1;
    } else {
      api_err4xx_count_[iface] = 1;
    }
  } else {
    if (api_err5xx_count_.count(iface)) {
      api_err5xx_count_[iface] += 1;
    } else {
      api_err5xx_count_[iface] = 1;
    }
  }
}

void ZgwMonitor::AddRequest() {
  if (request_count_.load() == 0) {
    request_count_ = 1;
  } else {
    request_count_ += 1;
  }
}

void ZgwMonitor::UpdateUpPartTime(double time) {
  // Maybe less precise
  static int n = 1; // Sequence of data flow
  // Avg(n)=(n-1)/n*Avg(n-1)+data[n]/n
  upload_part_time_ =
    static_cast<double>((n - 1) * upload_part_time_) / n + time / n;
  ++n;
}

void ZgwMonitor::AddQueryNum() {
  cur_query_num_++;
}

uint64_t ZgwMonitor::qps() {
  uint64_t cur_time_us = slash::NowMicros();
  uint64_t qps = (cur_query_num_ - last_query_num_) * 1000000
    / (cur_time_us - last_time_us_ + 1);
  if (qps == 0) {
    cur_query_num_ = 0;
  }
  last_query_num_ = cur_query_num_;
  last_time_us_ = cur_time_us;
  return qps;
}

static std::string InterfaceString(S3Interface iface) {
  switch(iface) {
    case kAuth:
      return "Auth: ";
    case kListAllBuckets:
      return "ListAllBuckets: ";
    case kDeleteBucket:
      return "DeleteBucket: ";
    case kListObjects:
      return "ListObjects: ";
    case kHeadBucket:
      return "HeadBucket: ";
    case kListMultiPartUpload:
      return "ListMultiPartUpload: ";
    case kPutBucket:
      return "PutBucket: ";
    case kDeleteObject:
      return "DeleteObject: ";
    case kDeleteMultiObjects:
      return "DeleteMultiObjects: ";
    case kGetObject:
      return "GetObject: ";
    case kGetObjectPartial:
      return "GetObjectPartial: ";
    case kHeadObject:
      return "HeadObject: ";
    case kPostObject:
      return "PostObject: ";
    case kPutObject:
      return "PutObject: ";
    case kPutObjectCopy:
      return "PutObjectCopy: ";
    case kInitMultipartUpload:
      return "InitMultipartUpload: ";
    case kUploadPart:
      return "UploadPart: ";
    case kUploadPartCopy:
      return "UploadPartCopy: ";
    case kUploadPartCopyPartial:
      return "UploadPartCopyPartial: ";
    case kCompleteMultiUpload:
      return "CompleteMultiUpload: ";
    case kAbortMultiUpload:
      return "AbortMultiUpload: ";
    case kListParts:
      return "ListParts: ";
    case kUnknow:
      return "Unknow: ";
  }
}

std::string ZgwMonitor::GetFormatInfo() {
  std::string result;
  result += "qps: " + std::to_string(qps()) + "\r\n";
  result += "cluster meta volumn: " + std::to_string(zgw_meta_volumn_.load()) + " bytes\r\n";
  result += "cluster data volumn: " + std::to_string(zgw_data_volumn_.load()) + " bytes\r\n";
  result += "cluster traffic: " + std::to_string(cluster_traffic_.load()) + " bytes\r\n";
  result += "buckets volumn: \r\n";
  for (auto& info : bucket_volumn_) {
    result += "\t-" + info.first + " " + std::to_string(info.second) + " bytes\r\n";
  }
  result += "buckets traffic: \r\n";
  for (auto& info : bucket_traffic_) {
    result += "\t-" + info.first + " " + std::to_string(info.second) + " bytes\r\n";
  }
  result += "api request count: \r\n";
  for (auto& info : api_request_count_) {
    result += "\t-" + InterfaceString(info.first) + std::to_string(info.second) + "\r\n";
  }
  result += "api err4xx count: \r\n";
  for (auto& info : api_err4xx_count_) {
    result += "\t-" + InterfaceString(info.first) + std::to_string(info.second) + "\r\n";
  }
  result += "api err5xx count: \r\n";
  for (auto& info : api_err5xx_count_) {
    result += "\t-" + InterfaceString(info.first) + std::to_string(info.second) + "\r\n";
  }
  result += "request count: " + std::to_string(request_count_.load()) + "\r\n";
  result += "upload part time: " + std::to_string(upload_part_time_.load()) + " ms\r\n";
  return result;
}

std::string ZgwMonitor::MetaKey() const {
  char buf[128] = {0};
  gethostname(buf, 128);
  return kMonitorInfoPrefix + std::string(buf) + std::to_string(g_zgw_conf->server_port);
}

std::string ZgwMonitor::MetaValue() const {
  std::string result;
  slash::PutFixed64(&result, zgw_meta_volumn_);
  slash::PutFixed64(&result, zgw_data_volumn_);
  slash::PutFixed64(&result, cluster_traffic_);
  slash::PutFixed64(&result, bucket_volumn_.size());
  for (auto& n : bucket_volumn_) {
    slash::PutLengthPrefixedString(&result, n.first);
    slash::PutFixed64(&result, n.second);
  }
  slash::PutFixed64(&result, bucket_traffic_.size());
  for (auto&  n : bucket_traffic_) {
    slash::PutLengthPrefixedString(&result, n.first);
    slash::PutFixed64(&result, n.second);
  }
  slash::PutFixed64(&result, api_request_count_.size());
  for (auto&  n : api_request_count_) {
    slash::PutFixed32(&result, n.first);
    slash::PutFixed64(&result, n.second);
  }
  slash::PutFixed64(&result, api_err4xx_count_.size());
  for (auto&  n : api_err4xx_count_) {
    slash::PutFixed32(&result, n.first);
    slash::PutFixed64(&result, n.second);
  }
  slash::PutFixed64(&result, api_err5xx_count_.size());
  for (auto&  n : api_err5xx_count_) {
    slash::PutFixed32(&result, n.first);
    slash::PutFixed64(&result, n.second);
  }
  slash::PutFixed64(&result, request_count_);
  slash::PutFixed64(&result, upload_part_time_);

  return result;
}

Status ZgwMonitor::ParseMetaValue(std::string* value) {
  uint64_t tmp;
  bool res = true;
  slash::GetFixed64(value, &tmp);
  zgw_meta_volumn_.store(tmp);
  slash::GetFixed64(value, &tmp);
  zgw_data_volumn_.store(tmp);
  slash::GetFixed64(value, &tmp);
  cluster_traffic_ = tmp;
  slash::GetFixed64(value, &tmp);
  std::string tmpstr;
  for (uint64_t i = 0; i < tmp; i++) {
    uint64_t tmp1;
    res = res && slash::GetLengthPrefixedString(value, &tmpstr);
    slash::GetFixed64(value, &tmp1);
    bucket_volumn_.insert(std::make_pair(tmpstr, tmp1));
  }
  slash::GetFixed64(value, &tmp);
  for (uint64_t i = 0; i < tmp; i++) {
    uint64_t tmp1;
    res = res && slash::GetLengthPrefixedString(value, &tmpstr);
    slash::GetFixed64(value, &tmp1);
    bucket_traffic_.insert(std::make_pair(tmpstr, tmp1));
  }
  slash::GetFixed64(value, &tmp);
  for (uint64_t i = 0; i < tmp; i++) {
    uint32_t tmp1;
    uint64_t tmp2;
    slash::GetFixed32(value, &tmp1);
    slash::GetFixed64(value, &tmp2);
    api_request_count_.insert(std::make_pair(static_cast<S3Interface>(tmp1), tmp2));
  }
  slash::GetFixed64(value, &tmp);
  for (uint64_t i = 0; i < tmp; i++) {
    uint32_t tmp1;
    uint64_t tmp2;
    slash::GetFixed32(value, &tmp1);
    slash::GetFixed64(value, &tmp2);
    api_err4xx_count_.insert(std::make_pair(static_cast<S3Interface>(tmp1), tmp2));
  }
  slash::GetFixed64(value, &tmp);
  for (uint64_t i = 0; i < tmp; i++) {
    uint32_t tmp1;
    uint64_t tmp2;
    slash::GetFixed32(value, &tmp1);
    slash::GetFixed64(value, &tmp2);
    api_err5xx_count_.insert(std::make_pair(static_cast<S3Interface>(tmp1), tmp2));
  }
  slash::GetFixed64(value, &tmp);
  request_count_ = tmp;

  slash::GetFixed64(value, &tmp);
  upload_part_time_ = tmp;

  if (!res) {
    return Status::Corruption("");
  }

  return Status::OK();
}

bool ZgwMonitor::ShouldUpdate() {
  // 3 a.m. Everyday
  time_t now = time(0);
  struct tm* t = localtime(&now);
  int h = t->tm_hour;
  if (h >= 3 && h < 4) {
    return true;
  }
  if (need_update_bucket_vol) {
    need_update_bucket_vol = false;
    return true;
  }
  return false;
}

void ZgwMonitor::Reset() {
  ClearBucketVol();
  need_update_bucket_vol = true;
  zgw_meta_volumn_ = 0;   // Used
  zgw_data_volumn_ = 0; // Available
  cluster_traffic_ = 0;
  bucket_volumn_.clear();
  bucket_traffic_.clear();
  api_request_count_.clear();
  api_err4xx_count_.clear();
  api_err5xx_count_.clear();
  request_count_ = 0;
  upload_part_time_ = 0;
}

void ZgwMonitorHandle::MaybeUpdateMonitor() const {
  zgw_monitor_ = g_zgw_server->zgw_monitor();
  libzgw::ZgwStore* store =
    static_cast<libzgw::ZgwStore*>(sthread_->get_private());

  Status s;
  std::string value;
  if (!zgw_monitor_->initialed) {
    s = store->GetMeta(zgw_monitor_->MetaKey(), &value);
    if (!s.ok() ||
        !zgw_monitor_->ParseMetaValue(&value).ok()) {
      UpdateBucketVol();
    }
    zgw_monitor_->initialed = true;
  }

  // Update qps
  zgw_monitor_->qps();

  if (zgw_monitor_->ShouldUpdate()) {
    UpdateBucketVol();
  }

  // Update cluster vol and capacity
  UpdateClusterVol();

  store->SetMeta(zgw_monitor_->MetaKey(), zgw_monitor_->MetaValue());
}

void ZgwMonitorHandle::UpdateClusterVol() const {
  libzgw::ZgwStore* store =
    static_cast<libzgw::ZgwStore*>(sthread_->get_private());
  uint64_t meta_vol= 0;
  uint64_t data_vol = 0;
  store->GetZgwSapce(&meta_vol, &data_vol);
  zgw_monitor_->SetClusterVol(meta_vol, data_vol);
}

// Scan all Buckets and calculate volumn
void ZgwMonitorHandle::UpdateBucketVol() const {
  LOG(INFO) << "UpdateBucketVol";
  zgw_monitor_->ClearBucketVol();
  zgw_monitor_->need_update_bucket_vol = false;
  libzgw::ZgwStore* store =
    static_cast<libzgw::ZgwStore*>(sthread_->get_private());
  
  libzgw::NameList* buckets_name;
  libzgw::NameList* objects_name;
  std::set<libzgw::ZgwUser *> user_list; // name : keys
  store->ListUsers(&user_list);
  // Buckets nums
  std::string access_key;
  for (auto& user : user_list) {
    const auto& info = user->user_info();
    for (auto& key_pair : user->access_keys()) {
      access_key = key_pair.first; // access key
    }
    g_zgw_server->RefAndGetBucketList(store, access_key, &buckets_name);
    std::set<std::string> bname_list, oname_list;
    {
      std::lock_guard<std::mutex> lock(buckets_name->list_lock);
      bname_list = buckets_name->name_list;
    }
    g_zgw_server->UnrefBucketList(store, access_key);
    for (const auto& bname : bname_list) {
      g_zgw_server->RefAndGetObjectList(store, bname, &objects_name);
      {
        std::lock_guard<std::mutex> lock(objects_name->list_lock);
        oname_list = objects_name->name_list;
      }
      for (const auto& oname : oname_list) {
        libzgw::ZgwObject object(bname, oname);
        store->GetObject(&object, false);
        zgw_monitor_->AddBucketVol(bname, object.info().size);
      }
      g_zgw_server->UnrefObjectList(store, bname);
    }
  }
  zgw_monitor_->BucketVolUpdate();
}
