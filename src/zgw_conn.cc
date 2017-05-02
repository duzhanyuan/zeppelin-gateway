#include "src/zgw_conn.h"

#include <memory>
#include <cctype>
#include <cstdint>

#include "slash/include/env.h"
#include "src/libzgw/zgw_namelist.h"
#include "src/zgw_server.h"
#include "src/zgw_monitor.h"
#include "src/zgw_auth.h"
#include "src/zgw_xml.h"
#include "src/zgw_util.h"

extern ZgwServer* g_zgw_server;

void ZgwConn::PreProcessUrl() {
  bucket_name_ = UrlDecode(bucket_name_);
  object_name_ = UrlDecode(object_name_);
  for (auto& item : req_->query_params) {
    item.second = UrlDecode(item.second);
  }
}

bool ZgwConn::IsValidBucket() {
  if (bucket_name_.empty() || !object_name_.empty()) {
    return false;
  }
  char c = bucket_name_.at(0);
  if (!std::isalnum(c)) {
    return false;
  }
  return true;
}

bool ZgwConn::IsValidObject() {
  if (bucket_name_.empty() || object_name_.empty()) {
    return false;
  }
  if (object_name_.find(libzgw::kInternalObjectNamePrefix) == 0) {
    return false;
  }
  return true;
}

ZgwConn::ZgwConn(const int fd,
                 const std::string &ip_port,
                 pink::Thread* worker)
      : HttpConn(fd, ip_port, worker) {
	store_ = static_cast<libzgw::ZgwStore*>(worker->get_private());
}

void ZgwConn::DealMessage(const pink::HttpRequest* req, pink::HttpResponse* resp) {
  Timer t("DealMessage " + ip_port() + ":");
  time_t now = time(0);
  LOG(INFO) << "Request from: " << ip_port() << ", " << ctime(&now);
  // DumpHttpRequest(req);
  g_zgw_server->zgw_monitor()->AddQueryNum();
  g_zgw_server->zgw_monitor()->AddRequest();

  // Copy req and resp
  req_ = const_cast<pink::HttpRequest *>(req);
  resp_ = resp;

  // Get bucket name and object name
  if (req_->path[0] != '/') {
    resp_->SetStatusCode(500);
    resp_->SetBody("Path parse error: " + req_->path);
    g_zgw_server->zgw_monitor()->AddApiRequest(kAuth, 500);
    return;
  }

  ExtraBucketAndObject(req_->path, &bucket_name_, &object_name_);
  PreProcessUrl();

  Status s;
  // Get access key from request and secret key from zp
  {
  Timer t("Authorization" + ip_port() + ":");
  ZgwAuth zgw_auth;
  if (!zgw_auth.ParseAuthInfo(req_, &access_key_) ||
      !store_->GetUser(access_key_, &zgw_user_).ok()) {
    if (zgw_auth.IsSignatureV2()) {
      resp_->SetStatusCode(400);
      resp_->SetBody(ErrorXml(InvalidRequest, "Please use AWS4-HMAC-SHA256."));
    } else {
      resp_->SetStatusCode(403);
      resp_->SetBody(ErrorXml(InvalidAccessKeyId));
      DLOG(INFO) << "InvalidAccessKeyId";
    }
    g_zgw_server->zgw_monitor()->AddApiRequest(kAuth, 403);
    return;
  }

  // Authorize request
  if (!zgw_auth.Auth(req_, zgw_user_->secret_key(access_key_))) {
    resp_->SetStatusCode(403);
    resp_->SetBody(ErrorXml(SignatureDoesNotMatch));
    DLOG(INFO) << "Auth failed: " << ip_port() << " " << req_->headers["authorization"];
    DLOG(INFO) << "Auth failed creq: " << zgw_auth.canonical_request();
    g_zgw_server->zgw_monitor()->AddApiRequest(kAuth, 403);
    return;
  }
  DLOG(INFO) << "Auth passed: " << ip_port() << " " << req_->headers["authorization"];
  }

  // Get buckets namelist and ref
  {
  Timer t("Ref Bucket listname: ");
  s = g_zgw_server->RefAndGetBucketList(store_, access_key_, &buckets_name_);
  if (!s.ok()) {
    resp_->SetStatusCode(500);
    LOG(ERROR) << "List buckets name list failed: " << s.ToString();
    g_zgw_server->zgw_monitor()->AddApiRequest(kUnknow, 500);
    return;
  }
  }

  if (!bucket_name_.empty() && buckets_name_->IsExist(bucket_name_)) {
    Timer t("Ref Object listname: ");
    // Get objects namelist and ref
    s = g_zgw_server->RefAndGetObjectList(store_, bucket_name_, &objects_name_);
    if (!s.ok()) {
      resp_->SetStatusCode(500);
      LOG(ERROR) << "List objects name list failed: " << s.ToString();
      s = g_zgw_server->UnrefBucketList(store_, access_key_);
      g_zgw_server->zgw_monitor()->AddApiRequest(kUnknow, 500);
      return;
    }
  }

  METHOD method;
  if (req_->method == "GET") {
    method = kGet;
  } else if (req_->method == "PUT") {
    method = kPut;
  } else if (req_->method == "DELETE") {
    method = kDelete;
  } else if (req_->method == "HEAD") {
    method = kHead;
  } else if (req_->method == "POST") {
    method = kPost;
  } else {
    method = kUnsupport;
  }

  if (bucket_name_.empty()) {
    if (method == kGet) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kListAllBuckets, 0);
      ListBucketHandle();
    } else {
      // Unknow request
      resp_->SetStatusCode(405);
      resp_->SetBody(ErrorXml(MethodNotAllowed));
      g_zgw_server->zgw_monitor()->AddApiRequest(kUnknow, 500);
    }
  } else if (IsValidBucket()) {
    switch(method) {
      case kGet:
        if (req_->query_params.count("uploads")) {
          g_zgw_server->zgw_monitor()->AddApiRequest(kListMultiPartUpload, 0);
          ListMultiPartsUpload();
        } else if (req_->query_params.count("location")) {
          GetBucketLocationHandle();
        } else {
          g_zgw_server->zgw_monitor()->AddApiRequest(kListObjects, 0);
          ListObjectHandle();
        }
        break;
      case kPut:
        g_zgw_server->zgw_monitor()->AddApiRequest(kPutBucket, 0);
        PutBucketHandle();
        break;
      case kDelete:
        g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteBucket, 0);
        DelBucketHandle();
        break;
      case kHead:
        g_zgw_server->zgw_monitor()->AddApiRequest(kHeadBucket, 0);
        if (!buckets_name_->IsExist(bucket_name_)) {
          g_zgw_server->zgw_monitor()->AddApiRequest(kHeadBucket, 404);
          resp_->SetStatusCode(404);
        } else {
          resp_->SetStatusCode(200);
        }
        break;
      case kPost:
        if (req_->query_params.find("delete") != req_->query_params.end()) {
          g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteMultiObjects, 0);
          DelMultiObjectsHandle();
        }
        break;
      default:
        break;
    }
  } else if (IsValidObject()) {
    // Check whether bucket existed in namelist meta
    if (!buckets_name_->IsExist(bucket_name_)) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kUnknow, 404);
      resp_->SetStatusCode(404);
      resp_->SetBody(ErrorXml(NoSuchBucket, bucket_name_));
    } else {
      DLOG(INFO) << "Object Op: " << req_->path << " confirm bucket exist";
      g_zgw_server->ObjectLock(bucket_name_ + object_name_);
      switch(method) {
        case kGet:
          if (req_->query_params.find("uploadId") != req_->query_params.end()) {
            g_zgw_server->zgw_monitor()->AddApiRequest(kListParts, 0);
            ListParts(req_->query_params["uploadId"]);
          } else {
            GetObjectHandle();
          }
          break;
        case kPut:
          if (req_->query_params.find("partNumber") != req_->query_params.end() &&
              req_->query_params.find("uploadId") != req_->query_params.end()) {
            UploadPartHandle(req_->query_params["partNumber"],
                             req_->query_params["uploadId"]);
          } else {
            PutObjectHandle();
          }
          break;
        case kDelete:
          if (req_->query_params.find("uploadId") != req_->query_params.end()) {
            g_zgw_server->zgw_monitor()->AddApiRequest(kAbortMultiUpload, 0);
            AbortMultiUpload(req_->query_params["uploadId"]);
          } else {
            g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteObject, 0);
            DelObjectHandle();
          }
          break;
        case kHead:
            g_zgw_server->zgw_monitor()->AddApiRequest(kHeadObject, 0);
          GetObjectHandle(true);
          break;
        case kPost:
          if (req_->query_params.find("uploads") != req_->query_params.end()) {
            g_zgw_server->zgw_monitor()->AddApiRequest(kInitMultipartUpload, 0);
            InitialMultiUpload();
          } else if (req_->query_params.find("uploadId") != req_->query_params.end()) {
            g_zgw_server->zgw_monitor()->AddApiRequest(kCompleteMultiUpload, 0);
            CompleteMultiUpload(req_->query_params["uploadId"]);
          }
          break;
        default:
          break;
      }
      g_zgw_server->ObjectUnlock(bucket_name_ + object_name_);
    }
  } else {
    // Unknow request
    g_zgw_server->zgw_monitor()->AddApiRequest(kUnknow, 501);
    resp_->SetStatusCode(501);
    resp_->SetBody(ErrorXml(NotImplemented));
  }

  // Unref namelist
  {
  Timer t("Unref namelist: ");
  Status s1 = Status::OK();
  s = g_zgw_server->UnrefBucketList(store_, access_key_);
  if (!bucket_name_.empty()) {
    s1 = g_zgw_server->UnrefObjectList(store_, bucket_name_);
  }
  if (!s.ok() || !s1.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kUnknow, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "Unref namelist failed: " << s.ToString();
    return;
  }
  }

  resp_->SetHeaders("Date", http_nowtime(time(NULL)));
  resp_->SetHeaders("x-amz-request-id", "tx00000000000000000113c-0058a43a07-7deaf-sh-bt-1"); // TODO (gaodq)
}

void ZgwConn::InitialMultiUpload() {
  std::string upload_id, internal_obname;

  timeval now;
  gettimeofday(&now, NULL);
  libzgw::ZgwObjectInfo ob_info(now, "", 0, libzgw::kStandard, zgw_user_->user_info());
  upload_id.assign(md5(object_name_ + std::to_string(now.tv_sec * 1000000 + now.tv_usec)));
  internal_obname.assign(libzgw::kInternalObjectNamePrefix + object_name_ + upload_id);

  libzgw::ZgwObject object(bucket_name_, internal_obname, "", ob_info);
  object.SetUploadId(upload_id);
  Status s = store_->AddObject(object);
  if (!s.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kInitMultipartUpload, 500);
    resp_->SetStatusCode(500);
    return;
  }
  DLOG(INFO) << "Get upload id, and insert multiupload meta to zp";

  // Insert into namelist
  objects_name_->Insert(internal_obname);
  DLOG(INFO) << "Insert into namelist: " << internal_obname;

  // Success Response
  resp_->SetBody(InitiateMultipartUploadResultXml(bucket_name_,
                                                       object_name_, upload_id));
  resp_->SetStatusCode(200);
}

void ZgwConn::UploadPartHandle(const std::string& part_num, const std::string& upload_id) {
  std::string internal_obname = libzgw::kInternalObjectNamePrefix + object_name_ + upload_id;
  if (!objects_name_->IsExist(internal_obname)) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kUploadPart, 404);
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchUpload, upload_id));
    return;
  }

  uint64_t upload_part_start = slash::NowMicros();
  Status s;
  timeval now;
  gettimeofday(&now, NULL);
  // Handle copy operation
  std::string src_bucket_name, src_object_name, etag;
  bool is_copy_op = !req_->headers["x-amz-copy-source"].empty();
  std::string object_content;
  if (is_copy_op) {
    Timer t("UploadPart: GetSourceObject");
    g_zgw_server->zgw_monitor()->AddApiRequest(kUploadPartCopy, 0);
    bool res = GetSourceObject(&object_content);
    DLOG(INFO) << "UploadPart: " << "SourceObject Size: " << object_content.size();
    if (!res) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kUploadPartCopy, 500);
      return;
    }
  } else {
    g_zgw_server->zgw_monitor()->AddApiRequest(kUploadPart, 0);
    object_content = req_->content;
    DLOG(INFO) << "UploadPart: " << "Part Size: " << object_content.size();
  }
  {
  Timer t("UploadPart: Calc md5");
  etag.assign("\"" + md5(object_content) + "\"");
  }
  libzgw::ZgwObjectInfo ob_info(now, etag, object_content.size(), libzgw::kStandard,
                                zgw_user_->user_info());
  {
  Timer t("UploadPart: UploadPart to zp");
  s = store_->UploadPart(bucket_name_, internal_obname, ob_info, object_content,
                         std::atoi(part_num.c_str()));
  }
  if (!s.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kUploadPart, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "UploadPart data failed: " << s.ToString();
    return;
  }
  DLOG(INFO) << "UploadPart: " << req_->path << " confirm add to zp success";

  if (is_copy_op) {
    resp_->SetBody(CopyObjectResultXml(now, etag));
  }

  double upload_time = static_cast<double>(slash::NowMicros() - upload_part_start) / 1000;
  DLOG(INFO) << "UploadPart: time: " << upload_time << " ms", 
  g_zgw_server->zgw_monitor()->UpdateUpPartTime(upload_time);
  resp_->SetHeaders("ETag", etag);
  resp_->SetStatusCode(200);
}

void ZgwConn::CompleteMultiUpload(const std::string& upload_id) {
  Status s;
  std::string internal_obname = libzgw::kInternalObjectNamePrefix + object_name_ + upload_id;
  if (!objects_name_->IsExist(internal_obname)) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kCompleteMultiUpload, 404);
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchUpload, upload_id));
    return;
  }
  DLOG(INFO) << "CompleteMultiUpload: " << req_->path << " confirm upload id exist";

  // Check every part's etag and part num
  std::vector<std::pair<int, std::string>> recv_parts;
  if (!ParseCompleteMultipartUploadXml(req_->content, &recv_parts) ||
      recv_parts.empty()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kCompleteMultiUpload, 400);
    resp_->SetStatusCode(400);
    resp_->SetBody(ErrorXml(MalformedXML));
    return;
  }
  std::vector<std::pair<int, libzgw::ZgwObject>> store_parts;
  {
  Timer t("CompleteMultiUpload: ListParts");
  s = store_->ListParts(bucket_name_, internal_obname, &store_parts);
  }
  if (!s.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kCompleteMultiUpload, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "CompleteMultiUpload failed in list object parts: " << s.ToString();
    return;
  }
  std::set<int> existed_parts;
  for (auto& it : store_parts) {
    existed_parts.insert(it.first);
  }
  if (recv_parts.size() != store_parts.size()) {
    resp_->SetStatusCode(400);
    resp_->SetBody(ErrorXml(InvalidPart));
  }
  for (size_t i = 0; i < recv_parts.size(); i++) {
    // check part num order and existance
    if (existed_parts.find(recv_parts[i].first) == existed_parts.end()) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kCompleteMultiUpload, 400);
      resp_->SetStatusCode(400);
      resp_->SetBody(ErrorXml(InvalidPart));
      return;
    }
    if (recv_parts[i].first != store_parts[i].first) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kCompleteMultiUpload, 400);
      resp_->SetStatusCode(400);
      resp_->SetBody(ErrorXml(InvalidPartOrder));
      return;
    }
    // Check etag
    const libzgw::ZgwObjectInfo& info = store_parts[i].second.info();
    if (info.etag != recv_parts[i].second) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kCompleteMultiUpload, 400);
      resp_->SetStatusCode(400);
      resp_->SetBody(ErrorXml(InvalidPart));
      return;
    }
  }

  // Delete old object data
  if (objects_name_->IsExist(object_name_)) {
    s = store_->DelObject(bucket_name_, object_name_);
    if (!s.ok() && !s.IsNotFound()) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kCompleteMultiUpload, 500);
      resp_->SetStatusCode(500);
      LOG(ERROR) << "CompleteMultiUpload failed in delete old object: " << s.ToString();
      return;
    }
    DLOG(INFO) << "CompleteMultiUpload: " << req_->path << " confirm delete old object";
  }

  // Update object meta in zp
  std::string final_etag;
  {
  Timer t("CompleteMultiUpload: CompleteMultiUpload to zp");
  s = store_->CompleteMultiUpload(bucket_name_, internal_obname, store_parts, &final_etag);
  }
  if (!s.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kCompleteMultiUpload, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "CompleteMultiUpload failed: " << s.ToString();
    return;
  }
  DLOG(INFO) << "CompleteMultiUpload: " << req_->path << " confirm zp's objects change name";

  objects_name_->Insert(object_name_);
  objects_name_->Delete(internal_obname);

  resp_->SetStatusCode(200);
  final_etag.erase(final_etag.size() - 1); // erase last '"'
  final_etag += "-" + std::to_string(store_parts.size()) + "\"";
  resp_->SetHeaders("ETag", final_etag);
  resp_->SetBody(CompleteMultipartUploadResultXml(bucket_name_,
                                                       object_name_,
                                                       final_etag));
}

void ZgwConn::AbortMultiUpload(const std::string& upload_id) {
  std::string internal_obname = libzgw::kInternalObjectNamePrefix + object_name_ + upload_id;
  if (!objects_name_->IsExist(internal_obname)) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kAbortMultiUpload, 404);
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchUpload, upload_id));
    return;
  }

  Status s = store_->DelObject(bucket_name_, internal_obname);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      // But founded in list meta, continue to delete from list meta
    } else {
      g_zgw_server->zgw_monitor()->AddApiRequest(kAbortMultiUpload, 500);
      resp_->SetStatusCode(500);
      LOG(ERROR) << "AbortMultiUpload failed: " << s.ToString();
      return;
    }
  }

  objects_name_->Delete(internal_obname);
  DLOG(INFO) << "AbortMultiUpload: " << req_->path << " confirm delete object meta from namelist success";

  // Success
  resp_->SetStatusCode(204);
}

void ZgwConn::ListParts(const std::string& upload_id) {
  Status s;
  std::string internal_obname = libzgw::kInternalObjectNamePrefix + object_name_ + upload_id;
  if (!objects_name_->IsExist(internal_obname)) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kListParts, 404);
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchUpload, upload_id));
    return;
  }
  DLOG(INFO) << "ListParts: " << req_->path << " confirm upload exist";

  std::string part_num_marker = req_->query_params["part-number-marker"];
  std::string mus = req_->query_params["max-parts"];
  int max_parts = 0;
  if (!mus.empty()) {
    max_parts = std::atoi(mus.c_str());
    if (max_parts <= 0 && !isdigit(mus[0])) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kListParts, 400);
      resp_->SetStatusCode(400);
      resp_->SetBody(ErrorXml(InvalidArgument, "max-parts"));
      return;
    }
  } else {
    max_parts = 1000;
  }
  bool is_trucated = false;
  std::vector<std::pair<int, libzgw::ZgwObject>> parts, needed_parts;
  {
  Timer t("ListParts: ListParts from zp");
  s = store_->ListParts(bucket_name_, internal_obname, &parts);
  }
  if (!s.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kListParts, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "ListParts failed: " << s.ToString();
    return;
  }

  for (auto& part : parts) {
    if (needed_parts.size() >= static_cast<size_t>(max_parts)) {
      if (max_parts > 0)
        is_trucated = true;
      break;
    }
    if (!part_num_marker.empty() &&
        std::to_string(part.first) < part_num_marker) {
      continue;
    }
    needed_parts.push_back(part);
  }

  std::map<std::string, std::string> args{
    {"Bucket", bucket_name_},
    {"Key", object_name_},
    {"UploadId", upload_id},
    {"StorageClass", "STANDARD"},
    {"PartNumberMarker", part_num_marker.empty() ? "0" : part_num_marker},
    {"NextPartNumberMarker", (!is_trucated || needed_parts.empty()) ? "0" :
      std::to_string(needed_parts.back().first)},
    {"MaxParts", std::to_string(max_parts)},
    {"IsTruncated", is_trucated ? "true" : "false"},
  };
  resp_->SetStatusCode(200);
  resp_->SetBody(ListPartsResultXml(needed_parts, zgw_user_->user_info(), args));
}

void ZgwConn::ListMultiPartsUpload() {
  // Check whether bucket existed in namelist meta
  if (!buckets_name_->IsExist(bucket_name_)) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kListMultiPartUpload, 404);
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchBucket, bucket_name_));
    return;
  }
  DLOG(INFO) << "ListMultiPartsUpload: " << req_->path << " confirm bucket exist";

  std::string delimiter = req_->query_params["delimiter"];
  if (!delimiter.empty() && delimiter.size() != 1) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kListMultiPartUpload, 400);
    resp_->SetStatusCode(400);
    resp_->SetBody(ErrorXml(InvalidArgument, "delimiter"));
    return;
  }
  std::string prefix = req_->query_params["prefix"];
  std::string mus = req_->query_params["max-uploads"];
  int max_uploads = 0;
  if (!mus.empty()) {
    max_uploads = std::atoi(mus.c_str());
    if (max_uploads <= 0 && !isdigit(mus[0])) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kListMultiPartUpload, 400);
      resp_->SetStatusCode(400);
      resp_->SetBody(ErrorXml(InvalidArgument, "max-uploads"));
      return;
    }
  } else {
    max_uploads = 1000;
  }
  std::string key_marker = req_->query_params["key-marker"];
  std::string upload_id_marker = req_->query_params["upload-id-marker"];

  bool is_trucated = false;
  Status s;
  std::set<std::string> commonprefixes;
  std::vector<std::string> candidate_names;
  std::vector<libzgw::ZgwObject> objects;
  {
    std::lock_guard<std::mutex> lock(objects_name_->list_lock);
    for (auto &name : objects_name_->name_list) {
      if (name.compare(0, 2, libzgw::kInternalObjectNamePrefix) != 0) {
        // Skip ordinary object
        continue;
      }
      if (!prefix.empty() &&
          name.substr(2, std::min(name.size(), prefix.size())) != prefix) {
        // Skip prefix
        continue;
      }
      size_t marker_size = 2 + key_marker.size() + upload_id_marker.size();
      if (!key_marker.empty() &&
          !upload_id_marker.empty() &&
          name.substr(0, std::min(name.size(), marker_size)) <=
          ("__" + key_marker + upload_id_marker)) {
        continue;
      }
      if (!delimiter.empty()) {
        size_t pos = name.find_first_of(delimiter, 2 + prefix.size());
        if (pos != std::string::npos) {
          commonprefixes.insert(name.substr(2, std::min(name.size(),
                                                        pos - 2 + 1)));
          continue;
        }
      }

      candidate_names.push_back(name);
    }
  }

  int diff = commonprefixes.size() + candidate_names.size()
    - static_cast<size_t>(max_uploads);
  is_trucated = diff > 0;
  if (is_trucated) {
    size_t extra_num = diff;
    if (commonprefixes.size() >= extra_num) {
      for (auto it = commonprefixes.rbegin();
           it != commonprefixes.rend() && extra_num--;) {
        commonprefixes.erase(*(it++));
      }
    } else {
      extra_num -= commonprefixes.size();
      commonprefixes.clear();
      if (candidate_names.size() <= extra_num) {
        candidate_names.clear();
      } else {
        while (extra_num--)
          candidate_names.pop_back();
      }
    }
  }

  is_trucated = is_trucated && max_uploads > 0;
  std::string next_key_marker;
  if (is_trucated) {
    if (!commonprefixes.empty()) {
      next_key_marker = *commonprefixes.rbegin();
    } else if (!candidate_names.empty()) {
      next_key_marker = candidate_names.back();
      size_t marker_size = next_key_marker.size() - 32 -2;
      next_key_marker =
        next_key_marker.substr(2, std::min(next_key_marker.size(), marker_size));
    }
  }

  {
  Timer t("ListObjects: ListObjects from zp");
  s = store_->ListObjects(bucket_name_, candidate_names, &objects);
  }
  if (!s.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kListMultiPartUpload, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "ListMultiPartsUpload failed: " << s.ToString();
    return;
  }

  std::map<std::string, std::string> args {
    {"Bucket", bucket_name_},
    {"KeyMarker", key_marker},
    {"Prefix", prefix},
    {"Delimiter", delimiter},
    {"MaxUploads", std::to_string(max_uploads)},
    {"IsTruncated", is_trucated ? "true" : "false"},
  };

  if (!next_key_marker.empty()) {
    args.insert(std::make_pair("NextKeyMarker", next_key_marker));
    std::string upload_id;
    if (is_trucated && !objects.empty()) {
      upload_id = objects.back().upload_id();
    }
    args.insert(std::make_pair("NextUploadIdMarker", upload_id));
  }
  resp_->SetStatusCode(200);
  resp_->SetBody(ListMultipartUploadsResultXml(objects, args, commonprefixes));
}

void ZgwConn::DelMultiObjectsHandle() {
  if (!buckets_name_->IsExist(bucket_name_)) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteMultiObjects, 404);
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchBucket, bucket_name_));
    return;
  }

  std::vector<std::string> keys;
  if (!ParseDelMultiObjectXml(req_->content, &keys)) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteMultiObjects, 400);
    resp_->SetStatusCode(400);
    resp_->SetBody(ErrorXml(MalformedXML));
  }
  std::vector<std::string> success_keys;
  std::map<std::string, std::string> error_keys;
  Status s;
  for (auto &key : keys) {
    DLOG(INFO) << "DeleteMuitiObjects: " << key;
    if (objects_name_->IsExist(key)) {
      s = store_->DelObject(bucket_name_, key);
      if (!s.ok()) {
        error_keys.insert(std::make_pair(key, "InternalError"));
        continue;
      }
    }
    objects_name_->Delete(key);
    success_keys.push_back(key);
  }
  resp_->SetBody(DeleteResultXml(success_keys, error_keys));
  resp_->SetStatusCode(200);
}

void ZgwConn::DelObjectHandle() {
  DLOG(INFO) << "DeleteObject: " << bucket_name_ << "/" << object_name_;

  // Check whether object existed in namelist meta
  if (!objects_name_->IsExist(object_name_)) {
    resp_->SetStatusCode(204);
    return;
  }
  DLOG(INFO) << "DelObject: " << req_->path << " confirm object exist";

  // Delete object
  Status s = store_->DelObject(bucket_name_, object_name_);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      // But founded in list meta, continue to delete from list meta
    } else {
      g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteObject, 500);
      resp_->SetStatusCode(500);
      LOG(ERROR) << "Delete object data failed: " << s.ToString();
      return;
    }
  }
  DLOG(INFO) << "DelObject: " << req_->path << " confirm delete object from zp success";

  // Delete from list meta
  objects_name_->Delete(object_name_);

  DLOG(INFO) << "DelObject: " << req_->path << " confirm delete object meta from namelist success";

  // Success
  resp_->SetStatusCode(204);
}

bool ZgwConn::ParseRange(const std::string& range,
                         std::vector<std::pair<int, uint32_t>>* segments) {
  // Check whether range is valid
  size_t pos = range.find("bytes=");
  if (pos == std::string::npos) {
    resp_->SetStatusCode(400);
    resp_->SetBody(ErrorXml(InvalidArgument, "range"));
    return false;
  }
  std::string range_header = range.substr(6);
  std::vector<std::string> elems;
  slash::StringSplit(range_header, ',', elems);
  for (auto& elem : elems) {
    int start = 0;
    uint32_t end = UINT32_MAX;
    int res = sscanf(elem.c_str(), "%d-%d", &start, &end);
    if (res > 0) {
      if (start > 0 && end < static_cast<uint32_t>(start)) {
        resp_->SetStatusCode(416);
        resp_->SetBody(ErrorXml(InvalidRange, bucket_name_));
        return false;
      }
      segments->push_back(std::make_pair(start, end));
      break; // Support one range for now
    }
  }
  return true;
}

void ZgwConn::GetObjectHandle(bool is_head_op) {
  DLOG(INFO) << "GetObjects: " << bucket_name_ << "/" << object_name_;

  if (!objects_name_->IsExist(object_name_)) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kGetObject, 404);
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchKey, object_name_));
    return;
  }
  DLOG(INFO) << "GetObject: " << req_->path << " confirm object exist";

  // Get object
  Status s;
  std::vector<std::pair<int, uint32_t>> segments;
  if (!req_->headers["range"].empty() &&
      !ParseRange(req_->headers["range"], &segments)) {
    return;
  }
  libzgw::ZgwObject object(bucket_name_, object_name_);
  bool need_content = !is_head_op;
  bool need_partial = !segments.empty();

  if (need_partial && need_content) {
    for (auto& seg : segments) {
      std::cout << seg.first << " - " << seg.second << std::endl;
    }
    Timer t("GetObject: GetPartialObject");
    g_zgw_server->zgw_monitor()->AddApiRequest(kGetObjectPartial, 0);
    s = store_->GetPartialObject(&object, segments);
  } else {
    Timer t("GetObject: ");
    g_zgw_server->zgw_monitor()->AddApiRequest(kGetObject, 0);
    s = store_->GetObject(&object, need_content);
  }
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(WARNING) << "Data size maybe strip count error";
    } else if (s.IsEndFile()) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kGetObjectPartial, 416);
      resp_->SetStatusCode(416);
      resp_->SetBody(ErrorXml(InvalidRange, bucket_name_));
      return;
    } else {
      g_zgw_server->zgw_monitor()->AddApiRequest(kGetObject, 500);
      resp_->SetStatusCode(500);
      LOG(ERROR) << "Get object data failed: " << s.ToString();
      return;
    }
  }
  DLOG(INFO) << "GetObject: " << req_->path << " confirm get object from zp success";
  DLOG(INFO) << "GetObject: " << req_->path << " Size: " << object.info().size;

  resp_->SetHeaders("Last-Modified", http_nowtime(object.info().mtime.tv_sec));
  resp_->SetBody(object.content());
  resp_->SetHeaders("Content-Length", object.info().size);
  resp_->SetHeaders("ETag", object.info().etag);
  if (need_partial) {
    char buf[256] = {0};
    sprintf(buf, "bytes %d-%u/%lu", segments[0].first, segments[0].second, object.info().size);
    resp_->SetHeaders("Content-Range", std::string(buf));
    resp_->SetHeaders("Content-Length", object.content().size());
    resp_->SetStatusCode(206);
  } else {
    resp_->SetStatusCode(200);
  }
}

bool ZgwConn::GetSourceObject(std::string* content) {
  std::string src_bucket_name, src_object_name;
  auto& source = req_->headers.at("x-amz-copy-source");
  DLOG(INFO) << "Copy source object: " << source;
  ExtraBucketAndObject(source, &src_bucket_name, &src_object_name);
  DLOG(INFO) << "Copy source object: " << src_bucket_name << " " << src_object_name;
  if (src_bucket_name.empty() || src_object_name.empty()) {
    resp_->SetStatusCode(400);
    resp_->SetBody(ErrorXml(InvalidArgument, "x-amz-copy-source"));
    return false;
  }
  if (!buckets_name_->IsExist(src_bucket_name)) {
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchBucket, src_bucket_name));
    return false;
  }

  libzgw::NameList *tmp_obnames = NULL;
  Status s = g_zgw_server->RefAndGetObjectList(store_, src_bucket_name, &tmp_obnames);
  if (!s.ok()) {
    resp_->SetStatusCode(500);
    LOG(ERROR) << "Ref objects name list failed: " << s.ToString();
    return false;
  }
  if (tmp_obnames == NULL || !tmp_obnames->IsExist(src_object_name)) {
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchKey, src_object_name));
    g_zgw_server->UnrefObjectList(store_, src_bucket_name);
    return false;
  }
  g_zgw_server->UnrefObjectList(store_, src_bucket_name);

  // Get source object
  libzgw::ZgwObject src_object(src_bucket_name, src_object_name);
  std::vector<std::pair<int, uint32_t>> segments;
  if (!req_->headers["x-amz-copy-source-range"].empty() &&
      !ParseRange(req_->headers["x-amz-copy-source-range"], &segments)) {
    return false;
  }
  bool need_partial = !segments.empty();
  if (need_partial) {
    DLOG(INFO) << "Get partial object: " << source << " " << segments[0].first << "-" << segments[0].second;
    Timer t("GetObject: GetPartialObject from zp");
    g_zgw_server->zgw_monitor()->AddApiRequest(kUploadPartCopyPartial, 0);
    s = store_->GetPartialObject(&src_object, segments);
    if (s.IsEndFile()) {
      resp_->SetStatusCode(416);
      resp_->SetBody(ErrorXml(InvalidRange, bucket_name_));
      return false;
    }
  } else {
    Timer t("GetObject: GetObject from zp");
    s = store_->GetObject(&src_object, true);
  }
  if (!s.ok()) {
    resp_->SetStatusCode(500);
    LOG(ERROR) << "Put copy object data failed: " << s.ToString();
    return false;
  }

  content->assign(src_object.content());
  return true;
}

void ZgwConn::PutObjectHandle() {
  DLOG(INFO) << "PutObjcet: " << req_->path << " Size: " << req_->content.size();

  Status s;
  timeval now;
  gettimeofday(&now, NULL);
  std::string etag;
  std::string object_content;
  // Handle copy operation
  bool is_copy_op = !req_->headers["x-amz-copy-source"].empty();
  if (is_copy_op) {
    Timer t("PutObject: GetSourceObject");
    g_zgw_server->zgw_monitor()->AddApiRequest(kPutObjectCopy, 0);
    bool res = GetSourceObject(&object_content);
    DLOG(INFO) << "PutObject: " << "SourceObject Size: " << object_content.size();
    if (!res) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kPutObjectCopy, 500);
      return;
    }
  } else {
    g_zgw_server->zgw_monitor()->AddApiRequest(kPutObject, 0);
    object_content = req_->content;
  }
  {
  Timer t("PutObject: Calc md5");
  etag.assign("\"" + md5(object_content) + "\"");
  }
  libzgw::ZgwObjectInfo ob_info(now, etag, object_content.size(), libzgw::kStandard,
                                zgw_user_->user_info());
  libzgw::ZgwObject object(bucket_name_, object_name_, object_content, ob_info);
  {
  Timer t("PutObject: AddObject");
  s = store_->AddObject(object);
  }
  if (!s.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kPutObject, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "Put object data failed: " << s.ToString();
    return;
  }
  DLOG(INFO) << "PutObject: " << req_->path << " confirm add to zp success";

  // Put object to list meta
  objects_name_->Insert(object_name_);

  DLOG(INFO) << "PutObject: " << req_->path << " confirm add to namelist success";

  if (is_copy_op) {
    resp_->SetBody(CopyObjectResultXml(now, etag));
  }
  resp_->SetHeaders("ETag", etag);
  resp_->SetStatusCode(200);
}

void ZgwConn::GetBucketLocationHandle() {
  DLOG(INFO) << "GetBucketLocation: " << bucket_name_;
  // Check whether bucket existed in namelist meta
  if (!buckets_name_->IsExist(bucket_name_)) {
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchBucket, bucket_name_));
    return;
  }
  DLOG(INFO) << "GetBucketLocation: " << req_->path << " confirm bucket exist";

  resp_->SetHeaders("Content-Type", "text/plain");
  resp_->SetBody(GetBucketLocationXml());
  resp_->SetStatusCode(200);
}

void ZgwConn::ListObjectHandle() {
  DLOG(INFO) << "ListObjects: " << bucket_name_;

  // Check whether bucket existed in namelist meta
  if (!buckets_name_->IsExist(bucket_name_)) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kListObjects, 404);
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchBucket, bucket_name_));
    return;
  }
  DLOG(INFO) << "ListObjects: " << req_->path << " confirm bucket exist";

  std::string delimiter = req_->query_params["delimiter"];
  if (!delimiter.empty() && delimiter.size() != 1) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kListObjects, 400);
    resp_->SetStatusCode(400);
    resp_->SetBody(ErrorXml(InvalidArgument, "delimiter"));
    return;
  }
  std::string prefix = req_->query_params["prefix"];
  std::string mks = req_->query_params["max-keys"];
  int max_keys = 0;
  if (!mks.empty()) {
    max_keys = std::atoi(mks.c_str());
    if (max_keys <= 0 && !isdigit(mks[0])) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kListObjects, 400);
      resp_->SetStatusCode(400);
      resp_->SetBody(ErrorXml(InvalidArgument, "max-keys"));
      return;
    }
  } else {
    max_keys = 1000;
  }
  std::string marker = req_->query_params["marker"];
  std::string start_after = req_->query_params["start-after"];
  std::string is_listv2 = req_->query_params["list-type"];
  if (!is_listv2.empty() && is_listv2 != "2") {
    g_zgw_server->zgw_monitor()->AddApiRequest(kListObjects, 400);
    resp_->SetStatusCode(400);
    resp_->SetBody(ErrorXml(InvalidArgument, "list-type"));
    return;
  }

  bool is_trucated = false;

  // Get objects meta from zp
  Status s;
  std::set<std::string> commonprefixes;
  std::vector<std::string> candidate_names;
  std::vector<libzgw::ZgwObject> objects;
  {
    std::lock_guard<std::mutex> lock(objects_name_->list_lock);
    for (auto &name : objects_name_->name_list) {
      if (name.compare(0, 2, libzgw::kInternalObjectNamePrefix) == 0) {
        // Skip Internal Object
        continue;
      }
      if (!prefix.empty() &&
          name.substr(0, std::min(name.size(), prefix.size())) != prefix) {
        // Skip prefix
        continue;
      }
      if (!start_after.empty() && !is_listv2.empty() &&
          name.substr(0, std::min(name.size(), start_after.size())) < start_after) {
        // Skip start after v2
        continue;
      }
      if (!marker.empty() && is_listv2.empty() &&
          name.substr(0, std::min(name.size(), marker.size())) <= marker) {
        // Skip marker v1
        continue;
      }
      if (!delimiter.empty()) {
        size_t pos = name.find_first_of(delimiter, prefix.size());
        if (pos != std::string::npos) {
          commonprefixes.insert(name.substr(0, std::min(name.size(), pos + 1)));
          continue;
        }
      }

      candidate_names.push_back(name);
    }
  }

  // Success Http response
  int diff = commonprefixes.size() + candidate_names.size()
    - static_cast<size_t>(max_keys);
  is_trucated = diff > 0;
  std::string next_token;
  if (is_trucated) {
    size_t extra_num = diff;
    if (commonprefixes.size() >= extra_num) {
      for (auto it = commonprefixes.rbegin();
           it != commonprefixes.rend() && extra_num--;) {
        next_token = *(it++);
        commonprefixes.erase(next_token);
      }
    } else {
      // extra_num > commonprefixes.size()
      extra_num -= commonprefixes.size();
      commonprefixes.clear();
      if (candidate_names.size() <= extra_num) {
        candidate_names.clear();
      } else {
        while (extra_num--) {
          next_token = candidate_names.back();
          candidate_names.pop_back();
        }
      }
    }
  }
  // Is not trucated if max keys equal zero
  is_trucated = is_trucated && max_keys > 0;
  std::string next_marker;
  if (is_trucated &&
      !delimiter.empty()) {
    if (!commonprefixes.empty()) {
      next_marker = *commonprefixes.rbegin();
    } else if (!candidate_names.empty()) {
      next_marker = candidate_names.back();
    }
  }

  if (!is_trucated) {
    next_token.clear();
  }

  std::map<std::string, std::string> args {
    {"Name", bucket_name_},
    {"Prefix", prefix},
    {"Delimiter", delimiter},
    {"MaxKeys", std::to_string(max_keys)},
    {"IsTruncated", is_trucated ? "true" : "false"},
  };
  if (is_listv2.empty()) {
    // ListObject v1
    args.insert(std::make_pair("Marker", marker));
    if (!next_marker.empty()) {
      args.insert(std::make_pair("NextMarker", next_marker));
    }
  } else {
    // ListObject v2
    args.insert(std::make_pair("StartAfter", start_after));
    int key_count = commonprefixes.size() + candidate_names.size();
    args.insert(std::make_pair("KeyCount", std::to_string(key_count)));
    args.insert(std::make_pair("NextContinuationToken", next_token));
  }

  {
  Timer t("ListObjects: ListObjects");
  s = store_->ListObjects(bucket_name_, candidate_names, &objects);
  }
  if (!s.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kListObjects, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "ListObjects failed: " << s.ToString();
    return;
  }
  DLOG(INFO) << "ListObjects: " << req_->path << " confirm get objects' meta from zp success";

  resp_->SetBody(ListObjectsXml(objects, args, commonprefixes));
  resp_->SetStatusCode(200);
}

void ZgwConn::DelBucketHandle() {
  DLOG(INFO) << "DeleteBucket: " << bucket_name_;
  // Check whether bucket existed in namelist meta
  if (!buckets_name_->IsExist(bucket_name_)) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteBucket, 404);
    resp_->SetStatusCode(404);
    resp_->SetBody(ErrorXml(NoSuchBucket, bucket_name_));
    return;
  }
  DLOG(INFO) << "DeleteBucket: " << req_->path << " confirm bucket exist";
  // Need not check return value

  Status s;
  // CheckOwner
  libzgw::ZgwBucket bucket(bucket_name_);
  s = store_->GetBucket(&bucket);
  if (!s.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteBucket, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "DeleteBucket: Get bucekt meta failed: " << s.ToString();
  }
  if (zgw_user_->user_info().user_id != bucket.user_info().user_id) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteBucket, 403);
    resp_->SetStatusCode(403);
    resp_->SetBody(ErrorXml(AccessDenied));
    LOG(ERROR) << "DeleteBucket: not own by user: " << zgw_user_->user_info().disply_name;
  }

  // AbortAllMultiPartUpload
  if (objects_name_ != NULL && !objects_name_->IsEmpty()) {
    bool has_normal_object = false;
    std::lock_guard<std::mutex> lock(objects_name_->list_lock);
    for (auto &name : objects_name_->name_list) {
      if (name.find(libzgw::kInternalObjectNamePrefix) != 0) {
        has_normal_object = true;
        break;
      }
    }
    if (!has_normal_object) {
      for (auto &name : objects_name_->name_list) {
        s = store_->DelObject(bucket_name_, name);
        if (!s.ok()) {
          g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteBucket, 500);
          resp_->SetStatusCode(500);
          LOG(ERROR) << "Delete bucket failed: " << s.ToString();
        }
      }
      objects_name_->name_list.clear();
    } else {
      g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteBucket, 409);
      resp_->SetStatusCode(409);
      resp_->SetBody(ErrorXml(BucketNotEmpty, bucket_name_));
      LOG(ERROR) << "DeleteBucket: BucketNotEmpty";
      return;
    }
  }

  s = store_->DelBucket(bucket_name_);
  if (s.ok()) {
    buckets_name_->Delete(bucket_name_);
    resp_->SetStatusCode(204);
  } else if (s.IsIOError()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kDeleteBucket, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "Delete bucket failed: " << s.ToString();
  }

  DLOG(INFO) << "DelBucket: " << req_->path << " confirm delete from namelist success";
}

void ZgwConn::PutBucketHandle() {
  DLOG(INFO) << "CreateBucket: " << bucket_name_;

  // Check whether bucket existed in namelist meta
  if (buckets_name_->IsExist(bucket_name_)) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kPutBucket, 409);
    resp_->SetStatusCode(409);
    resp_->SetBody(ErrorXml(BucketAlreadyOwnedByYou, ""));
    return;
  }

  // Check whether belong to other user
  std::set<libzgw::ZgwUser *> user_list; // name : keys
  Status s = store_->ListUsers(&user_list);
  if (!s.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kPutBucket, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "Create bucket failed: " << s.ToString();
  }
  libzgw::NameList *tmp_bk_list;
  bool already_exist = false;
  for (auto user : user_list) {
    auto access_key = user->access_key();
    s = g_zgw_server->RefAndGetBucketList(store_, access_key, &tmp_bk_list);
    if (!s.ok()) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kPutBucket, 500);
      resp_->SetStatusCode(500);
      LOG(ERROR) << "Create bucket failed: " << s.ToString();
      return;
    }
    if (tmp_bk_list->IsExist(bucket_name_)) {
      already_exist = true;
    }
    s = g_zgw_server->UnrefBucketList(store_, access_key);
    if (!s.ok()) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kPutBucket, 500);
      resp_->SetStatusCode(500);
      LOG(ERROR) << "Create bucket failed: " << s.ToString();
      return;
    }
    if (already_exist) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kPutBucket, 409);
      resp_->SetStatusCode(409);
      resp_->SetBody(ErrorXml(BucketAlreadyExists));
      return;
    }
  }

  DLOG(INFO) << "ListObjects: " << req_->path << " confirm bucket not exist";

  // Create bucket in zp
  s = store_->AddBucket(bucket_name_, zgw_user_->user_info());
  if (!s.ok()) {
      g_zgw_server->zgw_monitor()->AddApiRequest(kPutBucket, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "Create bucket failed: " << s.ToString();
    return;
  }
  DLOG(INFO) << "PutBucket: " << req_->path << " confirm add bucket to zp success";

  // Create list meta info
  buckets_name_->Insert(bucket_name_);

  DLOG(INFO) << "PutBucket: " << req_->path << " confirm add bucket to namelist success";

  // Success
  resp_->SetStatusCode(200);
}

void ZgwConn::ListBucketHandle() {
  DLOG(INFO) << "ListBuckets: ";

  // Load bucket info from zp
  Status s;
  std::vector<libzgw::ZgwBucket> buckets;
  std::set<std::string> name_list;
  {
    std::lock_guard<std::mutex> lock(buckets_name_->list_lock);
    name_list = buckets_name_->name_list;
  }
  s = store_->ListBucket(name_list, &buckets);
  if (!s.ok()) {
    g_zgw_server->zgw_monitor()->AddApiRequest(kPutBucket, 500);
    resp_->SetStatusCode(500);
    LOG(ERROR) << "ListBuckets failed: " << s.ToString();
    return;
  }

  // Zeppelin success, then build http body

  const libzgw::ZgwUserInfo &info = zgw_user_->user_info();
  resp_->SetStatusCode(200);
  resp_->SetBody(ListBucketXml(info, buckets));
}
