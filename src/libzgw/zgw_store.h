#ifndef ZGW_STORE_H
#define ZGW_STORE_H

#include <string>
#include <vector>

#include "slash/include/slash_status.h"

#include "libzp/include/zp_cluster.h"
#include "src/libzgw/zgw_bucket.h"
#include "src/libzgw/zgw_object.h"
#include "src/libzgw/zgw_user.h"
#include "src/libzgw/zgw_namelist.h"

using slash::Status;

namespace libzgw {

static const std::string kZgwMetaTableName = "__zgw_meta_table";
static const std::string kZgwDataTableName = "__zgw_data_table";
static const int kZgwTablePartitionNum = 10;
 
class NameList;
class ZgwObjectInfo;
class ZgwObject;

class ZgwStore {
public:
  static Status Open(const std::vector<std::string>& ips, ZgwStore** ptr);
  ~ZgwStore();

  // Operation On Service
  Status LoadAllUsers();
  Status AddUser(const std::string &user_name,
                 std::string *access_key, std::string *secret_key);
  Status GetUser(const std::string &access_key, ZgwUser **user);
  Status ListUsers(std::set<ZgwUser *> *user_list);
  Status SaveNameList(const NameList* nlist);
  Status GetNameList(NameList* nlist);
  
  // Operation On Buckets
  Status GetBucket(ZgwBucket* bucket);
  Status AddBucket(const std::string& bucket_name, const ZgwUserInfo& user_info);
  Status ListBucket(const std::set<std::string>& name_list, std::vector<ZgwBucket>* buckets);
  Status DelBucket(const std::string &bucket_name);
  
  // Operation On Objects
  Status AddObject(ZgwObject& object);
  Status GetObject(ZgwObject* object, bool need_content = false);
  Status ListObjects(const std::string& bucket_name,
                     const std::vector<std::string>&
                     candidate_names, std::vector<ZgwObject>* objects);
  Status GetPartialObject(ZgwObject* object, std::vector<std::pair<int, uint32_t>>& segments);
  Status DelObject(const std::string &bucket_name, const std::string &object_name);
  Status UploadPart(const std::string& bucket_name, const std::string& internal_obname,
                    const ZgwObjectInfo& info, const std::string& content, int part_num);
  Status ListParts(const std::string& bucket_name, const std::string& internal_obname,
                   std::vector<std::pair<int, ZgwObject>> *parts);
  Status CompleteMultiUpload(const std::string& bucket_name,
                             const std::string& internal_obname,
                             const std::vector<std::pair<int, ZgwObject>>& parts,
                             std::string *final_etag);

  // Original interface
  Status GetMeta(const std::string& key, std::string* value) {
    return zp_->Get(kZgwMetaTableName, key, value);
  }
  Status SetMeta(const std::string& key, const std::string& value) {
    return zp_->Set(kZgwMetaTableName, key, value);
  }
  Status GetZgwSapce(uint64_t* meta_vol, uint64_t* data_vol);

private:
  ZgwStore();
  Status Init(const std::vector<std::string>& ips);
  libzp::Cluster* zp_;
  ZgwUserList user_list_;
  std::map<std::string, ZgwUser*> access_key_user_map_;

  Status BuildMap();
  std::string GetRandomKey(int width);
  Status GetPartialObject(ZgwObject* object, int start, int end);
};

}  // namespace libzgw

#endif
