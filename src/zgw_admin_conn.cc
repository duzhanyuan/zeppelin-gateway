#include "src/zgw_admin_conn.h"

#include "src/zgw_util.h"
#include "src/zgw_server.h"
#include "src/zgw_monitor.h"

extern ZgwServer* g_zgw_server;

AdminConn::AdminConn(const int fd,
                     const std::string &ip_port,
                     pink::Thread* worker)
      : HttpConn(fd, ip_port, worker) {
	store_ = static_cast<libzgw::ZgwStore*>(worker->get_private());
}

void AdminConn::DealMessage(const pink::HttpRequest* req, pink::HttpResponse* resp) {
  Status s;
  std::string command, params;
  ExtraBucketAndObject(req->path, &command, &params);
  // Users operation, without authorization for now
  if (req->method == "GET") {
    if (command == "admin_list_users") {
      ListUsersHandle(resp);
    } else if (command == "status") {
      ListStatusHandle(resp);
    }
    return;
  } else if (req->method == "PUT" &&
             command == "admin_put_user") {
    if (params.empty()) {
      resp->SetStatusCode(400);
      return;
    }
    std::string access_key;
    std::string secret_key;
    s = store_->AddUser(params, &access_key, &secret_key);
    if (!s.ok()) {
      resp->SetStatusCode(500);
      resp->SetBody(s.ToString());
    } else {
      resp->SetStatusCode(200);
      resp->SetBody(access_key + "\r\n" + secret_key);
    }
    return;
  } else if (req->method == "OPTIONS") {
    if (command == "update_bucket_vol")  {
      g_zgw_server->zgw_monitor()->need_update_bucket_vol = true;
      resp->SetStatusCode(200);
    } else if (command == "reset_status") {
      g_zgw_server->zgw_monitor()->Reset();
      resp->SetStatusCode(200);
    }
  }
}

void AdminConn::ListStatusHandle(pink::HttpResponse* resp) {
  resp->SetBody(g_zgw_server->zgw_monitor()->GetFormatInfo());
  resp->SetStatusCode(200);
}

void AdminConn::ListUsersHandle(pink::HttpResponse* resp) {
  std::set<libzgw::ZgwUser *> user_list; // name : keys
  Status s = store_->ListUsers(&user_list);
  LOG(INFO) << "Call ListUsersHandle: " << ip_port();
  if (!s.ok()) {
    resp->SetStatusCode(500);
    resp->SetBody(s.ToString());
  } else {
    resp->SetStatusCode(200);
    std::string body;
    for (auto &user : user_list) {
      const auto &info = user->user_info();
      body.append("disply_name: " + info.disply_name + "\r\n");

      for (auto &key_pair : user->access_keys()) {
        body.append(key_pair.first + "\r\n"); // access key
        body.append(key_pair.second + "\r\n"); // secret key
      }
      body.append("\r\n");
    }
    resp->SetBody(body);
  }
}
