// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v3rpc

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

// etcd 安全是指安全模式下etcd的运行状态
// 访问安全：包括用户的认证和授权；
// 传输安全：是指使用SSL/TSL来加密数据信道。

// 用户权限功能是在etcd 2.1版本中增加的功能，在2.1版本之前，etcd是一个完全开放的系统，任何用户都可以通过REST API修改etcd存储的数据。
// etcd在2.1版本中增加了用户（User）和角色（Role）的概念，引入了用户认证的功能。为了保持向后兼容性和可升级性，etcd的用户权限功能默认是关闭的。
// 每个用户可以拥有多个角色。在安全功能被创建之前必须创建root用户，否则无法启用身份认证功能。

// etcd 包含三种类型的资源：
// 权限资源（permission resources）：表示用户（User）和角色（Role）信息。
// 键值资源（key-value resources）：表示键值对数据信息。
// 配置资源（settings resources）：安全配置信息、权限配置信息和etcd集群动态配置信息（选举/心跳等）。

type AuthServer struct {
	authenticator etcdserver.Authenticator
}

func NewAuthServer(s *etcdserver.EtcdServer) *AuthServer {
	return &AuthServer{authenticator: s}
}

func (as *AuthServer) AuthEnable(ctx context.Context, r *pb.AuthEnableRequest) (*pb.AuthEnableResponse, error) {
	resp, err := as.authenticator.AuthEnable(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) AuthDisable(ctx context.Context, r *pb.AuthDisableRequest) (*pb.AuthDisableResponse, error) {
	resp, err := as.authenticator.AuthDisable(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) AuthStatus(ctx context.Context, r *pb.AuthStatusRequest) (*pb.AuthStatusResponse, error) {
	resp, err := as.authenticator.AuthStatus(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) Authenticate(ctx context.Context, r *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error) {
	resp, err := as.authenticator.Authenticate(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleAdd(ctx context.Context, r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	resp, err := as.authenticator.RoleAdd(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleDelete(ctx context.Context, r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	resp, err := as.authenticator.RoleDelete(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleGet(ctx context.Context, r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	resp, err := as.authenticator.RoleGet(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleList(ctx context.Context, r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	resp, err := as.authenticator.RoleList(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleRevokePermission(ctx context.Context, r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	resp, err := as.authenticator.RoleRevokePermission(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleGrantPermission(ctx context.Context, r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	resp, err := as.authenticator.RoleGrantPermission(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) UserAdd(ctx context.Context, r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	resp, err := as.authenticator.UserAdd(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) UserDelete(ctx context.Context, r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	resp, err := as.authenticator.UserDelete(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) UserGet(ctx context.Context, r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	resp, err := as.authenticator.UserGet(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) UserList(ctx context.Context, r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	resp, err := as.authenticator.UserList(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) UserGrantRole(ctx context.Context, r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	resp, err := as.authenticator.UserGrantRole(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) UserRevokeRole(ctx context.Context, r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	resp, err := as.authenticator.UserRevokeRole(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) UserChangePassword(ctx context.Context, r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	resp, err := as.authenticator.UserChangePassword(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}
