package common

import "github.com/drkisler/utils"

type PluginRPCServer struct {
	Impl IPlugin
}

func (s *PluginRPCServer) Load(config string, resp *utils.TResponse) error {
	*resp = s.Impl.Load(config)
	return nil
}

func (s *PluginRPCServer) Run(args interface{}, resp *utils.TResponse) error {
	*resp = s.Impl.Run()
	return nil
}
func (s *PluginRPCServer) Running(args interface{}, resp *utils.TResponse) error {
	*resp = s.Impl.Running()
	return nil
}

func (s *PluginRPCServer) Stop(args interface{}, resp *utils.TResponse) error {
	*resp = s.Impl.Stop()
	return nil
}
func (s *PluginRPCServer) GetConfigTemplate(args interface{}, resp *utils.TResponse) error {
	*resp = s.Impl.GetConfigTemplate()
	return nil
}

func (s *PluginRPCServer) GetErrLog(params string, resp *utils.TResponse) error {
	*resp = s.Impl.GetErrLog(params)
	return nil
}
func (s *PluginRPCServer) GetErrLogDate(args interface{}, resp *utils.TResponse) error {
	*resp = s.Impl.GetErrLogDate()
	return nil
}
func (s *PluginRPCServer) DelErrOldLog(strDate string, resp *utils.TResponse) error {
	*resp = s.Impl.DelErrOldLog(strDate)
	return nil
}
func (s *PluginRPCServer) DelErrLog(params string, resp *utils.TResponse) error {
	*resp = s.Impl.DelErrLog(params)
	return nil
}

func (s *PluginRPCServer) GetInfoLog(params string, resp *utils.TResponse) error {
	*resp = s.Impl.GetInfoLog(params)
	return nil
}
func (s *PluginRPCServer) GetInfoLogDate(args interface{}, resp *utils.TResponse) error {
	*resp = s.Impl.GetInfoLogDate()
	return nil
}
func (s *PluginRPCServer) DelInfoOldLog(strDate string, resp *utils.TResponse) error {
	*resp = s.Impl.DelInfoOldLog(strDate)
	return nil
}
func (s *PluginRPCServer) DelInfoLog(params string, resp *utils.TResponse) error {
	*resp = s.Impl.DelInfoLog(params)
	return nil
}

func (s *PluginRPCServer) GetDebugLog(params string, resp *utils.TResponse) error {
	*resp = s.Impl.GetDebugLog(params)
	return nil
}
func (s *PluginRPCServer) GetDebugLogDate(args interface{}, resp *utils.TResponse) error {
	*resp = s.Impl.GetDebugLogDate()
	return nil
}
func (s *PluginRPCServer) DelDebugOldLog(strDate string, resp *utils.TResponse) error {
	*resp = s.Impl.DelDebugOldLog(strDate)
	return nil
}
func (s *PluginRPCServer) DelDebugLog(params string, resp *utils.TResponse) error {
	*resp = s.Impl.DelDebugLog(params)
	return nil
}
