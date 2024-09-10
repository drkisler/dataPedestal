package common

type PluginRPCServer struct {
	Impl IPlugin
}

func (s *PluginRPCServer) Load(config string, resp *TResponse) error {
	*resp = s.Impl.Load(config)
	return nil
}
func (s *PluginRPCServer) Run(args interface{}, resp *TResponse) error {
	*resp = s.Impl.Run()
	return nil
}
func (s *PluginRPCServer) Running(args interface{}, resp *TResponse) error {
	*resp = s.Impl.Running()
	return nil
}
func (s *PluginRPCServer) Stop(args interface{}, resp *TResponse) error {
	*resp = s.Impl.Stop()
	return nil
}
func (s *PluginRPCServer) GetConfigTemplate(args interface{}, resp *TResponse) error {
	*resp = s.Impl.GetConfigTemplate()
	return nil
}

func (s *PluginRPCServer) CustomInterface(pluginOperate TPluginOperate, resp *TResponse) error {
	*resp = s.Impl.CustomInterface(pluginOperate)
	return nil
}

func (s *PluginRPCServer) GetSystemUsage(args interface{}, resp *string) error {
	sysUsage := s.Impl.GetSystemUsage()
	*resp = sysUsage
	return nil
}
