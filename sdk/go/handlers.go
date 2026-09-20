package extension

func (s *server) registerHandlers(c *conn) {
	c.reqH[MethodExtensionInitialize] = s.withConnRequest(s.handleInitialize)
	c.reqH[MethodExtensionShutdown] = s.withConnRequest(s.handleShutdown)
	c.reqH[MethodExtensionIntercept] = s.withConnRequest(s.handleIntercept)
	c.reqH[MethodExtensionProviderCatalog] = s.withConnRequest(s.handleProviderCatalog)
	c.reqH[MethodExtensionProviderStreamOpen] = s.withConnRequest(s.handleStreamOpen)
	c.reqH[MethodExtensionProviderStreamCancel] = s.withConnRequest(s.handleStreamCancel)
	c.reqH[MethodExtensionSpeculationBegin] = s.withConnRequest(s.handleSpeculationBegin)
	c.reqH[MethodExtensionSpeculationObserve] = s.withConnRequest(s.handleSpeculationObserve)
	c.reqH[MethodExtensionSpeculationClaim] = s.withConnRequest(s.handleSpeculationClaim)
	c.reqH[MethodExtensionSpeculationComplete] = s.withConnRequest(s.handleSpeculationComplete)
	c.reqH[MethodExtensionSpeculationEnd] = s.withConnRequest(s.handleSpeculationEnd)
	c.reqH[MethodExtensionUIAction] = s.withConnRequest(s.handleUIAction)
	c.reqH[MethodExtensionUISubmit] = s.withConnRequest(s.handleUISubmit)
	c.notH[MethodExtensionInitialized] = s.withConnNotification(s.handleInitialized)
	c.notH[MethodExtensionEvent] = s.withConnNotification(s.handleEvent)
	c.notH[MethodExtensionResourcesChanged] = s.withConnNotification(s.handleResourcesChanged)
}
