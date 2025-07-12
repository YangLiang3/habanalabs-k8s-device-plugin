/*
 * Copyright (c) 2022, HabanaLabs Ltd.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"google.golang.org/grpc"

	hlml "github.com/HabanaAI/gohlml"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// HabanalabsDevicePlugin implements the Kubernetes device plugin API
type HabanalabsDevicePlugin struct {
	ResourceManager
	log          *slog.Logger
	stop         chan interface{}
	health       chan *pluginapi.Device
	server       *grpc.Server
	resourceName string
	socket       string
	devs         []*pluginapi.Device
}

// GetPreferredAllocation returns a preferred set of devices to allocate
// from a list of available ones. The resulting preferred allocation is not
// guaranteed to be the allocation ultimately performed by the
// devicemanager. It is only designed to help the devicemanager make a more
// informed allocation decision when possible.
func (m *HabanalabsDevicePlugin) GetPreferredAllocation(ctx context.Context, request *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	response := &pluginapi.PreferredAllocationResponse{}
	for _, req := range request.ContainerRequests {
		m.log.Info("GetPreferredAllocation called", "available_count", len(req.AvailableDeviceIDs), "requested_count", int(req.AllocationSize))

		if req.AllocationSize <= 0 {
			return nil, fmt.Errorf("invalid allocation size: %d, must be positive", req.AllocationSize)
		}

		// Get all available devices
		availableDevices := make([]*pluginapi.Device, 0, len(req.AvailableDeviceIDs))
		for _, id := range req.AvailableDeviceIDs {
			device := getDevice(m.devs, id)
			if device != nil {
				availableDevices = append(availableDevices, device)
			}
		}

		if len(availableDevices) < int(req.AllocationSize) {
			return nil, fmt.Errorf("not enough available devices: requested %d, available %d",
				req.AllocationSize, len(availableDevices))
		}

		// Group devices by NUMA node
		numaDevices := groupDevicesByNuma(availableDevices)
		m.log.Info("Devices grouped by NUMA", "numa_groups", len(numaDevices))

		// Calculate total devices per NUMA node
		totalDevicesPerNuma := make(map[int64]int)
		allDevicesByNuma := groupDevicesByNuma(m.devs)
		for numaID, devices := range allDevicesByNuma {
			totalDevicesPerNuma[numaID] = len(devices)
		}

		// Find NUMA nodes that already have allocations (available devices < total devices)
		var partiallyAllocatedNumas []int64
		for numaID, devices := range numaDevices {
			if len(devices) < totalDevicesPerNuma[numaID] {
				partiallyAllocatedNumas = append(partiallyAllocatedNumas, numaID)
			}
		}
		m.log.Info("Partially allocated NUMA nodes", "numas", partiallyAllocatedNumas)

		var preferredDevices []string

		// For 1, 2, or 4 cards, try to allocate from the same NUMA node
		if req.AllocationSize <= 4 {
			// First try to allocate from already partially allocated NUMA nodes
			if len(partiallyAllocatedNumas) > 0 {
				for _, numaID := range partiallyAllocatedNumas {
					devices := numaDevices[numaID]
					if len(devices) >= int(req.AllocationSize) {
						m.log.Info("Allocating from partially allocated NUMA node", "numa_id", numaID,
							"available", len(devices), "requested", req.AllocationSize)

						for i := 0; i < int(req.AllocationSize); i++ {
							preferredDevices = append(preferredDevices, devices[i].ID)
						}
						break
					}
				}
			}

			// If no partially allocated NUMA node has enough devices, try unallocated NUMA nodes
			if len(preferredDevices) != int(req.AllocationSize) {
				for numaID, devices := range numaDevices {
					// Skip already tried partially allocated NUMA nodes
					alreadyTried := false
					for _, allocatedNuma := range partiallyAllocatedNumas {
						if numaID == allocatedNuma {
							alreadyTried = true
							break
						}
					}
					if alreadyTried {
						continue
					}

					if len(devices) >= int(req.AllocationSize) {
						m.log.Info("Found NUMA node with enough devices", "numa_id", numaID,
							"available", len(devices), "requested", req.AllocationSize)

						// Allocate the requested number of devices
						for i := 0; i < int(req.AllocationSize); i++ {
							preferredDevices = append(preferredDevices, devices[i].ID)
						}
						break
					}
				}
			}

			// If still couldn't find a suitable NUMA node, fall back to cross-NUMA allocation
			if len(preferredDevices) != int(req.AllocationSize) {
				m.log.Warn("Could not allocate requested devices from a single NUMA node, using cross-NUMA allocation",
					"requested", req.AllocationSize)
			}
		}

		// For more than 4 cards or if same-NUMA allocation failed, allocate across NUMA nodes
		if len(preferredDevices) != int(req.AllocationSize) {
			m.log.Info("Allocating devices across NUMA nodes", "requested", req.AllocationSize)
			preferredDevices = allocateAcrossNuma(numaDevices, int(req.AllocationSize))
		}

		// Final check if we have enough devices
		if len(preferredDevices) != int(req.AllocationSize) {
			m.log.Error("Failed to allocate requested number of devices",
				"requested", req.AllocationSize, "allocated", len(preferredDevices))
			return nil, fmt.Errorf("could not allocate requested number of devices: requested %d, allocated %d",
				req.AllocationSize, len(preferredDevices))
		}

		m.log.Info("Preferred allocation", "devices", preferredDevices)
		resp := &pluginapi.ContainerPreferredAllocationResponse{
			DeviceIDs: preferredDevices,
		}

		response.ContainerResponses = append(response.ContainerResponses, resp)
	}

	return response, nil
}

// allocateAcrossNuma allocates devices across NUMA nodes
func allocateAcrossNuma(numaDevices map[int64][]*pluginapi.Device, count int) []string {
	result := make([]string, 0, count)

	// Sort NUMA nodes by number of devices (descending)
	type numaGroup struct {
		numaID  int64
		devices []*pluginapi.Device
	}

	numaGroups := make([]numaGroup, 0, len(numaDevices))
	for numaID, devices := range numaDevices {
		numaGroups = append(numaGroups, numaGroup{numaID, devices})
	}

	// Sort by number of devices (descending)
	for i := 0; i < len(numaGroups); i++ {
		for j := i + 1; j < len(numaGroups); j++ {
			if len(numaGroups[i].devices) < len(numaGroups[j].devices) {
				numaGroups[i], numaGroups[j] = numaGroups[j], numaGroups[i]
			}
		}
	}

	// Allocate devices from NUMA nodes with the most devices first
	remaining := count
	for _, group := range numaGroups {
		numToAllocate := min(remaining, len(group.devices))
		for i := 0; i < numToAllocate; i++ {
			result = append(result, group.devices[i].ID)
		}
		remaining -= numToAllocate
		if remaining == 0 {
			break
		}
	}

	return result
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// groupDevicesByNuma groups devices by their NUMA node
func groupDevicesByNuma(devices []*pluginapi.Device) map[int64][]*pluginapi.Device {
	numaDevices := make(map[int64][]*pluginapi.Device)

	for _, device := range devices {
		var numaID int64 = -1
		if device.Topology != nil && len(device.Topology.Nodes) > 0 {
			numaID = device.Topology.Nodes[0].ID
		}

		numaDevices[numaID] = append(numaDevices[numaID], device)
	}

	return numaDevices
}

// NewHabanalabsDevicePlugin returns an initialized HabanalabsDevicePlugin.
func NewHabanalabsDevicePlugin(log *slog.Logger, resourceManager ResourceManager, resourceName string, socket string) *HabanalabsDevicePlugin {
	return &HabanalabsDevicePlugin{
		log:             log,
		ResourceManager: resourceManager,
		resourceName:    resourceName,
		socket:          socket,

		stop:   make(chan interface{}),
		health: make(chan *pluginapi.Device),

		// will be initialized on every server restart.
		devs: nil,
	}
}

// GetDevicePluginOptions returns the device plugin options.
func (m *HabanalabsDevicePlugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{
		GetPreferredAllocationAvailable: true, // Indicate to kubelet we have an implementation.
	}, nil
}

// dial establishes the gRPC communication with the registered device plugin.
func dial(unixSocketPath string, timeout time.Duration) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	c, err := grpc.DialContext(ctx, unixSocketPath,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return net.DialTimeout("unix", s, timeout)
		}),
	)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// Start starts the gRPC server of the device plugin
func (m *HabanalabsDevicePlugin) Start() error {
	err := m.cleanup()
	if err != nil {
		return err
	}

	if m.stop == nil {
		m.stop = make(chan interface{})
	}

	//  initialize Devices
	m.devs, err = m.Devices()
	if err != nil {
		return err
	}

	sock, err := net.Listen("unix", m.socket)
	if err != nil {
		return err
	}

	// First start serving the gRPC connection before registering.
	// It is required since kubernetes 1.26. Change is backward compatible.
	m.server = grpc.NewServer([]grpc.ServerOption{}...)
	pluginapi.RegisterDevicePluginServer(m.server, m)

	// Ignore error returns since the next block will fail if Serve fails.
	go func() { _ = m.server.Serve(sock) }()

	// Wait for server to start by launching a blocking connection
	conn, err := dial(m.socket, 5*time.Second)
	if err != nil {
		return err
	}
	conn.Close()

	go m.healthcheck()

	return nil
}

// Stop gRPC server
func (m *HabanalabsDevicePlugin) Stop() error {
	if m.server == nil {
		return nil
	}

	m.log.Info("Stoppping device plugin", "resource_name", m.resourceName, "socket", m.socket)
	m.server.Stop()
	m.server = nil
	close(m.stop)
	m.stop = nil

	return m.cleanup()
}

// Register registers the device plugin for the given resourceName with Kubelet.
func (m *HabanalabsDevicePlugin) Register() error {
	conn, err := dial(pluginapi.KubeletSocket, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)
	reqt := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     path.Base(m.socket),
		ResourceName: m.resourceName,
	}

	_, err = client.Register(context.Background(), reqt)
	if err != nil {
		return err
	}
	return nil
}

// ListAndWatch lists devices and update that list according to the health status
func (m *HabanalabsDevicePlugin) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	err := s.Send(&pluginapi.ListAndWatchResponse{Devices: m.devs})
	if err != nil {
		return err
	}

	for {
		select {
		case <-m.stop:
			return nil
		case d := <-m.health:
			d.Health = pluginapi.Unhealthy
			m.log.Info("Device is unhealthy", "resource", m.resourceName, "id", d.ID)
			if err := s.Send(&pluginapi.ListAndWatchResponse{Devices: m.devs}); err != nil {
				m.log.Error("Failed sending ListAndWatch to kubelet", "error", err)
			}
		}
	}
}

func (m *HabanalabsDevicePlugin) unhealthy(dev *pluginapi.Device) {
	m.health <- dev
}

// Allocate which return list of devices.
func (m *HabanalabsDevicePlugin) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	devs := m.devs
	response := pluginapi.AllocateResponse{ContainerResponses: []*pluginapi.ContainerAllocateResponse{}}
	for _, req := range reqs.ContainerRequests {
		var devicesList []*pluginapi.DeviceSpec
		netConfig := make([]string, 0, len(req.DevicesIDs))
		paths := make([]string, 0, len(req.DevicesIDs))
		uuids := make([]string, 0, len(req.DevicesIDs))
		visibleModule := make([]string, 0, len(req.DevicesIDs))

		for _, id := range req.DevicesIDs {
			device := getDevice(devs, id)
			if device == nil {
				return nil, fmt.Errorf("invalid request for %q: device unknown: %s", m.resourceName, id)
			}
			m.log.Info("Preparing device for registration", "device", device)

			m.log.Info("Getting device handle from hlml")
			deviceHandle, err := hlml.DeviceHandleBySerial(id)
			if err != nil {
				m.log.Error(err.Error())
				return nil, err
			}

			m.log.Info("Getting device minor number")
			minor, err := deviceHandle.MinorNumber()
			if err != nil {
				m.log.Error(err.Error())
				return nil, err
			}

			m.log.Info("Getting device module id")
			moduleID, err := deviceHandle.ModuleID()
			if err != nil {
				m.log.Error(err.Error())
				return nil, err
			}

			path := fmt.Sprintf("/dev/accel/accel%d", minor)
			paths = append(paths, path)
			uuids = append(uuids, id)
			netConfig = append(netConfig, fmt.Sprintf("%d", minor))
			visibleModule = append(visibleModule, fmt.Sprintf("%d", moduleID))

			ds := &pluginapi.DeviceSpec{
				ContainerPath: path,
				HostPath:      path,
				Permissions:   "rw",
			}
			devicesList = append(devicesList, ds)
			path = fmt.Sprintf("/dev/accel/accel_controlD%d", minor)

			ds = &pluginapi.DeviceSpec{
				ContainerPath: path,
				HostPath:      path,
				Permissions:   "rw",
			}
			devicesList = append(devicesList, ds)
		}

		envMap := map[string]string{
			"HABANA_VISIBLE_DEVICES":  strings.Join(netConfig, ","),
			"HL_VISIBLE_DEVICES":      strings.Join(paths, ","),
			"HL_VISIBLE_DEVICES_UUID": strings.Join(uuids, ","),
		}

		if len(req.DevicesIDs) < len(m.devs) {
			envMap["HABANA_VISIBLE_MODULES"] = strings.Join(visibleModule, ",")
		}

		response.ContainerResponses = append(response.ContainerResponses, &pluginapi.ContainerAllocateResponse{
			Devices: devicesList,
			Envs:    envMap,
		})
	}

	return &response, nil
}

// PreStartContainer performs actions before the container start
func (m *HabanalabsDevicePlugin) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

func (m *HabanalabsDevicePlugin) cleanup() error {
	if err := os.Remove(m.socket); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (m *HabanalabsDevicePlugin) healthcheck() {
	ctx, cancel := context.WithCancel(context.Background())

	xids := make(chan *pluginapi.Device)
	go watchXIDs(ctx, m.devs, xids)

	for {
		select {
		case <-m.stop:
			cancel()
			return
		case dev := <-xids:
			m.unhealthy(dev)
		}
	}
}

// Serve starts the gRPC server and register the device plugin to Kubelet
func (m *HabanalabsDevicePlugin) Serve() error {
	err := m.Start()
	if err != nil {
		return fmt.Errorf("could not start device plugln: %w", err)
	}
	m.log.Info("Starting to serve", "socket", m.socket)

	err = m.Register()
	if err != nil {
		_ = m.Stop()
		return fmt.Errorf("could not register device plugin: %w", err)
	}
	m.log.Info("Registered device plugin with Kubelet")

	return nil
}
